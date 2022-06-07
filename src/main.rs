use bytes::Bytes;
use rand::rngs::OsRng;
use rand::Rng;
use rayon::current_num_threads;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use microkv::MicroKV;
use tokio::sync::RwLock;
use tokio::task;

#[tokio::main]
async fn main() {
    let mut write_handles = vec![];
    let mut read_handles = vec![];
    let stored_keys = Arc::new(RwLock::new(vec![]));
    let kv: Arc<RwLock<MicroKV>> = Arc::new(RwLock::new(
        MicroKV::open_with_base_path("safe", PathBuf::from("./dbs"))
            .unwrap()
            .set_auto_commit(true),
    ));

    let now = Instant::now();

    for _ in 0..100 {
        let stored = stored_keys.clone();
        let kv = kv.clone();
        let handle = task::spawn(async move {
            // 1mb random data chunk
            let random_value = random_bytes(1024 * 1024);
            let random_key = &random_value[0..32];

            let write_guard = kv.write().await;

            write_guard
                .put(format!("{random_key:?}"), &random_value)
                .unwrap();
            stored.write().await.push(format!("{random_key:?}"));
        });

        write_handles.push(handle);
    }

    futures::future::join_all(write_handles).await;

    let write = now.elapsed();
    println!("Write time: {write:?}");

    for key in stored_keys.clone().read().await.iter() {
        let kv = kv.clone();
        let key_clone = key.clone();
        let handle = task::spawn(async move {
            let _: Bytes = kv.read().await.get_unwrap(key_clone).unwrap();
            // assert_eq!(read_value, key_clone.1);
        });

        read_handles.push(handle);
    }

    futures::future::join_all(read_handles).await;

    let read = now.elapsed() - write;
    println!("Read time: {read:?}");
}

// Generates a random vector using provided `length`.
fn random_bytes(length: usize) -> Bytes {
    use rayon::prelude::*;
    let threads = current_num_threads();

    if threads > length {
        let mut rng = OsRng;
        return ::std::iter::repeat(())
            .map(|()| rng.gen::<u8>())
            .take(length)
            .collect();
    }

    let per_thread = length / threads;
    let remainder = length % threads;

    let mut bytes: Vec<u8> = (0..threads)
        .par_bridge()
        .map(|_| vec![0u8; per_thread])
        .map(|mut bytes| {
            let bytes = bytes.as_mut_slice();
            rand::thread_rng().fill(bytes);
            bytes.to_owned()
        })
        .flatten()
        .collect();

    bytes.extend(vec![0u8; remainder]);

    Bytes::from(bytes)
}
