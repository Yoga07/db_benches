use std::sync::Arc;
use std::time::Instant;
use bytes::Bytes;
use rand::Rng;
use rand::rngs::OsRng;
use rayon::current_num_threads;

use tokio::task;
use cacache::{write, read};
use tokio::sync::RwLock;


#[tokio::main]
async fn main() {
    let mut write_handles = vec![];
    let mut read_handles = vec![];
    let stored_keys = Arc::new(RwLock::new(vec![]));

    let now = Instant::now();

    for _ in 0..999 {
        let stored = stored_keys.clone();
        let handle = task::spawn(async move  {
            // 1mb random data chunk
            let random_value = random_bytes( 1024 * 1024);
            let random_key = &random_value[0..32];

            let _ = write("./db", format!("{random_key:?}"), &random_value).await;
            stored.write().await.push(format!("{random_key:?}"));
        });

        write_handles.push(handle);
    }

    futures::future::join_all(write_handles).await;

    let write = now.elapsed();
    println!("Write time: {write:?}");

    for key in stored_keys.clone().read().await.iter() {
        let key_clone = key.clone();
        let handle = task::spawn(async move  {
            let _ = read("./db", key_clone).await.unwrap();
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