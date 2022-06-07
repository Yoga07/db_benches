use std::borrow::Cow;
use bytes::{Buf, Bytes};
use rand::rngs::OsRng;
use rand::Rng;
use rayon::current_num_threads;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use tokio::task;

use std::fs;
use std::path::Path;
use heed::{EnvOpenOptions, Database, RwTxn};
use heed::types::*;

#[tokio::main]
async fn main() {
    let mut stored_keys = vec![];

    fs::create_dir_all(Path::new("./db").join("bytemuck.mdb")).unwrap();
    let env = EnvOpenOptions::new().open(Path::new("./db").join("bytemuck.mdb")).unwrap();

    // we will open the default unnamed database
    let heed_db: Database<OwnedType<i32>, Str> = env.create_database(None).unwrap();
    let db = heed_db;

    let now = Instant::now();

    for i in 0..1000 {
        // 1mb random data chunk
        let random_value = random_bytes(1024 * 1024);
        let random_key = &random_value[0..32];
        let key = &format!("{random_key:?}");

        // opening a write transaction
        let mut wtxn = env.write_txn().unwrap();
        // TODO: Figure out how to pass Bytes
        db.put(&mut wtxn, &i, &format!("{random_value:?}")).unwrap();
        wtxn.commit().unwrap();

        println!("Wrote {i:?}");

        stored_keys.push(key.clone());
    }

    let write = now.elapsed();
    println!("Write time: {write:?}");

    // for key in stored_keys {
    for i in 0..1000 {
        let mut rtxn = env.read_txn().unwrap();
        // let _ = db.get(&rtxn, &key).unwrap().unwrap();
        let _ = db.get(&rtxn, &i).unwrap().unwrap();
    }

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
