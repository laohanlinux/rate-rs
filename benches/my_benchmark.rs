#![feature(async_closure)]

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rate_rs::Bucket;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::time::Duration;

pub fn criterion_benchmark(c: &mut Criterion) {
    let bucket = Arc::new(Bucket::new_bucket(Duration::from_nanos(1), 16 * 1024));
    c.bench_function("wait", |b| b.iter(|| {
        bucket.take(1);
    }));
}

pub fn criterion_benchmark2(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
    let count = Arc::new(AtomicUsize::new(1));
    let bucket = Arc::new(Bucket::new_bucket(Duration::from_nanos(1), 16 * 1024));
    rt.block_on(async {
        c.bench_function("wait", |b| b.iter(async|| {
            bucket.wait(1).await;
            count.fetch_add(1, Ordering::SeqCst);
        }));
    });
}

pub fn criterion_benchmark3(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread().build().unwrap();
    let count = Arc::new(AtomicUsize::new(1));
    let bucket = Arc::new(Bucket::new_bucket(Duration::from_nanos(1), 16 * 1024));
    rt.block_on(async {
        c.bench_function("wait", |b| b.iter(async|| {
            bucket.wait(1).await;
            count.fetch_add(1, Ordering::SeqCst);
        }));
    });
    rt.block_on(async {
        tokio::fs::write("count.text", format!("{}", count.load(Ordering::SeqCst))).await;
    });
}


criterion_group!(benches, criterion_benchmark, criterion_benchmark2,criterion_benchmark3);
criterion_main!(benches);