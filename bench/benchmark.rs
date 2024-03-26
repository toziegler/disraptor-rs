use disraptor_rs::{cache_padded::CachePadded, Disraptor};
use rand::prelude::*;
use std::{
    sync::{
        atomic::{AtomicBool, AtomicU64},
        Arc,
    },
    time::{Duration, Instant},
    usize,
};

use clap::Parser;

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value_t = 1)]
    producer_threads: u64,

    #[arg(short, long, default_value_t = 1)]
    consumer_threads: u64,
}

const PRODUCER_BATCH_SIZE: u64 = 1024;
const PRODUCER_ITERATIONS: u64 = 100_000;
//const OPERATIONS: u64 = PRODUCER_ITERATIONS * PRODUCER_BATCH_SIZE * PRODUCER_THREADS;

fn main() {
    let args = Args::parse();
    let consumer_threads = args.consumer_threads;
    let producer_threads = args.producer_threads;

    assert!(consumer_threads.is_power_of_two());
    assert!(producer_threads.is_power_of_two());

    let operations: u64 = PRODUCER_ITERATIONS * PRODUCER_BATCH_SIZE * producer_threads;
    let disraptor = std::sync::Arc::new(Disraptor::<CachePadded<u64>, 524_288>::new(&[
        consumer_threads,
    ]));
    let messages_consumed: Arc<Vec<std::sync::Arc<CachePadded<AtomicU64>>>> = Arc::new(vec![
            Arc::new(CachePadded::new(AtomicU64::new(0)));
            consumer_threads as usize
        ]);
    let shutdown_signal = std::sync::Arc::new(AtomicBool::new(false));
    std::thread::scope(|s| {
        {
            let shutdown_signal = shutdown_signal.clone();
            s.spawn(move || {
                std::thread::sleep(Duration::new(5, 0));
                shutdown_signal.store(true, std::sync::atomic::Ordering::SeqCst);
            });
        }

        for _ in 0..producer_threads {
            s.spawn(|| {
                let mut producer_handle = disraptor.get_producer_handle();
                while !shutdown_signal.load(std::sync::atomic::Ordering::Relaxed) {
                    let mut batch = producer_handle.prepare_batch(PRODUCER_BATCH_SIZE as usize);
                    batch.write_for_all(|| CachePadded::new(1));
                }
            });
        }
        for id in 0..consumer_threads {
            let shutdown_signal = shutdown_signal.clone();
            let disraptor = Arc::clone(&disraptor);
            let messages_consumed = messages_consumed[id as usize].clone();
            s.spawn(move || {
                let mut consumer_handle = disraptor.get_consumer_handle(0, id as usize);
                let mut sum = 0;
                while !shutdown_signal.load(std::sync::atomic::Ordering::Relaxed) {
                    let mut c_batch = consumer_handle.get_range();
                    c_batch.consume_until_empty_or_condition(|msg, index| {
                        if (index as u64 & (consumer_threads - 1)) != id {
                            sum += 1;
                            return true;
                        }
                        assert_eq!(**msg, 1);
                        messages_consumed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        sum += **msg;
                        true
                    });
                    c_batch.immutable().release();
                }
            });
        }

        {
            let shutdown_signal = shutdown_signal.clone();
            let message_consumed = messages_consumed.clone();
            std::thread::spawn(move || {
                let mut timer = Instant::now();
                println!("msgs, producer, responder");
                while !shutdown_signal.load(std::sync::atomic::Ordering::Relaxed) {
                    if timer.elapsed() > Duration::new(1, 0) {
                        timer = Instant::now();
                        let mut message_processed = 0;
                        for b in message_consumed.iter() {
                            message_processed += b.swap(0, std::sync::atomic::Ordering::Relaxed);
                        }
                        println!(
                            "{},{},{}",
                            message_processed, producer_threads, consumer_threads
                        );
                    }
                }
            })
        }
    });
}
