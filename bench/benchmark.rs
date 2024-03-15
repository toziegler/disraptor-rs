use disraptor_rs::Disraptor;
use rand::prelude::*;
use std::{sync::Arc, time::Instant};

const PRODUCER_THREADS: u64 = 1;
const CONSUMER_THREADS_1: u64 = 1;
const CONSUMER_THREADS_2: u64 = 1;
const CONSUMER_THREADS_3: u64 = 1;

const PRODUCER_BATCH_SIZE: u64 = 1024;
const PRODUCER_ITERATIONS: u64 = 1_000_000;
const OPERATIONS: u64 = PRODUCER_ITERATIONS * PRODUCER_BATCH_SIZE;

fn main() {
    let disraptor = std::sync::Arc::new(Disraptor::<u64, 524_288>::new(&[
        CONSUMER_THREADS_1,
        CONSUMER_THREADS_2,
        CONSUMER_THREADS_3,
    ]));
    std::thread::scope(|s| {
        for _ in 0..PRODUCER_THREADS {
            s.spawn(|| {
                let mut producer_handle = disraptor.get_producer_handle();
                for _ in 0..PRODUCER_ITERATIONS {
                    producer_handle.prepare_batch(PRODUCER_BATCH_SIZE as usize);
                    while let Some(msg) = producer_handle.get_next_prepared() {
                        *msg = 1;
                    }
                    producer_handle.commit_batch();
                }
            });
        }
        for id in 0..CONSUMER_THREADS_1 {
            let disraptor = Arc::clone(&disraptor);
            s.spawn(move || {
                let mut consumer_handle = disraptor.get_consumer_handle(0, id as usize);
                let mut sum = 0;
                while sum < OPERATIONS {
                    //println!("consumer 1");
                    while let Some((msg, _)) = consumer_handle.get_next_slot() {
                        assert_eq!(*msg, 1);
                        sum += *msg;
                    }
                    consumer_handle.synchronize();
                }
            });
        }
        for id in 0..CONSUMER_THREADS_2 {
            let disraptor = Arc::clone(&disraptor);
            s.spawn(move || {
                let mut consumer_handle = disraptor.get_consumer_handle(1, id as usize);
                //let mut large_buffer = Box::new([[0_u8; 4096]; 65000]);
                //let mut large_buffer = vec![0_u8; 4096 * 65000].into_boxed_slice();
                let mut large_buffer: Vec<[u8; 4096]> = (0..65000).map(|_| [0_u8; 4096]).collect();
                let mut rng = rand::thread_rng();

                let mut all_sum = 0;
                let mut sum = 0;
                while sum < OPERATIONS {
                    while let Some((msg, index)) = consumer_handle.get_next_slot() {
                        if index as u64 % CONSUMER_THREADS_2 != id {
                            sum += 1;
                            continue;
                        }
                        assert_eq!(*msg, 1);
                        sum += *msg;
                        let slot_id = rng.gen_range(0..65000);
                        let fill: u8 = slot_id as u8;
                        large_buffer[slot_id][0..256].fill(fill);
                        let checksum_page: u64 = large_buffer[slot_id][0..256]
                            .iter()
                            .map(|number| *number as u64)
                            .sum();
                        assert_eq!(checksum_page, fill as u64 * 256);
                        all_sum += checksum_page;
                    }
                    consumer_handle.synchronize();
                }
                println!("checksum {}", all_sum);
                println!("memcopys performed {}", sum);
            });
        }
        for id in 0..CONSUMER_THREADS_3 {
            let disraptor = Arc::clone(&disraptor);
            s.spawn(move || {
                let mut consumer_handle = disraptor.get_consumer_handle(2, id as usize);
                let mut timer = std::time::Instant::now();
                let mut sum = 0;
                while sum < OPERATIONS {
                    while let Some((msg, _)) = consumer_handle.get_next_slot() {
                        assert_eq!(*msg, 1);
                        sum += *msg;
                        if sum % 10_000_000 == 0 {
                            let duration = timer.elapsed().as_secs_f64();
                            timer = Instant::now();
                            println!(
                                "{}, {},  {}",
                                PRODUCER_THREADS,
                                CONSUMER_THREADS_2,
                                10_000_000.0 / duration
                            );
                        }
                    }
                    consumer_handle.synchronize();
                }
            });
        }
    });
}
