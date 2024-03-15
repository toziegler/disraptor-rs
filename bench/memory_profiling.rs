use disraptor_rs::Disraptor;
// intended to be used with dhat or some other memory profiler to see if we have
//  unnecessary copies
fn main() {
    let disraptor = Disraptor::<u64, 16>::new(&[1]);

    const BATCH_SIZE: u64 = 10;
    let mut producer_handle = disraptor.get_producer_handle();
    let mut consumer_handle = disraptor.get_consumer_handle(0, 0);
    for _ in 0..1000 {
        producer_handle.prepare_batch(BATCH_SIZE as usize);
        while let Some(msg) = producer_handle.get_next_prepared() {
            *msg = 1;
        }
        producer_handle.commit_batch();
        let mut sum = 0;
        while let Some((msg, _)) = consumer_handle.get_next_slot() {
            assert_eq!(*msg, 1);
            sum += *msg;
        }
        assert_eq!(sum, BATCH_SIZE);
        consumer_handle.synchronize();
    }
}
