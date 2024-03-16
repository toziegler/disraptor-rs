pub mod cache_padded;
pub mod constants;
pub mod unchecked_fixed_array;

use cache_padded::CachePadded;
use std::sync::atomic::AtomicUsize;

pub use unchecked_fixed_array::UncheckedFixedArray;

#[allow(dead_code)]
#[derive(Debug)]
pub struct Disraptor<T, const SIZE: usize> {
    message_buffer: UncheckedFixedArray<T>, // backing buffer
    released_slots: [CachePadded<AtomicUsize>; 1],
    head_prepared_slots: CachePadded<AtomicUsize>,
    consumer_counters: Box<[CachePadded<AtomicUsize>]>,
    topology: Vec<u64>,
}

impl<T, const SIZE: usize> Disraptor<T, SIZE> {
    const INITIAL_PRODUCER_SLOT: usize = SIZE;
    const INITIAL_CONSUMER_SLOT: usize = SIZE - 1;

    pub fn new(topology: &[u64]) -> Self {
        let consumer_threads = topology.iter().sum();
        Self {
            message_buffer: UncheckedFixedArray::<T>::new(SIZE),
            released_slots: [CachePadded::new(AtomicUsize::new(
                Self::INITIAL_CONSUMER_SLOT,
            ))],
            head_prepared_slots: CachePadded::new(AtomicUsize::new(Self::INITIAL_PRODUCER_SLOT)),
            consumer_counters: (0..consumer_threads)
                .map(|_| CachePadded::new(AtomicUsize::new(Self::INITIAL_CONSUMER_SLOT)))
                .collect(),
            topology: topology.to_owned(),
        }
    }

    pub fn get_producer_handle(&self) -> ProducerHandle<T, SIZE> {
        let number_consumer_threads = self.topology.iter().sum::<u64>() as usize;
        let begin = number_consumer_threads
            - *self
                .topology
                .last()
                .expect("Topology should never be empty") as usize;
        let end = number_consumer_threads;

        ProducerHandle {
            disraptor: self,
            last_consumers_tail: &self.consumer_counters[begin..end],
            cached_last_consumer: Self::INITIAL_CONSUMER_SLOT,
            released_slots: &self.released_slots[0],
            head_prepared_slots: &self.head_prepared_slots,
        }
    }

    pub fn get_consumer_handle(
        &self,
        layer_id: usize,
        thread_id: usize,
    ) -> ConsumerHandle<T, SIZE> {
        assert!(self.topology.len() > layer_id);
        assert!(self.topology[layer_id] as usize > thread_id);

        if layer_id == 0 {
            return ConsumerHandle {
                disraptor: self,
                predecessors_tail: &self.released_slots[0..],
                cached_last_predecessor: Self::INITIAL_CONSUMER_SLOT,
                consumed_slot_tail: &self.consumer_counters[thread_id],
                current_slot: Self::INITIAL_CONSUMER_SLOT,
            };
        }

        let layer_previous = layer_id - 1;
        let begin = self.topology.iter().take(layer_previous).sum::<u64>();
        let end = begin + self.topology[layer_previous];

        ConsumerHandle {
            disraptor: self,
            predecessors_tail: &self.consumer_counters[begin as usize..end as usize],
            cached_last_predecessor: Self::INITIAL_CONSUMER_SLOT,
            consumed_slot_tail: &self.consumer_counters[end as usize + thread_id],
            current_slot: Self::INITIAL_CONSUMER_SLOT,
        }
    }

    // TODO: Custom drop
}

pub struct ProducerBatch<'a, 'b, T, const SIZE: usize> {
    handle: &'b ProducerHandle<'a, T, SIZE>,
    begin: usize,
    end: usize,
    current: usize,
}

impl<'a, 'b, T, const SIZE: usize> ProducerBatch<'a, 'b, T, SIZE>
where
    'b: 'a,
{
    pub fn write_for_all<F>(&mut self, mut produce_element_fn: F)
    where
        F: FnMut() -> T,
    {
        for index in self.begin..=self.end {
            unsafe {
                self.handle
                    .disraptor
                    .message_buffer
                    .write(index % SIZE, produce_element_fn())
            };
        }
        self.current = self.end + 1;
    }

    pub fn write_next(&mut self, value: T) -> bool {
        debug_assert!(self.begin <= self.current);
        if self.current <= self.end {
            unsafe {
                self.handle
                    .disraptor
                    .message_buffer
                    .write(self.current % SIZE, value);
            }
            self.current += 1;
            return true;
        }
        false
    }

    // consumes self
    pub fn release(self) {
        assert_eq!(self.current - 1, self.end);
        let expected_sequence = self.begin - 1;
        while self
            .handle
            .released_slots
            .load(std::sync::atomic::Ordering::SeqCst)
            != expected_sequence
        {}
        self.handle
            .released_slots
            .store(self.end, std::sync::atomic::Ordering::Release);
    }
}

pub struct ProducerHandle<'a, T, const SIZE: usize> {
    disraptor: &'a Disraptor<T, SIZE>,
    last_consumers_tail: &'a [CachePadded<AtomicUsize>],
    cached_last_consumer: usize,
    released_slots: &'a CachePadded<AtomicUsize>,
    head_prepared_slots: &'a CachePadded<AtomicUsize>,
}

impl<'a, T, const SIZE: usize> ProducerHandle<'a, T, SIZE> {
    #[inline(always)]
    pub fn prepare_batch<'b>(&'b mut self, batch_size: usize) -> ProducerBatch<'a, 'b, T, SIZE> {
        assert!(batch_size > 0);
        assert!(batch_size <= SIZE);

        let claimed_batch_begin = self
            .head_prepared_slots
            .fetch_add(batch_size, std::sync::atomic::Ordering::SeqCst);
        let claimed_batch_end = claimed_batch_begin + batch_size - 1;

        while claimed_batch_end > (self.cached_last_consumer + SIZE) {
            self.cached_last_consumer = self
                .last_consumers_tail
                .iter()
                .map(|number| number.load(std::sync::atomic::Ordering::SeqCst))
                .min()
                .expect("Could not find minimum value on empty value");
        }
        ProducerBatch {
            handle: self,
            begin: claimed_batch_begin,
            end: claimed_batch_end,
            current: claimed_batch_begin,
        }
    }
}

pub struct ConsumerBatch<'a, 'b, T, const SIZE: usize> {
    handle: &'b ConsumerHandle<'a, T, SIZE>,
    begin: usize,
    end: usize,
    current: usize,
}
impl<'a, 'b, T, const SIZE: usize> ConsumerBatch<'a, 'b, T, SIZE>
where
    'b: 'a,
{
    pub fn get_mut_for_all<F>(&mut self, mut consumer_fn: F)
    where
        F: FnMut(&mut T, usize),
    {
        for index in self.begin..self.end {
            let element: &mut T = unsafe {
                self.handle
                    .disraptor
                    .message_buffer
                    .get_mut((index + 1) % SIZE)
            };
            consumer_fn(element, index + 1);
        }
        self.current = self.end;
    }
    pub fn get_for_all<F>(&mut self, mut consumer_fn: F)
    where
        F: FnMut(&T, usize),
    {
        for index in self.begin..self.end {
            let element: &T =
                unsafe { self.handle.disraptor.message_buffer.get((index + 1) % SIZE) };
            consumer_fn(element, index + 1);
        }
        self.current = self.end;
    }

    pub fn get_next_mut(&mut self) -> Option<(&'b mut T, usize)> {
        if self.current < self.end {
            self.current += 1;
            let element: &mut T = unsafe {
                self.handle
                    .disraptor
                    .message_buffer
                    .get_mut(self.current % SIZE)
            };
            return Some((element, self.current));
        }
        None
    }

    pub fn get_next(&mut self) -> Option<(&'b T, usize)> {
        if self.current < self.end {
            self.current += 1;
            let element: &T = unsafe {
                self.handle
                    .disraptor
                    .message_buffer
                    .get(self.current % SIZE)
            };
            return Some((element, self.current));
        }
        None
    }
    pub fn release(self) {
        let current_counter = self
            .handle
            .consumed_slot_tail
            .load(std::sync::atomic::Ordering::Relaxed);
        assert!(
            current_counter <= self.current,
            "message buffer not fully consumed"
        );
        if self.current - current_counter == 0 {
            return;
        }
        // TODO: Could be a simple store right?
        self.handle.consumed_slot_tail.fetch_add(
            self.current - current_counter,
            std::sync::atomic::Ordering::Release,
        );
    }
}

pub struct ConsumerHandle<'a, T, const SIZE: usize> {
    disraptor: &'a Disraptor<T, SIZE>,
    predecessors_tail: &'a [CachePadded<AtomicUsize>],
    cached_last_predecessor: usize,
    consumed_slot_tail: &'a CachePadded<AtomicUsize>,
    current_slot: usize,
}

impl<'a, T, const SIZE: usize> ConsumerHandle<'a, T, SIZE> {
    pub fn get_prepared_batch<'b>(&'b mut self) -> ConsumerBatch<'a, 'b, T, SIZE> {
        let begin = self.cached_last_predecessor;
        // update last predecessor
        self.cached_last_predecessor = self
            .predecessors_tail
            .iter()
            .map(|number| number.load(std::sync::atomic::Ordering::SeqCst))
            .min()
            .expect("Could not find minimum value on empty value");

        ConsumerBatch {
            handle: self,
            begin,
            end: self.cached_last_predecessor,
            current: begin,
        }
    }
}

// ProducerGuard are they guards?
// do they need to have the reference to the Disraptor
// ConsumerGuard are they guards?
#[cfg(test)]
mod tests {
    use crate::Disraptor;

    #[test]
    fn construction() {
        let dis = Disraptor::<i32, 12>::new(&[1, 2]);
        let mut consumer_handle = dis.get_consumer_handle(0, 0);
        let mut producer_handle = dis.get_producer_handle();
        let mut batch = producer_handle.prepare_batch(10);
        batch.write_for_all(|| 1);
        batch.release();
    }
    #[test]
    fn multiple_elements() {
        let dis = Disraptor::<i32, 12>::new(&[1]);
        let mut producer_handle = dis.get_producer_handle();
        let mut consumer_handle = dis.get_consumer_handle(0, 0);
        let mut batch = producer_handle.prepare_batch(10);
        let mut counter = 0;
        batch.write_for_all(|| {
            let tmp = counter;
            counter += 1;
            tmp
        });
        batch.release();
        counter = 0;
        let mut batch = consumer_handle.get_prepared_batch();
        batch.get_for_all(|msg, _| {
            assert_eq!(*msg, counter);
            counter += 1;
        });
        batch.release();
        assert_eq!(counter, 10);
    }
    #[test]
    #[should_panic(expected = "assertion failed")]
    fn empty_batch() {
        let dis = Disraptor::<i32, 12>::new(&[1]);
        let mut producer_handle = dis.get_producer_handle();
        let batch = producer_handle.prepare_batch(0); // Empty batch
        assert!(batch.begin == batch.end); // Validate if the batch is indeed empty
        batch.release(); // This should not cause any issue
    }
    #[test]
    fn max_size_batch() {
        let dis = Disraptor::<i32, 12>::new(&[1]);
        let mut producer_handle = dis.get_producer_handle();
        let mut batch = producer_handle.prepare_batch(12); // Max size batch
        let mut counter = 0;
        batch.write_for_all(|| {
            counter += 1;
            counter
        });
        batch.release();

        let mut consumer_handle = dis.get_consumer_handle(0, 0);
        let mut batch = consumer_handle.get_prepared_batch();
        counter = 0;
        batch.get_for_all(|&msg, _| {
            counter += 1;
            assert_eq!(msg, counter);
        });
        batch.release();
        assert_eq!(counter, 12);
    }
    #[test]
    fn concurrency_test() {
        use std::sync::Arc;
        use std::thread;

        let dis = Arc::new(Disraptor::<i32, 100>::new(&[1, 2]));
        let handles: Vec<_> = (0..10)
            .map(|id| {
                let dis_clone = Arc::clone(&dis);
                thread::spawn(move || {
                    let mut producer_handle = dis_clone.get_producer_handle();
                    let mut batch = producer_handle.prepare_batch(10);
                    batch.write_for_all(|| id);
                    batch.release();
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }
        let mut consumer_handle = dis.get_consumer_handle(0, 0);
        let mut batch = consumer_handle.get_prepared_batch();
        let mut sum = 0;
        batch.get_for_all(|&msg, _| {
            sum += msg;
        });
        assert!(450 == sum);
    }

    // TODO: Improve interface here
    #[test]
    fn test_overflow_write() {
        let dis = Disraptor::<i32, 5>::new(&[1]);
        let mut producer_handle = dis.get_producer_handle();
        let mut batch = producer_handle.prepare_batch(5);
        // This should panic because it tries to write beyond the buffer size
        batch.write_for_all(|| 99);
        assert_eq!(false, batch.write_next(100));
        batch.release();
    }
    #[test]
    fn sequence_test() {
        let dis = Disraptor::<i32, 12>::new(&[1]);
        let mut producer_handle = dis.get_producer_handle();
        let mut batch = producer_handle.prepare_batch(3);
        let values = [1, 2, 3];
        for &val in &values {
            assert!(batch.write_next(val));
        }
        batch.release();

        let mut consumer_handle = dis.get_consumer_handle(0, 0);
        let mut batch = consumer_handle.get_prepared_batch();
        let mut idx = 0;
        batch.get_for_all(|&msg, _| {
            assert_eq!(msg, values[idx]);
            idx += 1;
        });
        assert_eq!(idx, values.len());
        batch.release();
    }
    #[test]
    #[should_panic(expected = "assertion failed")]
    fn invalid_batch_size() {
        let dis = Disraptor::<i32, 12>::new(&[1]);
        let mut producer_handle = dis.get_producer_handle();
        producer_handle.prepare_batch(13); // This should panic
    }
}

// Topology = {("WAL", 2), ("PrefixSum", 1)}
// get_consumer_handle(0, 0)
// get_consumer_handle(0, 1)
// get_consumer_handle(1, 0)
