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

impl<T: Default, const SIZE: usize> Disraptor<T, SIZE> {
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
            current_slot: Self::INITIAL_PRODUCER_SLOT,
            batch_begin: Self::INITIAL_PRODUCER_SLOT,
            batch_end: Self::INITIAL_PRODUCER_SLOT,
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
}
pub struct ProducerHandle<'a, T, const SIZE: usize> {
    disraptor: &'a Disraptor<T, SIZE>,
    last_consumers_tail: &'a [CachePadded<AtomicUsize>],
    cached_last_consumer: usize,
    released_slots: &'a CachePadded<AtomicUsize>,
    head_prepared_slots: &'a CachePadded<AtomicUsize>,
    current_slot: usize,
    batch_begin: usize,
    batch_end: usize,
}

impl<'a, T: Default, const SIZE: usize> ProducerHandle<'a, T, SIZE> {
    #[inline(always)]
    pub fn prepare_batch(&mut self, batch_size: usize) {
        assert!(batch_size > 0);
        assert!(batch_size <= SIZE);
        assert!(self.current_slot >= self.batch_end);

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
        self.current_slot = claimed_batch_begin;
        self.batch_begin = claimed_batch_begin;
        self.batch_end = claimed_batch_end;
    }

    #[inline(always)]
    pub fn get_next_prepared(&mut self) -> Option<&mut T> {
        assert!(self.batch_begin <= self.current_slot);
        if self.current_slot <= self.batch_end {
            let t: &mut T = unsafe {
                self.disraptor
                    .message_buffer
                    .get_mut(self.current_slot % SIZE)
            };
            self.current_slot += 1;
            return Some(t);
        }
        None
    }
    #[inline(always)]
    pub fn commit_batch(&mut self) {
        assert_eq!(self.current_slot - 1, self.batch_end);
        let expected_sequence = self.batch_begin - 1;
        while self
            .released_slots
            .load(std::sync::atomic::Ordering::SeqCst)
            != expected_sequence
        {}
        self.released_slots
            .store(self.batch_end, std::sync::atomic::Ordering::Release);
    }
}
pub struct ConsumerHandle<'a, T, const SIZE: usize> {
    disraptor: &'a Disraptor<T, SIZE>,
    predecessors_tail: &'a [CachePadded<AtomicUsize>],
    cached_last_predecessor: usize,
    consumed_slot_tail: &'a CachePadded<AtomicUsize>,
    current_slot: usize,
}

impl<'a, T: Default, const SIZE: usize> ConsumerHandle<'a, T, SIZE> {
    #[inline(always)]
    pub fn get_next_slot(&mut self) -> Option<(&mut T, usize)> {
        if self.current_slot >= self.cached_last_predecessor {
            self.cached_last_predecessor = self
                .predecessors_tail
                .iter()
                .map(|number| number.load(std::sync::atomic::Ordering::SeqCst))
                .min()
                .expect("Could not find minimum value on empty value");
        }

        if self.current_slot < self.cached_last_predecessor {
            self.current_slot += 1;
            let t: &mut T = unsafe {
                self.disraptor
                    .message_buffer
                    .get_mut(self.current_slot % SIZE)
            };
            return Some((t, self.current_slot));
        }
        None
    }

    #[inline(always)]
    pub fn synchronize(&mut self) {
        let current_counter = self
            .consumed_slot_tail
            .load(std::sync::atomic::Ordering::Relaxed);
        assert!(
            current_counter <= self.current_slot,
            "message buffer not fully consumed"
        );
        if self.current_slot - current_counter == 0 {
            return;
        }
        // TODO: Could be a simple store right?
        self.consumed_slot_tail.fetch_add(
            self.current_slot - current_counter,
            std::sync::atomic::Ordering::Release,
        );
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
        let msg = consumer_handle.get_next_slot();
        assert_eq!(msg, None);
        consumer_handle.synchronize();
        let mut producer_handle = dis.get_producer_handle();
        producer_handle.prepare_batch(10);
        while let Some(_) = producer_handle.get_next_prepared() {}
        producer_handle.commit_batch();
    }
}

// Topology = {("WAL", 2), ("PrefixSum", 1)}
// get_consumer_handle(0, 0)
// get_consumer_handle(0, 1)
// get_consumer_handle(1, 0)
