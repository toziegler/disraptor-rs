pub mod cache_padded;
pub mod constants;
pub mod unchecked_fixed_array;

use cache_padded::CachePadded;
use std::{marker::PhantomData, sync::atomic::AtomicUsize};

pub use unchecked_fixed_array::UncheckedFixedArray;

#[allow(dead_code)]
#[derive(Debug)]
// Does currently not implement a custom Drop.
// HACK: Therefore, we added the trait bound, Copy
//
pub struct Disraptor<T, const SIZE: usize> {
    message_buffer: UncheckedFixedArray<T>, // backing buffer
    released_slots: [CachePadded<AtomicUsize>; 1],
    head_prepared_slots: CachePadded<AtomicUsize>,
    consumer_counters: Box<[CachePadded<AtomicUsize>]>,
    topology: Vec<u64>,
}

impl<T: Copy + Clone, const SIZE: usize> Disraptor<T, SIZE> {
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
                indexes_predecessor_end: &self.released_slots[0..],
                index_end_cached: Self::INITIAL_CONSUMER_SLOT,
                index_released: &self.consumer_counters[thread_id],
                index_consumed: std::cell::Cell::new(Self::INITIAL_CONSUMER_SLOT),
            };
        }

        let layer_previous = layer_id - 1;
        let begin = self.topology.iter().take(layer_previous).sum::<u64>();
        let end = begin + self.topology[layer_previous];

        ConsumerHandle {
            disraptor: self,
            indexes_predecessor_end: &self.consumer_counters[begin as usize..end as usize],
            index_end_cached: Self::INITIAL_CONSUMER_SLOT,
            index_released: &self.consumer_counters[end as usize + thread_id],
            index_consumed: std::cell::Cell::new(Self::INITIAL_CONSUMER_SLOT),
        }
    }
}

// if drop then every batch and handle are gone
// check unconsumed elemnets and drop them

pub struct ProducerBatch<'a, 'b, T, const SIZE: usize> {
    handle: &'b ProducerHandle<'a, T, SIZE>,
    begin: usize,
    end: usize,
    current: usize,
}

impl<'a, 'b, T, const SIZE: usize> ProducerBatch<'a, 'b, T, SIZE> {
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
    #[must_use]
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
}
// Drops the batch and synchronizes the atomics, but is blocking
impl<'a, 'b, T, const SIZE: usize> Drop for ProducerBatch<'a, 'b, T, SIZE> {
    fn drop(&mut self) {
        assert_eq!(self.current - 1, self.end);
        let expected_sequence = self.begin - 1;
        // NOTE: Blocking
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

pub struct Mutable<'m, 'h, T, const SIZE: usize> {
    is_immutable: bool,
    handle: Option<&'m ConsumerHandle<'h, T, SIZE>>,
    index_begin: usize,    // this is the begin of the mutable range and is fixed
    index_consumed: usize, // this is the index that tracks the actual consumption
    index_end_cached: usize, // this can be updated when we extend the range
                           // This makes the struct invariant over 'm. It's not required here, but can help the borrow
                           // checker infer lifetimes in some cases.
                           // Is this really ~invariant~?  &'a mut T
                           //                      covariant^      ^ invariant
                           //                      a: b
                           //_data: PhantomData<&'m mut ()>,
}

impl<T, const SIZE: usize> Drop for Mutable<'_, '_, T, SIZE> {
    fn drop(&mut self) {
        debug_assert!(self.is_immutable);
    }
}
pub struct Immutable<'a> {
    index_released: &'a CachePadded<AtomicUsize>,
    begin: usize,
    end: usize,
}

impl Range<Immutable<'_>> {
    // Note: Maintain the order here!
    pub fn release(&self) {
        // check that we do not decrement the counter
        if self.mutability.end
            < self
                .mutability
                .index_released
                .load(std::sync::atomic::Ordering::Relaxed)
        {
            println!("we have a wrong order in sync");
            return; // do not decrement the counter
        }
        self.mutability
            .index_released
            .store(self.mutability.end, std::sync::atomic::Ordering::Release);
    }
}

pub struct Range<Mutability> {
    mutability: Mutability,
}

impl<'m, 'h, T, const SIZE: usize> Range<Mutable<'m, 'h, T, SIZE>> {
    pub fn consume_and_extend_until_empty_or_condition(
        &mut self,
        mut consumer_fn: impl FnMut(&T, usize) -> bool,
    ) {
        let mut consumed = self.mutability.index_consumed;
        'outer: loop {
            for index in self.mutability.index_consumed..self.mutability.index_end_cached {
                let element: &T = unsafe {
                    self.mutability
                        .handle
                        .unwrap()
                        .disraptor
                        .message_buffer
                        .get_mut((index + 1) % SIZE)
                };
                if consumer_fn(element, index + 1) == false {
                    break 'outer;
                }
                consumed += 1;
            }
            self.mutability.index_consumed = consumed;

            self.mutability.index_end_cached = self
                .mutability
                .handle
                .unwrap()
                .indexes_predecessor_end
                .iter()
                .map(|number| number.load(std::sync::atomic::Ordering::SeqCst))
                .min()
                .expect("Could not find minimum value on empty value");

            if self.mutability.index_consumed == self.mutability.index_end_cached {
                break;
            }
        }
        self.mutability.index_consumed = consumed;
    }

    pub fn consume_until_empty_or_condition(
        &mut self,
        mut consumer_fn: impl FnMut(&T, usize) -> bool,
    ) {
        let mut consumed = self.mutability.index_consumed;
        for index in self.mutability.index_consumed..self.mutability.index_end_cached {
            let element: &T = unsafe {
                self.mutability
                    .handle
                    .unwrap()
                    .disraptor
                    .message_buffer
                    .get_mut((index + 1) % SIZE)
            };
            if consumer_fn(element, index + 1) == false {
                break;
            }
            consumed += 1;
        }
        self.mutability.index_consumed = consumed;
    }

    pub fn immutable(mut self) -> Range<Immutable<'h>> {
        let handle = self.mutability.handle.take().expect("Handle should exist");
        handle.index_consumed.set(self.mutability.index_consumed); // communicate the state to the
        self.mutability.is_immutable = true;
        // handle
        Range {
            mutability: Immutable {
                index_released: handle.index_released, // needed to synchronize the range
                begin: self.mutability.index_begin,
                end: self.mutability.index_consumed,
            },
        }
    }
}

impl<'a> Range<Immutable<'a>> {}

pub struct ConsumerHandle<'a, T, const SIZE: usize> {
    disraptor: &'a Disraptor<T, SIZE>,
    indexes_predecessor_end: &'a [CachePadded<AtomicUsize>], // multiple indexes from the predecessors
    index_end_cached: usize,
    index_consumed: std::cell::Cell<usize>, // this maintains the state of multiple ranges, i.e., if we set one to
    // immutable the next should begin where the previous ended consuming
    index_released: &'a CachePadded<AtomicUsize>,
}

impl<'a, T, const SIZE: usize> ConsumerHandle<'a, T, SIZE> {
    fn update_cached_end(&mut self) {
        assert!(
            self.index_consumed.get() <= self.index_end_cached,
            "Our consumed index should never go past the end"
        );
        if self.index_consumed.get() == self.index_end_cached {
            self.index_end_cached = self
                .indexes_predecessor_end
                .iter()
                .map(|number| number.load(std::sync::atomic::Ordering::SeqCst))
                .min()
                .expect("Could not find minimum value on empty value");
        }
    }

    pub fn get_range<'m>(&'m mut self) -> Range<Mutable<'m, 'a, T, SIZE>> {
        self.update_cached_end();
        Range {
            mutability: Mutable {
                is_immutable: false,
                handle: Some(self),
                index_begin: self.index_consumed.get(),
                index_consumed: self.index_consumed.get(),
                index_end_cached: self.index_end_cached,
                //_data: PhantomData,
            },
        }
    }
}

// ProducerGuard are they guards?
// do they need to have the reference to the Disraptor
// ConsumerGuard are they guards?
#[cfg(test)]
mod tests {
    use crate::{Disraptor, Immutable};

    #[test]
    fn construction() {
        let dis = Disraptor::<i32, 12>::new(&[1, 2]);
        _ = dis.get_consumer_handle(0, 0);
        let mut producer_handle = dis.get_producer_handle();
        let mut batch = producer_handle.prepare_batch(10);
        batch.write_for_all(|| 1);
    }

    #[test]
    fn get_range_extensible() {
        let dis = Disraptor::<i32, 20>::new(&[1]);
        _ = dis.get_consumer_handle(0, 0);
        let mut producer_handle = dis.get_producer_handle();
        {
            let mut counter = 0;
            let mut batch = producer_handle.prepare_batch(10);
            batch.write_for_all(|| {
                counter += 1;
                counter
            });
        }

        let mut consumer_handle = dis.get_consumer_handle(0, 0);
        let mut range = consumer_handle.get_range();

        let mut counter = 0;
        range.consume_and_extend_until_empty_or_condition(|msg, _| {
            if counter == 5 {
                return false;
            }
            counter += 1;
            assert_eq!(*msg, counter);
            true
        });
        {
            let mut counter = 10;
            let mut batch = producer_handle.prepare_batch(10);
            batch.write_for_all(|| {
                counter += 1;
                counter
            });
        } // synchronize
        range.consume_and_extend_until_empty_or_condition(|msg, _| {
            counter += 1;
            assert_eq!(*msg, counter);
            true
        });
        let immutable_range = range.immutable();
        immutable_range.release();
        assert_eq!(counter, 20);
    }

    #[test]
    fn get_range_fixed() {
        let dis = Disraptor::<i32, 12>::new(&[1]);
        _ = dis.get_consumer_handle(0, 0);
        let mut producer_handle = dis.get_producer_handle();
        {
            let mut counter = 0;
            let mut batch = producer_handle.prepare_batch(10);
            batch.write_for_all(|| {
                counter += 1;
                counter
            });
        }

        let mut consumer_handle = dis.get_consumer_handle(0, 0);
        let mut range = consumer_handle.get_range();

        let mut counter = 0;
        range.consume_until_empty_or_condition(|msg, _| {
            if counter == 5 {
                return false;
            }
            counter += 1;
            assert_eq!(*msg, counter);
            true
        });
        let immutable_range = range.immutable();
        let mut range2 = consumer_handle.get_range();
        range2.consume_until_empty_or_condition(|msg, _| {
            counter += 1;
            assert_eq!(*msg, counter);
            true
        });

        range2.consume_until_empty_or_condition(|_, _| {
            assert!(false, "should never happen");
            true
        });

        let immutable_range2 = range2.immutable();
        immutable_range.release();
        immutable_range2.release();

        let mut batch = producer_handle.prepare_batch(10);
        batch.write_for_all(|| 1);
    }

    #[test]
    fn multiple_elements_consumer_until() {
        let dis = Disraptor::<i32, 12>::new(&[1]);
        let mut producer_handle = dis.get_producer_handle();
        let mut consumer_handle = dis.get_consumer_handle(0, 0);
        let mut counter = 0;
        {
            let mut batch = producer_handle.prepare_batch(10);
            batch.write_for_all(|| {
                let tmp = counter;
                counter += 1;
                tmp
            });
        }
        counter = 0;
        let mut range = consumer_handle.get_range();
        range.consume_until_empty_or_condition(|msg, _| {
            if counter == 5 {
                return false;
            }
            assert_eq!(*msg, counter);
            counter += 1;
            true
        });
        range.consume_until_empty_or_condition(|msg, _| {
            assert_eq!(*msg, counter); // 6 5
            counter += 1;
            true
        });
        range.consume_until_empty_or_condition(|msg, _| {
            assert!(false, "should never happen"); // 6 5
            true
        });

        assert_eq!(counter, 10);
        range.immutable().release();
    }
    #[test]
    fn multiple_elements() {
        let dis = Disraptor::<i32, 12>::new(&[1]);
        let mut producer_handle = dis.get_producer_handle();
        let mut consumer_handle = dis.get_consumer_handle(0, 0);
        let mut counter = 0;
        {
            let mut batch = producer_handle.prepare_batch(10);
            batch.write_for_all(|| {
                let tmp = counter;
                counter += 1;
                tmp
            });
        }
        counter = 0;
        let mut range = consumer_handle.get_range();
        range.consume_until_empty_or_condition(|msg, _| {
            assert_eq!(*msg, counter);
            counter += 1;
            true
        });
        assert_eq!(counter, 10);
        range.immutable().release();
    }
    #[test]
    #[should_panic(expected = "assertion failed")]
    fn empty_batch() {
        let dis = Disraptor::<i32, 12>::new(&[1]);
        let mut producer_handle = dis.get_producer_handle();
        let batch = producer_handle.prepare_batch(0); // Empty batch
        assert!(batch.begin == batch.end); // Validate if the batch is indeed empty
    }
    #[test]
    fn max_size_batch() {
        let dis = Disraptor::<i32, 12>::new(&[1]);
        let mut producer_handle = dis.get_producer_handle();
        let mut counter = 0;
        {
            let mut batch = producer_handle.prepare_batch(12); // Max size batch
            batch.write_for_all(|| {
                counter += 1;
                counter
            });
        }
        let mut consumer_handle = dis.get_consumer_handle(0, 0);
        let mut batch = consumer_handle.get_range();
        counter = 0;
        batch.consume_until_empty_or_condition(|&msg, _| {
            counter += 1;
            assert_eq!(msg, counter);
            true
        });
        assert_eq!(counter, 12);
        batch.immutable().release();
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
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }
        let mut consumer_handle = dis.get_consumer_handle(0, 0);
        let mut range = consumer_handle.get_range();
        let mut sum = 0;
        range.consume_until_empty_or_condition(|&msg, _| {
            sum += msg;
            true
        });
        assert!(450 == sum);
        range.immutable().release();
    }

    // TODO: Improve interface here
    #[test]
    fn test_overflow_write() {
        let dis = Disraptor::<i32, 5>::new(&[1]);
        let mut producer_handle = dis.get_producer_handle();
        let mut batch = producer_handle.prepare_batch(5);
        batch.write_for_all(|| 99);
        assert_eq!(false, batch.write_next(100));
    }
    #[test]
    fn sequence_test() {
        let dis = Disraptor::<i32, 12>::new(&[1]);
        let mut producer_handle = dis.get_producer_handle();
        let values = [1, 2, 3];
        {
            let mut batch = producer_handle.prepare_batch(3);
            for &val in &values {
                assert!(batch.write_next(val));
            }
        }

        let mut consumer_handle = dis.get_consumer_handle(0, 0);
        let mut batch = consumer_handle.get_range();
        let mut idx = 0;
        batch.consume_until_empty_or_condition(|&msg, _| {
            assert_eq!(msg, values[idx]);
            idx += 1;
            true
        });
        assert_eq!(idx, values.len());
        batch.immutable().release();
    }
    #[test]
    #[should_panic(expected = "assertion failed")]
    fn invalid_batch_size() {
        let dis = Disraptor::<i32, 12>::new(&[1]);
        let mut producer_handle = dis.get_producer_handle();
        producer_handle.prepare_batch(13); // This should panic
    }
}
