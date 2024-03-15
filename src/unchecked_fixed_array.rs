use std::cell::UnsafeCell;

/// This data structure is used as a building block in our system
/// It provides uncchecked access to an array from multiple threads.
/// Where uncecked here means not borrow checked.
#[derive(Debug)]
pub struct UncheckedFixedArray<T> {
    data: Box<[UnsafeCell<T>]>,
}

impl<T> UncheckedFixedArray<T> {
    /// Creates a new instance of `UncheckedFixedArray` with a fixed size,
    /// initializing each element using the default constructor of type `T`.
    /// The size of the array is determined by the `HOT_RING_SIZE` constant.
    ///
    /// # Panics
    ///
    /// Panics if the memory allocation for the internal array fails.
    pub fn new(capacity: usize) -> Self {
        let mut temporary_vec = Vec::new();
        temporary_vec
            .try_reserve_exact(capacity)
            .expect("Out of memory error ");

        unsafe {
            // SAFETY: We have reserved the capacity above
            temporary_vec.set_len(capacity);
        }

        UncheckedFixedArray {
            data: temporary_vec.into_boxed_slice(),
        }
    }
    /// Provides mutable access to the element at the specified `sequence`
    /// index within the array. The caller must ensure that the index is
    /// within bounds and that no other mutable rferences to the same element
    /// exist at the same time to prevent undefined behavior.
    ///
    /// # Safety
    ///
    /// The caller must ensure that:
    /// - No other mutable references to the accessed element are alive.
    /// - The access does not lead to data races or violates Rust's aliasing rules.
    #[allow(clippy::mut_from_ref)]
    #[inline(always)]
    pub unsafe fn get_mut(&self, index: usize) -> &mut T {
        let cell = self.data.get_unchecked(index);
        &mut *cell.get()
    }

    /// Provides immutable access to the element at the specified `sequence`
    /// index within the array. The caller must ensure that the index is
    /// within bounds and that no mutable references to the same element
    /// exist at the same time to prevent undefined behavior.
    ///
    /// # Safety
    /// Same as `get_mut`
    #[inline(always)]
    pub unsafe fn get(&self, index: usize) -> &T {
        let cell = self.data.get_unchecked(index);
        &*cell.get()
    }
}
// The thread safe access must be ensured otherwise e.g. by mutex or something else
unsafe impl<T: Send> Send for UncheckedFixedArray<T> {}
unsafe impl<T: Sync> Sync for UncheckedFixedArray<T> {}
