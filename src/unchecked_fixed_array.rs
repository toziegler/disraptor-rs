use std::cell::UnsafeCell;
use std::mem::MaybeUninit;

/// This data structure is used as a building block in our system
/// It provides uncchecked access to an array from multiple threads.
/// Where uncecked here means not borrow checked.
/// Note: This data structure does not DROP the values
#[derive(Debug)]
pub struct UncheckedFixedArray<T> {
    data: Box<[MaybeUninit<UnsafeCell<T>>]>,
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

        // SAFETY: Safety Requirements for set_len are satisified
        // 1.) We have reserved the capacity above
        // 2.) And MaybeUninit doe not require initialization
        // see https://doc.rust-lang.org/std/mem/union.MaybeUninit.html#initializing-an-array-element-by-element
        unsafe {
            temporary_vec.set_len(capacity);
        }

        UncheckedFixedArray {
            data: temporary_vec.into_boxed_slice(),
        }
    }
    /// Initializes the element at `index`.
    /// # Safety
    ///
    /// The caller must ensure that:
    /// - Index is not out-of-bound
    /// - The access does not lead to data races
    pub unsafe fn write(&self, index: usize, value: T) {
        let m_uninit = self.data.get_unchecked(index);
        //need to use `get_raw`:  https://doc.rust-lang.org/std/cell/struct.UnsafeCell.html#method.raw_get
        UnsafeCell::raw_get(m_uninit.as_ptr()).write(value);
    }

    /// Provides mutable access to the element at the specified `sequence`
    /// index within the array. The caller must ensure that the index is
    /// within bounds and that no other mutable rferences to the same element
    /// exist at the same time to prevent undefined behavior.
    ///
    /// # Safety
    ///
    /// The caller must ensure that:
    /// - Index is not out-of-bound
    /// - No other mutable references to the accessed element are alive.
    /// - The access does not lead to data races or violates Rust's aliasing rules.
    /// - Caller must have called WRITE
    #[allow(clippy::mut_from_ref)]
    #[inline(always)]
    pub unsafe fn get_mut(&self, index: usize) -> &mut T {
        let cell = self.data.get_unchecked(index);
        &mut *cell.assume_init_ref().get()
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
        &*cell.assume_init_ref().get()
    }
}
// The thread safe access must be ensured otherwise e.g. by mutex or something else
unsafe impl<T: Send> Send for UncheckedFixedArray<T> {}
unsafe impl<T: Sync> Sync for UncheckedFixedArray<T> {}

#[cfg(test)]
mod test {
    use std::cell::Cell;

    use super::UncheckedFixedArray;

    #[test]
    fn drop_uninitialized() {
        struct IncrementOnDrop<'a> {
            cell: &'a Cell<i32>,
        }

        impl Drop for IncrementOnDrop<'_> {
            fn drop(&mut self) {
                let before = self.cell.get();
                self.cell.set(before + 1);
            }
        }

        let array = UncheckedFixedArray::<IncrementOnDrop>::new(1024);
        drop(array);
    }

    #[test]
    fn initialize_correctly() {
        let array = UncheckedFixedArray::<usize>::new(1024);
        unsafe { array.write(0, 1) };
        let number = unsafe { array.get(0) };
        println!("{}", number);
    }
}
