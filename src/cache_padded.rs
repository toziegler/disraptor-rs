use super::constants;
use core::fmt;
/// Stolen from crossbeam_utils::CachePadded
use static_assertions::const_assert_eq;
use std::{
    ops::{Deref, DerefMut},
    sync::atomic::AtomicUsize,
};

#[repr(align(64))]
#[derive(Clone, Copy, Default, Hash, PartialEq, Eq)]
pub struct CachePadded<T> {
    value: T,
}
unsafe impl<T: Send> Send for CachePadded<T> {}
unsafe impl<T: Sync> Sync for CachePadded<T> {}

impl<T> CachePadded<T> {
    /// Pads and aligns a value to the length of a cache line.
    ///
    /// # Examples
    ///
    /// ```
    ///  use disraptor_rs::cache_padded::CachePadded;
    /// let padded_value = CachePadded::new(1);
    /// ```
    pub const fn new(t: T) -> CachePadded<T> {
        CachePadded::<T> { value: t }
    }

    /// Returns the inner value.
    ///
    /// # Examples
    ///
    /// ```
    ///  use disraptor_rs::cache_padded::CachePadded;
    /// let padded_value = CachePadded::new(7);
    /// let value = padded_value.into_inner();
    /// assert_eq!(value, 7);
    /// ```
    pub fn into_inner(self) -> T {
        self.value
    }
}

impl<T> Deref for CachePadded<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.value
    }
}

impl<T> DerefMut for CachePadded<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.value
    }
}

impl<T: fmt::Debug> fmt::Debug for CachePadded<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CachePadded")
            .field("value", &self.value)
            .finish()
    }
}

impl<T> From<T> for CachePadded<T> {
    fn from(t: T) -> Self {
        CachePadded::new(t)
    }
}

const_assert_eq!(
    std::mem::size_of::<CachePadded::<AtomicUsize>>(),
    constants::CACHELINE_SIZE
);

const_assert_eq!(
    std::mem::align_of::<CachePadded::<AtomicUsize>>(),
    constants::CACHELINE_SIZE
);
