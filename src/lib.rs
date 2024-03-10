mod unchecked_fixed_array;
pub use unchecked_fixed_array::UncheckedFixedArray;
#[cfg(test)]
mod tests {
    use crate::UncheckedFixedArray;

    #[test]
    fn construction() {
        let fixed_array = UncheckedFixedArray::<usize>::new(1);
    }
}
