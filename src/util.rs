#[must_use = "No effect, conversion only"]
pub fn usize_to_u64(value: usize) -> u64 {
    value
        .try_into()
        .expect("usize should be smaller or the same size as u64")
}
