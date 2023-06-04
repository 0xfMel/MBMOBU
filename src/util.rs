use std::io::{self, ErrorKind};

#[must_use = "No effect, conversion only"]
pub fn usize_to_u64(value: usize) -> u64 {
    value
        .try_into()
        .expect("usize should be smaller or the same size as u64")
}

pub trait AnyhowExt<T> {
    fn into_io(self) -> io::Result<T>;
}

impl<T> AnyhowExt<T> for anyhow::Result<T> {
    fn into_io(self) -> io::Result<T> {
        self.map_err(|e| io::Error::new(ErrorKind::Other, e))
    }
}
