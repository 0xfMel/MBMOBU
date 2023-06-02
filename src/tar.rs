use std::{
    io::{self, Write},
    mem::ManuallyDrop,
    path::Path,
};

pub use tarlib::Header;

pub trait TarConsumer {
    fn consume(&mut self, buf: &[u8]) -> io::Result<()>;
}

struct TarConsumerWriter<C> {
    inner: C,
}

impl<C: TarConsumer> Write for TarConsumerWriter<C> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.consume(buf)?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub struct TarBuilder<C: TarConsumer> {
    tar: ManuallyDrop<tarlib::Builder<TarConsumerWriter<C>>>,
}

impl<C: TarConsumer> TarBuilder<C> {
    pub fn new(consumer: C) -> Self {
        Self {
            tar: ManuallyDrop::new(tarlib::Builder::new(TarConsumerWriter { inner: consumer })),
        }
    }

    pub fn append_file<P: AsRef<Path>>(
        &mut self,
        header: &mut Header,
        path: P,
        data: &[u8],
    ) -> io::Result<()> {
        self.tar.append_data(header, path, data)
    }
}
