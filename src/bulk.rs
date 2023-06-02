use std::{io, path::Path};

use anyhow::Context;
use memmap2::Mmap;

use crate::{
    config,
    tar::{self, TarBuilder, TarConsumer},
    zstd::Zstd,
    BLOCK_USIZE,
};

pub struct BulkCompressor<'a> {
    zstd: Zstd<'a>,
    buf: Vec<u8>,
}

impl BulkCompressor<'_> {
    pub fn new(config: config::Compression) -> anyhow::Result<Self> {
        let mut zstd = Zstd::new();
        zstd.set_level(config.level)
            .context("Failed to set zstd compression level")?;
        zstd.set_long(config.long)
            .context("Failed to set zstd long range matching")?;
        Ok(Self {
            zstd,
            buf: Vec::with_capacity(BLOCK_USIZE),
        })
    }

    pub fn compress<P: AsRef<Path>>(
        &mut self,
        mmap: &Mmap,
        mut header: tar::Header,
        path: P,
    ) -> anyhow::Result<()> {
        TarBuilder::new(&mut *self)
            .append_file(&mut header, path, mmap)
            .context("Failed to append file to archive")?;

        loop {
            let remaining = self
                .zstd
                .end_frame(&mut self.buf)
                .context("Failed to end zstd frame")?;
            if remaining == 0 {
                break;
            }
        }

        Ok(())
    }

    pub const fn buf(&self) -> &Vec<u8> {
        &self.buf
    }

    pub fn reset(&mut self) {
        self.buf.clear();
    }
}

impl TarConsumer for &mut BulkCompressor<'_> {
    fn consume(&mut self, buf: &[u8]) -> io::Result<()> {
        let len = buf.len();
        let mut pos = 0;
        loop {
            pos += self.zstd.compress(
                buf.get(pos..).expect("pos should not be out of range"),
                &mut self.buf,
            )?;

            if pos == len {
                break;
            }
        }

        Ok(())
    }
}
