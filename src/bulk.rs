use std::{io, path::Path};

use anyhow::Context;
use memmap2::Mmap;

use crate::{
    tar::{self, TarBuilder, TarConsumer},
    zstd::Zstd,
    BLOCK_USIZE, ZSTD_LEVEL,
};

pub struct BulkCompressor<'a> {
    zstd: Zstd<'a>,
    buf: Vec<u8>,
}

impl BulkCompressor<'_> {
    pub fn new() -> anyhow::Result<Self> {
        let mut zstd = Zstd::new();
        zstd.set_level(ZSTD_LEVEL)
            .context("Failed to set zstd compression level")?;
        zstd.set_long()
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
        relative: P,
    ) -> anyhow::Result<()> {
        TarBuilder::new(&mut *self)
            .append_file(&mut header, relative, mmap)
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

    pub fn buf(&mut self) -> &mut Vec<u8> {
        &mut self.buf
    }
}

impl TarConsumer for &mut BulkCompressor<'_> {
    fn consume(&mut self, buf: &[u8]) -> io::Result<()> {
        let len = buf.len();
        let mut pos = 0;
        loop {
            pos += self
                .zstd
                .compress(buf.get(pos..).expect("buf shrunk"), &mut self.buf)?;

            if pos == len {
                break;
            }
        }

        Ok(())
    }
}
