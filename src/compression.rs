use std::{
    io::{self, Cursor, ErrorKind},
    path::Path,
};

use anyhow::Context;
use memmap2::Mmap;
use tokio::runtime::Handle;

use crate::{
    config,
    file_group::{FileGroupGuard, HasReset},
    tar::{self, TarBuilder, TarConsumer},
    util, BLOCK_USIZE,
};

use self::zstd::Zstd;

mod zstd;

const DEFAULT_BUF_SIZE: usize = 8 * 1024;

pub struct Compressor<'a> {
    zstd: Zstd<'a>,
    buf: Vec<u8>,
    rt: Handle,
}

impl Compressor<'_> {
    pub fn new(config: config::Compression) -> anyhow::Result<Self> {
        let mut zstd = Zstd::new();
        zstd.set_level(config.level)
            .context("Failed to set zstd compression level")?;
        zstd.set_long(config.long)
            .context("Failed to set zstd long range matching")?;
        Ok(Self {
            zstd,
            buf: Vec::with_capacity(BLOCK_USIZE),
            rt: Handle::current(),
        })
    }

    pub fn bulk<P: AsRef<Path>>(
        &mut self,
        mmap: &Mmap,
        mut header: tar::Header,
        path: P,
    ) -> anyhow::Result<()> {
        TarBuilder::new(BulkCompress::new(&mut *self))
            .append_file(&mut header, path, mmap)
            .context("Failed to append file to archive")?;

        loop {
            if self
                .zstd
                .end_frame(&mut self.buf)
                .context("Failed to end frame")?
                == 0
            {
                break;
            }
        }

        Ok(())
    }

    pub fn stream<P: AsRef<Path>>(
        &mut self,
        mut file_group: FileGroupGuard,
        mmap: &Mmap,
        mut header: tar::Header,
        path: P,
    ) -> anyhow::Result<()> {
        TarBuilder::new(StreamCompress::new(self, &mut file_group))
            .append_file(&mut header, path, mmap)
            .context("Failed to append file to archive")?;

        loop {
            let remaining = self
                .zstd
                .end_frame(&mut self.buf)
                .context("Failed to end frame")?;
            self.block_on_stream_write(&mut file_group)?;

            if remaining == 0 {
                break;
            }
        }

        Ok(())
    }

    fn block_on_stream_write(
        &mut self,
        file_group: &mut FileGroupGuard,
    ) -> anyhow::Result<HasReset> {
        let reset = self
            .rt
            .block_on(file_group.write(&self.buf))
            .context("Failed to write buf to file group")?;
        self.buf.clear();
        Ok(reset)
    }

    pub const fn buf(&self) -> &Vec<u8> {
        &self.buf
    }

    pub fn reset(&mut self) {
        self.buf.clear();
    }
}

struct StreamCompress<'a, 'b> {
    compressor: &'b mut Compressor<'a>,
    file_group: &'b mut FileGroupGuard,
}

impl<'a, 'b> StreamCompress<'a, 'b> {
    fn new(compressor: &'b mut Compressor<'a>, file_group: &'b mut FileGroupGuard) -> Self {
        Self {
            compressor,
            file_group,
        }
    }
}

impl TarConsumer for StreamCompress<'_, '_> {
    fn consume(&mut self, buf: &[u8]) -> io::Result<()> {
        for chunk in buf.chunks(DEFAULT_BUF_SIZE) {
            let len = util::usize_to_u64(chunk.len());
            let mut src = Cursor::new(chunk);
            #[allow(clippy::significant_drop_tightening)]
            loop {
                self.compressor
                    .zstd
                    .compress(&mut src, &mut self.compressor.buf)?;
                if self
                    .compressor
                    .block_on_stream_write(self.file_group)
                    .map_err(|e| io::Error::new(ErrorKind::Other, e))?
                    .has_reset()
                {
                    // TODO - checkpoints
                }

                if src.position() == len {
                    break;
                }
            }
        }

        Ok(())
    }
}

struct BulkCompress<'a, 'b> {
    compressor: &'b mut Compressor<'a>,
}

impl<'a, 'b> BulkCompress<'a, 'b> {
    fn new(compressor: &'b mut Compressor<'a>) -> Self {
        Self { compressor }
    }
}

impl TarConsumer for BulkCompress<'_, '_> {
    fn consume(&mut self, buf: &[u8]) -> std::io::Result<()> {
        for chunk in buf.chunks(DEFAULT_BUF_SIZE) {
            let len = util::usize_to_u64(chunk.len());
            let mut src = Cursor::new(chunk);
            loop {
                self.compressor
                    .zstd
                    .compress(&mut src, &mut self.compressor.buf)?;

                if src.position() == len {
                    break;
                }
            }
        }

        Ok(())
    }
}
