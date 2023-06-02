use std::{
    io::{self, ErrorKind},
    path::Path,
};

use anyhow::Context;
use memmap2::Mmap;
use tokio::runtime::Handle;

use crate::{
    config,
    file_group::{FileGroupGuard, StreamGroupWrite},
    tar::{self, TarBuilder, TarConsumer},
    zstd::Zstd,
    DEFAULT_BUF_SIZE,
};

pub struct StreamCompressor<'a> {
    zstd: Zstd<'a>,
    compress_buf: Vec<u8>,
    rt: Handle,
}

impl StreamCompressor<'_> {
    pub fn new(config: config::Compression) -> anyhow::Result<Self> {
        let mut zstd = Zstd::new();
        zstd.set_level(config.level)
            .context("Failed to set zstd compression level")?;
        zstd.set_long(config.long)
            .context("Failed to set zstd long range matching")?;
        Ok(Self {
            zstd,
            compress_buf: Vec::with_capacity(DEFAULT_BUF_SIZE),
            rt: Handle::current(),
        })
    }

    pub fn compress<P: AsRef<Path>>(
        &mut self,
        mut file_group: FileGroupGuard,
        mmap: &Mmap,
        mut header: tar::Header,
        path: P,
    ) -> anyhow::Result<()> {
        TarBuilder::new(StreamTarConsumer::new(self, &mut file_group))
            .append_file(&mut header, path, mmap)
            .context("Failed to append file to archive")?;

        loop {
            let remaining = self
                .zstd
                .end_frame(&mut self.compress_buf)
                .context("Failed to end zstd frame")?;
            self.block_write_stream(&mut file_group)?;

            if remaining == 0 {
                break;
            }
        }

        Ok(())
    }

    fn block_write_stream(
        &mut self,
        file_group: &mut FileGroupGuard,
    ) -> anyhow::Result<StreamGroupWrite> {
        let ret = self
            .rt
            .block_on(file_group.write_stream(&self.compress_buf))
            .context("Failed to write compress_buf to file group")?;
        self.compress_buf.clear();
        Ok(ret)
    }
}

struct StreamTarConsumer<'a, 'b> {
    inner: &'b mut StreamCompressor<'a>,
    file_group: &'b mut FileGroupGuard,
}

impl<'a, 'b> StreamTarConsumer<'a, 'b> {
    fn new(inner: &'b mut StreamCompressor<'a>, file_group: &'b mut FileGroupGuard) -> Self {
        Self { inner, file_group }
    }
}

impl TarConsumer for StreamTarConsumer<'_, '_> {
    fn consume(&mut self, buf: &[u8]) -> io::Result<()> {
        fn block_write_stream(
            this: &mut StreamCompressor<'_>,
            file_group: &mut FileGroupGuard,
        ) -> io::Result<StreamGroupWrite> {
            this.block_write_stream(file_group)
                .map_err(|e| io::Error::new(ErrorKind::Other, e))
        }

        let len = buf.len();
        let mut pos = 0;
        // No idea why this is triggered - error message is broken, possible false-positive
        #[allow(clippy::significant_drop_tightening)]
        loop {
            pos += self.inner.zstd.compress(
                buf.get(pos..).expect("pos should not be out of range"),
                &mut self.inner.compress_buf,
            )?;

            let chkpt = block_write_stream(self.inner, self.file_group)?;

            if chkpt.checkpoint() {
                loop {
                    let remaining = self.inner.zstd.end_frame(&mut self.inner.compress_buf)?;
                    block_write_stream(self.inner, self.file_group)?;

                    if remaining == 0 {
                        break;
                    }
                }

                // TODO - checkpoint
            }

            if pos == len {
                break;
            }
        }
        Ok(())
    }
}
