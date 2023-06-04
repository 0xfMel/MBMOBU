use std::{hash::Hasher, io::Cursor, path::Path, time::Duration};

use anyhow::Context;
use kanal::Receiver;
use memmap2::Mmap;
use tokio::{runtime::Handle, task, time};
use tokio_util::sync::CancellationToken;
use twox_hash::XxHash64;
use ulid::Ulid;

use crate::{
    config,
    file_group::FileGroupGuard,
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
    ) -> anyhow::Result<Option<Ulid>> {
        let ret = self
            .rt
            .block_on(file_group.write(&self.buf))
            .context("Failed to write buf to file group")?;
        self.buf.clear();
        Ok(ret)
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
    hash: XxHash64,
    pos: u64,
    chkpt_timer: CheckpointTimer,
    checkpoint: Option<Checkpoint>,
}

impl<'a, 'b> StreamCompress<'a, 'b> {
    fn new(compressor: &'b mut Compressor<'a>, file_group: &'b mut FileGroupGuard) -> Self {
        Self {
            compressor,
            file_group,
            hash: XxHash64::default(),
            pos: 0,
            chkpt_timer: CheckpointTimer::new(),
            checkpoint: None,
        }
    }
}

impl TarConsumer for StreamCompress<'_, '_> {
    fn consume(&mut self, buf: &[u8]) -> anyhow::Result<()> {
        #[allow(clippy::significant_drop_tightening)]
        for chunk in buf.chunks(DEFAULT_BUF_SIZE) {
            self.hash.write(chunk);
            let len = util::usize_to_u64(chunk.len());
            self.pos += len;

            let mut src = Cursor::new(chunk);
            loop {
                self.compressor
                    .zstd
                    .compress(&mut src, &mut self.compressor.buf)
                    .context("Failed to stream compress")?;

                if let Some(chkpt) = self.checkpoint.as_mut() {
                    chkpt.offset += util::usize_to_u64(self.compressor.buf.len());
                }

                if let Some(block_id) = self
                    .compressor
                    .block_on_stream_write(self.file_group)
                    .context("Failed to write compressed stream to file group")?
                {
                    if let Some(mut chkpt) = self.checkpoint.take() {
                        chkpt.blocks.push(block_id);
                        // TODO - handle emitted checkpoint
                        println!("{chkpt:#?}");
                    }
                }

                if src.position() == len {
                    break;
                }
            }

            if self.chkpt_timer.do_checkpoint() && self.checkpoint.is_none() {
                let checkpoint = self
                    .checkpoint
                    .insert(Checkpoint::new(self.hash.finish(), self.pos));

                loop {
                    let remaining = self
                        .compressor
                        .zstd
                        .end_frame(&mut self.compressor.buf)
                        .context("Failed to end frame for checkpoint")?;
                    if let Some(block_id) =
                        self.compressor
                            .block_on_stream_write(self.file_group)
                            .context("Failed to write compressed stream checkpoint to file group")?
                    {
                        checkpoint.blocks.push(block_id);
                    }

                    if remaining == 0 {
                        break;
                    }
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
    fn consume(&mut self, buf: &[u8]) -> anyhow::Result<()> {
        for chunk in buf.chunks(DEFAULT_BUF_SIZE) {
            let len = util::usize_to_u64(chunk.len());
            let mut src = Cursor::new(chunk);
            loop {
                self.compressor
                    .zstd
                    .compress(&mut src, &mut self.compressor.buf)
                    .context("Failed to bulk compress")?;

                if src.position() == len {
                    break;
                }
            }
        }

        Ok(())
    }
}

#[derive(Debug)]
struct Checkpoint {
    blocks: Vec<Ulid>,
    offset: u64,
    hash: u64,
    pos: u64,
}

impl Checkpoint {
    const fn new(hash: u64, pos: u64) -> Self {
        Self {
            blocks: Vec::new(),
            offset: 0,
            hash,
            pos,
        }
    }
}

struct CheckpointTimer {
    cancel: CancellationToken,
    rx: Receiver<()>,
}

impl CheckpointTimer {
    fn new() -> Self {
        let (tx, rx) = kanal::bounded(0);
        let cancel = CancellationToken::new();

        task::spawn({
            let cancel = cancel.clone();
            async move {
                let tx = tx.to_async();
                loop {
                    tokio::select! {
                        _ = cancel.cancelled() => break,
                        _ = time::sleep(Duration::from_secs(2 * 60)) => {
                            if tx.send(()).await.is_err() {
                                break;
                            }
                        }
                    }
                }
            }
        });

        Self { cancel, rx }
    }

    fn do_checkpoint(&self) -> bool {
        self.rx
            .try_recv()
            .expect("Channel should not close")
            .is_some()
    }
}

impl Drop for CheckpointTimer {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}
