use std::{
    ops::{Deref, DerefMut},
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Weak,
    },
};

use anyhow::Context;
use tokio::{
    fs::{self, File},
    io::AsyncWriteExt,
    sync::{Mutex, Notify, OwnedMutexGuard},
    task,
};
use ulid::Ulid;

use crate::{util, BLOCK, BLOCK_USIZE, TMP_DIR};

pub struct FileGroup {
    inner: Option<FileGroupInner>,
    file_buf: Vec<u8>,
    len: u64,
}

struct FileGroupInner {
    file: File,
    path: PathBuf,
}

impl FileGroupInner {
    async fn new() -> anyhow::Result<Self> {
        let path = TMP_DIR.join(Ulid::new().to_string());
        Ok(Self {
            file: File::open(&path)
                .await
                .context("Failed to create file group temp file")?,
            path,
        })
    }
}

pub enum StreamGroupWrite {
    Continue,
    Checkpoint,
}

impl StreamGroupWrite {
    pub const fn checkpoint(&self) -> bool {
        matches!(*self, Self::Checkpoint)
    }
}

impl FileGroup {
    fn new() -> Self {
        Self {
            inner: None,
            file_buf: Vec::with_capacity(BLOCK_USIZE),
            len: 0,
        }
    }

    pub async fn write_full(&mut self, buf: &[u8]) -> anyhow::Result<()> {
        let buf_len = util::usize_to_u64(buf.len());
        if buf_len + self.len > BLOCK {
            self.reset().await.context("Failed to reset file group")?;
        }

        assert!(
            buf_len <= BLOCK - self.len,
            "write_all buf too big for a single block"
        );

        self.len += buf_len;
        self.inner()
            .await
            .context("Failed to get file group inner")?
            .file
            .write_all(buf)
            .await
            .context("Failed to write_all directly to file group file")
    }

    pub async fn write_stream(&mut self, buf: &[u8]) -> anyhow::Result<StreamGroupWrite> {
        let mut ret = StreamGroupWrite::Continue;
        let buf_len = util::usize_to_u64(buf.len());
        if buf_len + self.len > BLOCK {
            self.reset().await.context("Failed to reset file group")?;
            ret = StreamGroupWrite::Checkpoint;
        }

        self.len += buf_len;
        self.file_buf.extend_from_slice(buf);

        Ok(ret)
    }

    async fn inner(&mut self) -> anyhow::Result<&mut FileGroupInner> {
        if self.inner.is_none() {
            let inner = FileGroupInner::new()
                .await
                .context("Failed to create new file group inner")?;
            self.inner = Some(inner);
        }

        self.inner.as_mut().map_or_else(|| unreachable!(), Ok)
    }

    async fn reset(&mut self) -> anyhow::Result<()> {
        self.inner()
            .await
            .context("Failed to get file group inner")?;
        let mut inner = self
            .inner
            .take()
            .expect("inner should be set by above inner() call");

        if !self.file_buf.is_empty() {
            inner
                .file
                .write_all(&self.file_buf)
                .await
                .context("Failed to write file_buf to file group file")?;
        }

        self.len = 0;
        Ok(())
    }
}

impl Drop for FileGroup {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            task::spawn(async {
                drop(fs::remove_file(inner.path).await);
            });
        }
    }
}

struct ScheduledFileGroupInner {
    notifiers: Vec<Weak<Notify>>,
    file_group: FileGroup,
}

pub struct ScheduledFileGroup {
    file_group: Arc<Mutex<ScheduledFileGroupInner>>,
    file_id: Arc<AtomicUsize>,
}

pub struct FileGroupHandle {
    scheduled: Arc<Mutex<ScheduledFileGroupInner>>,
    file_id: Arc<AtomicUsize>,
    notify: Arc<Notify>,
}

impl ScheduledFileGroup {
    pub fn new() -> Self {
        Self {
            file_group: Arc::new(Mutex::new(ScheduledFileGroupInner {
                notifiers: Vec::new(),
                file_group: FileGroup::new(),
            })),
            file_id: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub async fn new_handle(&self) -> FileGroupHandle {
        let notify = Arc::new(Notify::new());
        self.file_group
            .lock()
            .await
            .notifiers
            .push(Arc::downgrade(&notify));
        FileGroupHandle {
            scheduled: Arc::clone(&self.file_group),
            file_id: Arc::clone(&self.file_id),
            notify,
        }
    }
}

impl ScheduledFileGroupInner {
    fn notify_all(&mut self) {
        self.notifiers.retain(|n| {
            n.upgrade().map_or(false, |n| {
                n.notify_one();
                true
            })
        });
    }
}

pub struct FileGroupGuard {
    guard: OwnedMutexGuard<ScheduledFileGroupInner>,
    file_id: Arc<AtomicUsize>,
}

impl Drop for FileGroupGuard {
    fn drop(&mut self) {
        self.file_id.fetch_add(1, Ordering::Release);
        self.guard.notify_all();
    }
}

impl Deref for FileGroupGuard {
    type Target = FileGroup;

    fn deref(&self) -> &Self::Target {
        &self.guard.file_group
    }
}

impl DerefMut for FileGroupGuard {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard.file_group
    }
}

impl Drop for FileGroupHandle {
    fn drop(&mut self) {
        task::block_in_place(|| {
            self.scheduled.blocking_lock().notify_all();
        });
    }
}

impl FileGroupHandle {
    pub async fn wait_for(&mut self, id: usize) -> FileGroupGuard {
        loop {
            if self.file_id.load(Ordering::Acquire) == id {
                break;
            }

            self.notify.notified().await;
        }

        FileGroupGuard {
            guard: Mutex::lock_owned(Arc::clone(&self.scheduled)).await,
            file_id: Arc::clone(&self.file_id),
        }
    }
}
