#![allow(unknown_lints)]
//#![warn(missing_docs)]
#![warn(rustdoc::all)]
#![warn(absolute_paths_not_starting_with_crate)]
#![warn(elided_lifetimes_in_paths)]
#![warn(explicit_outlives_requirements)]
#![warn(fuzzy_provenance_casts)]
#![warn(let_underscore_drop)]
#![warn(lossy_provenance_casts)]
#![warn(macro_use_extern_crate)]
#![warn(meta_variable_misuse)]
#![warn(missing_debug_implementations)]
#![warn(missing_copy_implementations)]
#![warn(must_not_suspend)]
#![warn(non_ascii_idents)]
#![warn(non_exhaustive_omitted_patterns)]
#![warn(noop_method_call)]
#![warn(pointer_structural_match)]
#![warn(trivial_casts)]
#![warn(trivial_numeric_casts)]
#![warn(unused_crate_dependencies)]
#![warn(unused_extern_crates)]
#![warn(unused_import_braces)]
#![warn(unused_lifetimes)]
#![warn(unused_macro_rules)]
#![warn(unused_qualifications)]
#![warn(unused_tuple_struct_fields)]
#![warn(variant_size_differences)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![warn(clippy::cargo)]
#![warn(clippy::as_underscore)]
#![warn(clippy::assertions_on_result_states)]
#![warn(clippy::dbg_macro)]
#![warn(clippy::decimal_literal_representation)]
#![warn(clippy::default_numeric_fallback)]
#![warn(clippy::default_union_representation)]
#![warn(clippy::deref_by_slicing)]
#![warn(clippy::disallowed_script_idents)]
#![warn(clippy::else_if_without_else)]
#![warn(clippy::empty_drop)]
#![warn(clippy::empty_structs_with_brackets)]
#![warn(clippy::exit)]
#![warn(clippy::float_cmp_const)]
#![warn(clippy::format_push_string)]
#![warn(clippy::get_unwrap)]
#![warn(clippy::if_then_some_else_none)]
#![warn(clippy::indexing_slicing)]
#![warn(clippy::integer_division)]
#![warn(clippy::let_underscore_must_use)]
#![warn(clippy::lossy_float_literal)]
#![warn(clippy::map_err_ignore)]
#![warn(clippy::mixed_read_write_in_expression)]
#![warn(clippy::mod_module_files)]
#![warn(clippy::multiple_inherent_impl)]
#![warn(clippy::non_ascii_literal)]
#![warn(clippy::partial_pub_fields)]
#![warn(clippy::pattern_type_mismatch)]
#![warn(clippy::rc_buffer)]
#![warn(clippy::rc_mutex)]
#![warn(clippy::rest_pat_in_fully_bound_structs)]
#![warn(clippy::same_name_method)]
#![warn(clippy::unseparated_literal_suffix)]
#![warn(clippy::str_to_string)]
#![warn(clippy::string_add)]
#![warn(clippy::string_slice)]
#![warn(clippy::string_to_string)]
#![warn(clippy::suspicious_xor_used_as_pow)]
#![warn(clippy::todo)]
#![warn(clippy::try_err)]
#![warn(clippy::undocumented_unsafe_blocks)]
#![warn(clippy::unimplemented)]
#![warn(clippy::unnecessary_self_imports)]
#![warn(clippy::unneeded_field_pattern)]
#![warn(clippy::unwrap_used)]
#![warn(clippy::use_debug)]
#![warn(clippy::semicolon_outside_block)]
#![warn(clippy::mutex_atomic)]
#![warn(clippy::let_underscore_untyped)]
#![warn(clippy::impl_trait_in_params)]
#![warn(clippy::multiple_unsafe_ops_per_block)]
#![warn(clippy::missing_assert_message)]
#![warn(clippy::tests_outside_test_module)]
#![warn(clippy::allow_attributes)]
#![deny(unsafe_op_in_unsafe_fn)]
#![deny(clippy::clone_on_ref_ptr)]
// Cleaner in some cases
#![allow(clippy::match_bool)]
// Better with repetition
#![allow(clippy::module_name_repetitions)]
// I like my complicated functions
#![allow(clippy::too_many_lines)]
// Prefered over not having pub(crate) and being unclear about visibility
#![allow(clippy::redundant_pub_crate)]
// Not a public crate
#![allow(clippy::cargo_common_metadata)]
// Can't be easily fixed
#![allow(clippy::multiple_crate_versions)]

use std::{
    io::{self, ErrorKind, Write},
    mem::ManuallyDrop,
    ops::{Deref, DerefMut},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Weak,
    },
};

use anyhow::Context;
use memmap2::Mmap;
use once_cell::sync::Lazy;
use tokio::{
    fs::{self, File},
    io::AsyncWriteExt,
    runtime::Handle,
    sync::{Mutex, Notify, OwnedMutexGuard},
    task::{self, JoinSet},
};
use ulid::Ulid;
use walkdir::WalkDir;
use zstd_safe::{zstd_sys::ZSTD_EndDirective, InBuffer, OutBuffer};

const COMPRESS_TASKS: usize = 6;
const LEVEL: i32 = 19;
const BLOCK: u64 = (25 * 1024 * 1024) - 512;
#[allow(clippy::cast_possible_truncation)]
const BLOCK_USIZE: usize = BLOCK as usize;
const DEFAULT_BUF_SIZE: usize = 8 * 1024;
const APP_ID: &str = "mbmobu";

static TMP_DIR: Lazy<PathBuf> = Lazy::new(|| {
    Path::new("/var/tmp")
        .join(APP_ID)
        .join(users::get_current_uid().to_string())
});

#[tokio::main]
async fn main() {
    let base_path = Path::new("/data/ssd/files.bkp");
    let (walk_tx, walk_rx) = kanal::bounded(1);

    let walker = task::spawn_blocking(move || {
        let mut id = 0;
        for entry in WalkDir::new(base_path) {
            let entry = entry.context("Error walking")?;
            if !entry.file_type().is_file() {
                continue;
            }

            if walk_tx
                .send(FileEntry {
                    id,
                    path: entry.into_path(),
                })
                .is_err()
            {
                break;
            }
            id += 1;
        }

        Ok::<_, anyhow::Error>(())
    });

    let mut compress_set = JoinSet::new();
    let file_group_scheduler = ScheduledFileGroup::new();
    for _ in 0..COMPRESS_TASKS {
        let mut file_group_handle = file_group_scheduler.new_handle().await;
        let base_path = base_path.to_path_buf();
        let walk_rx = walk_rx.clone_async();
        compress_set.spawn(async move {
            let mut bulk = BulkCompressor::new().context("Failed to create bulk compressor")?;
            let mut stream =
                StreamCompressor::new().context("Failed to create stream compressor")?;

            loop {
                let Ok(FileEntry { id, path: absolute }) = walk_rx.recv().await else {
                    break;
                };

                let file = File::open(&absolute)
                    .await
                    .with_context(|| format!("Failed to open file: {}", absolute.display()))?;
                let meta = file.metadata().await.with_context(|| {
                    format!("Failed to fetch metadata for file: {}", absolute.display())
                })?;
                // SAFETY: TODO
                let mmap = unsafe { Mmap::map(&file) }.context("Failed to mmap file")?;
                drop(file);

                let mut header = tar::Header::new_gnu();
                header.set_metadata(&meta);

                let relative = absolute.strip_prefix(&base_path).with_context(|| {
                    format!("Failed to strip prefix for path: {}", absolute.display())
                })?;
                let max_compressed_size = max_compressed_size(meta.len(), relative)?;

                if max_compressed_size > BLOCK {
                    let file_group = file_group_handle.wait_for(id).await;
                    task::block_in_place(|| {
                        stream
                            .compress(file_group, &mmap, header, relative)
                            .with_context(|| {
                                format!("Failed to stream compress {}", absolute.display())
                            })
                    })?;
                } else {
                    task::block_in_place(|| {
                        bulk.compress(&mmap, header, relative).with_context(|| {
                            format!("Failed to bulk compress {}", absolute.display())
                        })
                    })?;

                    let mut file_group = file_group_handle.wait_for(id).await;
                    file_group.write_full(&bulk.buf).await.with_context(|| {
                        format!(
                            "Failed to write bulk compressed file to file group: {}",
                            absolute.display()
                        )
                    })?;
                }
            }

            Ok::<_, anyhow::Error>(())
        });
    }
    drop(walk_rx);
}

struct FileGroup {
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

enum StreamGroupWrite {
    Continue,
    Checkpoint,
}

impl StreamGroupWrite {
    const fn checkpoint(&self) -> bool {
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

    async fn write_full(&mut self, buf: &[u8]) -> anyhow::Result<()> {
        let buf_len = usize_to_u64(buf.len());
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

    async fn write_stream(&mut self, buf: &[u8]) -> anyhow::Result<StreamGroupWrite> {
        let mut ret = StreamGroupWrite::Continue;
        let buf_len = usize_to_u64(buf.len());
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

struct FileEntry {
    id: usize,
    path: PathBuf,
}

struct ScheduledFileGroupInner {
    notifiers: Vec<Weak<Notify>>,
    file_group: FileGroup,
}

struct ScheduledFileGroup {
    file_group: Arc<Mutex<ScheduledFileGroupInner>>,
    file_id: Arc<AtomicUsize>,
}

struct FileGroupHandle {
    scheduled: Arc<Mutex<ScheduledFileGroupInner>>,
    file_id: Arc<AtomicUsize>,
    notify: Arc<Notify>,
}

impl ScheduledFileGroup {
    fn new() -> Self {
        Self {
            file_group: Arc::new(Mutex::new(ScheduledFileGroupInner {
                notifiers: Vec::new(),
                file_group: FileGroup::new(),
            })),
            file_id: Arc::new(AtomicUsize::new(0)),
        }
    }

    async fn new_handle(&self) -> FileGroupHandle {
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

struct FileGroupGuard {
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
    async fn wait_for(&mut self, id: usize) -> FileGroupGuard {
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

const TAR_BLOCK: u64 = 512;
const TAR_HEADER: u64 = TAR_BLOCK;

fn max_compressed_size<P: AsRef<Path>>(mut size: u64, path: P) -> anyhow::Result<u64> {
    size += TAR_HEADER * 2; // Tar Gnu header for data & path

    // Account for possible account path added as extra block, in case the path is invalid for regular header for reasons other than length
    let path_len: u64 = usize_to_u64(path.as_ref().as_os_str().len());
    let remaining = TAR_BLOCK - (path_len % TAR_BLOCK);
    if remaining < TAR_BLOCK {
        size += remaining; // Padding
    }
    size += path_len;

    let remaining = TAR_BLOCK - (size % TAR_BLOCK);
    if remaining < TAR_BLOCK {
        size += remaining; // Padding
    }

    Ok(usize_to_u64(zstd_safe::compress_bound(
        size.try_into()
            .context("'size' is too big for compress bound calculation")?,
    )))
}

#[must_use = "No effect, conversion only"]
pub fn usize_to_u64(value: usize) -> u64 {
    value
        .try_into()
        .expect("usize value doesn't fit inside u64")
}

struct BulkCompressor<'a> {
    zstd: Zstd<'a>,
    buf: Vec<u8>,
}

impl BulkCompressor<'_> {
    fn new() -> anyhow::Result<Self> {
        let mut zstd = Zstd::new();
        zstd.set_level(LEVEL)
            .context("Failed to set zstd compression level")?;
        zstd.set_long()
            .context("Failed to set zstd long range matching")?;
        Ok(Self {
            zstd,
            buf: Vec::with_capacity(BLOCK_USIZE),
        })
    }

    fn compress<P: AsRef<Path>>(
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

struct StreamCompressor<'a> {
    zstd: Zstd<'a>,
    compress_buf: Vec<u8>,
    rt: Handle,
}

impl StreamCompressor<'_> {
    fn new() -> anyhow::Result<Self> {
        let mut zstd = Zstd::new();
        zstd.set_level(LEVEL)
            .context("Failed to set zstd compression level")?;
        zstd.set_long()
            .context("Failed to set zstd long range matching")?;
        Ok(Self {
            zstd,
            compress_buf: Vec::with_capacity(DEFAULT_BUF_SIZE),
            rt: Handle::current(),
        })
    }

    fn compress<P: AsRef<Path>>(
        &mut self,
        mut file_group: FileGroupGuard,
        mmap: &Mmap,
        mut header: tar::Header,
        relative: P,
    ) -> anyhow::Result<()> {
        TarBuilder::new(StreamTarConsumer::new(self, &mut file_group))
            .append_file(&mut header, relative, mmap)
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
        #[allow(clippy::significant_drop_tightening)]
        loop {
            pos += self.inner.zstd.compress(
                buf.get(pos..).expect("buf shrunk"),
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

trait TarConsumer {
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

struct TarBuilder<C: TarConsumer> {
    tar: ManuallyDrop<tar::Builder<TarConsumerWriter<C>>>,
}

impl<C: TarConsumer> TarBuilder<C> {
    fn new(consumer: C) -> Self {
        Self {
            tar: ManuallyDrop::new(tar::Builder::new(TarConsumerWriter { inner: consumer })),
        }
    }

    fn append_file<P: AsRef<Path>>(
        &mut self,
        header: &mut tar::Header,
        path: P,
        data: &[u8],
    ) -> io::Result<()> {
        self.tar.append_data(header, path, data)
    }
}

struct Zstd<'a> {
    cctx: zstd_safe::CCtx<'a>,
}

impl Zstd<'_> {
    pub fn new() -> Self {
        Self {
            cctx: zstd_safe::CCtx::create(),
        }
    }

    pub fn set_level(&mut self, level: i32) -> io::Result<()> {
        self.cctx
            .set_parameter(zstd_safe::CParameter::CompressionLevel(level))
            .map_err(map_zstd_error)?;
        Ok(())
    }

    pub fn set_long(&mut self) -> io::Result<()> {
        self.cctx
            .set_parameter(zstd_safe::CParameter::EnableLongDistanceMatching(true))
            .map_err(map_zstd_error)?;
        Ok(())
    }

    fn compress(&mut self, buf: &[u8], out: &mut Vec<u8>) -> io::Result<usize> {
        let mut in_buf = InBuffer::around(buf);
        let mut out_buf = OutBuffer::around_pos(out, out.len());
        self.cctx
            .compress_stream2(
                &mut out_buf,
                &mut in_buf,
                ZSTD_EndDirective::ZSTD_e_continue,
            )
            .map_err(map_zstd_error)?;
        Ok(in_buf.pos())
    }

    fn end_frame(&mut self, out: &mut Vec<u8>) -> io::Result<usize> {
        let mut in_buf = InBuffer::around(&[]);
        let mut out_buf = OutBuffer::around_pos(out, out.len());
        let remaining = self
            .cctx
            .compress_stream2(&mut out_buf, &mut in_buf, ZSTD_EndDirective::ZSTD_e_end)
            .map_err(map_zstd_error)?;
        Ok(remaining)
    }
}

fn map_zstd_error(code: usize) -> io::Error {
    let msg = zstd_safe::get_error_name(code);
    io::Error::new(ErrorKind::Other, msg.to_owned())
}
