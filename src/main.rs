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
    sync::{Mutex, MutexGuard, Notify},
    task::{self, JoinSet},
};
use ulid::Ulid;
use walkdir::WalkDir;
use zstd_safe::zstd_sys::ZSTD_EndDirective;

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
                    file_group_handle.wait_for(id).await;
                    todo!("stream compression");
                } else {
                    let bulk_buf = bulk.compress(&mmap, header, relative).with_context(|| {
                        format!("Failed to bulk compress {}", absolute.display())
                    })?;

                    let mut file_group = file_group_handle.wait_for(id).await;
                    file_group.write_all(bulk_buf).await.with_context(|| {
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

impl FileGroup {
    fn new() -> Self {
        Self {
            inner: None,
            file_buf: Vec::with_capacity(BLOCK_USIZE),
            len: 0,
        }
    }

    async fn write_all(&mut self, buf: &[u8]) -> anyhow::Result<()> {
        let buf_len = usize_to_u64(buf.len());
        if buf_len + self.len > BLOCK - self.len {
            self.reset();
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

    async fn inner(&mut self) -> anyhow::Result<&mut FileGroupInner> {
        if self.inner.is_none() {
            let inner = FileGroupInner::new()
                .await
                .context("Failed to create new file group inner")?;
            self.inner = Some(inner);
        }

        self.inner.as_mut().map_or_else(|| unreachable!(), Ok)
    }

    fn reset(&mut self) {
        self.inner.take();
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
    inner: Mutex<ScheduledFileGroupInner>,
    file_id: AtomicUsize,
}

struct FileGroupHandle {
    scheduled: Arc<ScheduledFileGroup>,
    notify: Arc<Notify>,
}

impl ScheduledFileGroup {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(ScheduledFileGroupInner {
                notifiers: Vec::new(),
                file_group: FileGroup::new(),
            }),
            file_id: AtomicUsize::new(0),
        })
    }

    async fn new_handle(self: &Arc<Self>) -> FileGroupHandle {
        let notify = Arc::new(Notify::new());
        self.inner
            .lock()
            .await
            .notifiers
            .push(Arc::downgrade(&notify));
        FileGroupHandle {
            scheduled: Arc::clone(self),
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

struct FileGroupGuard<'a> {
    guard: MutexGuard<'a, ScheduledFileGroupInner>,
    file_id: &'a AtomicUsize,
}

impl Drop for FileGroupGuard<'_> {
    fn drop(&mut self) {
        self.file_id.fetch_add(1, Ordering::Release);
        self.guard.notify_all();
    }
}

impl Deref for FileGroupGuard<'_> {
    type Target = FileGroup;

    fn deref(&self) -> &Self::Target {
        &self.guard.file_group
    }
}

impl DerefMut for FileGroupGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard.file_group
    }
}

impl Drop for FileGroupHandle {
    fn drop(&mut self) {
        task::block_in_place(|| {
            self.scheduled.inner.blocking_lock().notify_all();
        });
    }
}

impl FileGroupHandle {
    async fn wait_for(&mut self, id: usize) -> FileGroupGuard<'_> {
        loop {
            if self.scheduled.file_id.load(Ordering::Acquire) == id {
                break;
            }

            self.notify.notified().await;
        }

        FileGroupGuard {
            guard: self.scheduled.inner.lock().await,
            file_id: &self.scheduled.file_id,
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
    tar_zstd: TarZstd<'a, Vec<u8>>,
}

impl BulkCompressor<'_> {
    fn new() -> anyhow::Result<Self> {
        let mut tar_zstd = TarZstd::new(Vec::with_capacity(BLOCK_USIZE));
        tar_zstd
            .set_level(LEVEL)
            .context("Failed to set zstd compression level")?;
        tar_zstd
            .set_long()
            .context("Failed to set zstd long range matching")?;
        Ok(Self { tar_zstd })
    }

    fn compress<P: AsRef<Path>>(
        &mut self,
        mmap: &Mmap,
        mut header: tar::Header,
        relative: P,
    ) -> anyhow::Result<&mut Vec<u8>> {
        task::block_in_place(|| {
            self.tar_zstd
                .append_file(&mut header, relative, mmap)
                .context("Failed to append file to archive")?;
            self.tar_zstd
                .end_frame()
                .context("Failed to end zstd frame")?;

            Ok::<_, anyhow::Error>(self.tar_zstd.inner_mut())
        })
    }
}

struct TarZstd<'a, W: Write> {
    inner: tar::Builder<Zstd<'a, W>>,
}

impl<W: Write> TarZstd<'_, W> {
    pub fn new(inner: W) -> Self {
        Self {
            inner: tar::Builder::new(Zstd::new(inner)),
        }
    }

    pub fn set_level(&mut self, level: i32) -> io::Result<()> {
        self.inner.get_mut().set_level(level)
    }

    pub fn set_long(&mut self) -> io::Result<()> {
        self.inner.get_mut().set_long()
    }

    pub fn end_frame(&mut self) -> io::Result<()>
    where
        W: Write,
    {
        self.inner.get_mut().end_frame()
    }

    pub fn inner_mut(&mut self) -> &mut W {
        self.inner.get_mut().inner_mut()
    }

    pub fn append_file<P: AsRef<Path>>(
        &mut self,
        header: &mut tar::Header,
        path: P,
        data: &[u8],
    ) -> io::Result<()> {
        self.inner.append_data(header, path, data)
    }
}

struct Zstd<'a, W> {
    cctx: zstd_safe::CCtx<'a>,
    inner: W,
    buf: Vec<u8>,
}

impl<W> Zstd<'_, W> {
    pub fn new(inner: W) -> Self {
        Self {
            cctx: zstd_safe::CCtx::create(),
            inner,
            buf: Vec::with_capacity(DEFAULT_BUF_SIZE),
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

    pub fn end_frame(&mut self) -> io::Result<()>
    where
        W: Write,
    {
        loop {
            if self.flush_with(ZSTD_EndDirective::ZSTD_e_end)? == 0 {
                break;
            }
        }

        Ok(())
    }

    fn flush_with(&mut self, directive: ZSTD_EndDirective) -> io::Result<usize>
    where
        W: Write,
    {
        let mut in_buf = zstd_safe::InBuffer::around(&[]);
        let mut out_buf = zstd_safe::OutBuffer::around(&mut self.buf);
        let remaining = self
            .cctx
            .compress_stream2(&mut out_buf, &mut in_buf, directive)
            .map_err(map_zstd_error)?;

        self.inner.write_all(&self.buf)?;
        self.buf.clear();

        self.inner.flush()?;
        Ok(remaining)
    }

    pub fn inner_mut(&mut self) -> &mut W {
        &mut self.inner
    }
}

impl<W: Write> Write for Zstd<'_, W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        loop {
            let mut in_buf = zstd_safe::InBuffer::around(buf);
            let mut out_buf = zstd_safe::OutBuffer::around(&mut self.buf);
            self.cctx
                .compress_stream2(
                    &mut out_buf,
                    &mut in_buf,
                    ZSTD_EndDirective::ZSTD_e_continue,
                )
                .map_err(map_zstd_error)?;
            self.inner.write_all(&self.buf)?;
            self.buf.clear();

            let written = in_buf.pos();
            if written > 0 || buf.is_empty() {
                return Ok(written);
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        loop {
            if self.flush_with(ZSTD_EndDirective::ZSTD_e_flush)? == 0 {
                break;
            }
        }

        self.inner.flush()
    }
}

fn map_zstd_error(code: usize) -> io::Error {
    let msg = zstd_safe::get_error_name(code);
    io::Error::new(ErrorKind::Other, msg.to_owned())
}
