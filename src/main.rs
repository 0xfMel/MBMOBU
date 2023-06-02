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

mod bulk;
mod file_group;
mod stream;
mod tar;
mod util;
mod zstd;

use std::path::{Path, PathBuf};

use anyhow::Context;
use bulk::BulkCompressor;
use file_group::ScheduledFileGroup;
use memmap2::Mmap;
use once_cell::sync::Lazy;
use stream::StreamCompressor;
use tokio::{
    fs::File,
    task::{self, JoinSet},
};
use walkdir::WalkDir;

const COMPRESS_TASKS: usize = 6;
const ZSTD_LEVEL: i32 = 19;
const BLOCK: u64 = (25 * 1024 * 1024) - 512;
#[allow(clippy::cast_possible_truncation)]
const BLOCK_USIZE: usize = BLOCK as usize;
const DEFAULT_BUF_SIZE: usize = 8 * 1024;
const APP_ID: &str = "mbmobu";
const TAR_BLOCK: u64 = 512;
const TAR_HEADER: u64 = TAR_BLOCK;

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
                    file_group.write_full(bulk.buf()).await.with_context(|| {
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

struct FileEntry {
    id: usize,
    path: PathBuf,
}

fn max_compressed_size<P: AsRef<Path>>(mut size: u64, path: P) -> anyhow::Result<u64> {
    size += TAR_HEADER * 2; // Tar Gnu header for data & path

    // Account for possible account path added as extra block, in case the path is invalid for regular header for reasons other than length
    let path_len: u64 = util::usize_to_u64(path.as_ref().as_os_str().len());
    let remaining = TAR_BLOCK - (path_len % TAR_BLOCK);
    if remaining < TAR_BLOCK {
        size += remaining; // Padding
    }
    size += path_len;

    let remaining = TAR_BLOCK - (size % TAR_BLOCK);
    if remaining < TAR_BLOCK {
        size += remaining; // Padding
    }

    Ok(util::usize_to_u64(zstd_safe::compress_bound(
        size.try_into()
            .context("'size' is too big for compress bound calculation")?,
    )))
}
