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

use std::path::Path;

use anyhow::Context;
use memmap2::Mmap;
use tokio::{
    fs::File,
    task::{self, JoinSet},
};
use walkdir::WalkDir;

const COMPRESS_TASKS: usize = 6;
const BLOCK: u64 = (25 * 1024 * 1024) - 512;

#[tokio::main]
async fn main() {
    let base_path = Path::new("/data/ssd/files.bkp");
    let (walk_tx, walk_rx) = kanal::bounded(1);

    let walker = task::spawn_blocking(move || {
        for entry in WalkDir::new(base_path) {
            let entry = entry.context("Error walking")?;
            if !entry.file_type().is_file() {
                continue;
            }

            if walk_tx.send(entry.into_path()).is_err() {
                break;
            }
        }

        Ok::<_, anyhow::Error>(())
    });

    let mut compress_set = JoinSet::new();
    for _ in 0..COMPRESS_TASKS {
        let base_path = base_path.to_path_buf();
        let walk_rx = walk_rx.clone_async();
        compress_set.spawn(async move {
            loop {
                let Ok(asbsolute) = walk_rx.recv().await else {
                    break;
                };

                let file = File::open(&asbsolute)
                    .await
                    .with_context(|| format!("Failed to open file: {}", asbsolute.display()))?;
                let meta = file.metadata().await.with_context(|| {
                    format!("Failed to fetch metadata for file: {}", asbsolute.display())
                })?;
                // SAFETY: TODO
                let mmap = unsafe { Mmap::map(&file) }.context("Failed to mmap file")?;
                drop(file);

                let mut header = tar::Header::new_gnu();
                header.set_metadata(&meta);

                let relative = asbsolute.strip_prefix(&base_path).with_context(|| {
                    format!("Failed to strip prefix for path: {}", asbsolute.display())
                })?;
                let max_compressed_size = max_compressed_size(meta.len(), relative)?;

                if max_compressed_size > BLOCK {
                    todo!("stream compression");
                } else {
                    todo!("bulk compression");
                }
            }

            Ok::<_, anyhow::Error>(())
        });
    }
    drop(walk_rx);
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
