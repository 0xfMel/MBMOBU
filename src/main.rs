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

mod compression;
mod config;
mod encryption;
mod file_group;
mod tar;
mod util;

use std::{
    panic,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::Context;
use compression::Compressor;
use file_group::{ScheduledFileGroup, BLOCK_RESERVED};
use memmap2::Mmap;
use once_cell::sync::Lazy;
use tokio::{
    fs::{self, File},
    runtime,
    task::{self, JoinSet},
};
use walkdir::WalkDir;

const KB: usize = 1024;
const MB: usize = 1024 * KB;
const UPLOAD_OFFSET: usize = 512;
const BLOCK_USIZE: usize = (25 * MB) - UPLOAD_OFFSET - BLOCK_RESERVED;
const BLOCK: u64 = BLOCK_USIZE as u64;

const APP_ID: &str = "mbmobu";
const TAR_BLOCK: u64 = 512;
const TAR_HEADER: u64 = TAR_BLOCK;

static TMP_DIR: Lazy<PathBuf> = Lazy::new(|| {
    Path::new("/var/tmp")
        .join(APP_ID)
        .join(users::get_current_uid().to_string())
});

fn data_dir() -> anyhow::Result<PathBuf> {
    dirs::data_local_dir()
        .map(|d| d.join(APP_ID))
        .context("No data local directory")
}

/*
fn main() {
    use chacha20poly1305::{aead::Aead, KeyInit};
    use std::io::{Read, Write};

    let file = std::fs::File::open("/var/tmp/mbmobu/1000/01H1YZ9P248TET77KV71K3735R").unwrap();
    let mut file = std::io::BufReader::new(file);
    let pk = encryption::get_pk(data_dir().unwrap()).unwrap();
    let mut nonce = [0; 24];
    file.read_exact(&mut nonce).unwrap();
    let nonce = chacha20poly1305::XNonce::from_slice(&nonce);
    let mut key = [0; 512];
    file.read_exact(&mut key).unwrap();
    let key = pk.decrypt(rsa::Pkcs1v15Encrypt, &key).unwrap();
    let cipher = chacha20poly1305::XChaCha20Poly1305::new_from_slice(&key).unwrap();
    let mut buf = Vec::new();
    file.read_to_end(&mut buf).unwrap();
    let buf = cipher.decrypt(&nonce, buf.as_ref()).unwrap();
    std::io::stdout().write_all(&buf).unwrap();
}
*/

fn main() -> anyhow::Result<()> {
    runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .thread_keep_alive(Duration::ZERO)
        .build()
        .context("Failed to build Tokio runtime")?
        .block_on(start())
}

async fn start() -> anyhow::Result<()> {
    fs::create_dir_all(&*TMP_DIR)
        .await
        .context("Failed to create tmp dir")?;
    let data_dir = data_dir()?;
    fs::create_dir_all(&data_dir)
        .await
        .context("Failed to create data dir")?;

    let config = config::get_config(&data_dir)
        .await
        .context("Failed to get config")?;

    let pubk =
        encryption::get_pubk(&config.password, data_dir).context("Failed to get public key")?;

    let (walk_tx, walk_rx) = kanal::bounded(1);

    let walker = task::spawn_blocking(move || {
        let mut id = 0;
        for bkp_path in config.paths {
            let prefix = Path::new(&bkp_path.prefix);
            for entry in WalkDir::new(&bkp_path.path) {
                let entry = entry.context("Error walking")?;
                if !entry.file_type().is_file() {
                    continue;
                }

                let path = entry.into_path();
                let relative = path.strip_prefix(&bkp_path.path).with_context(|| {
                    format!("Failed to strip prefix for path: {}", path.display())
                })?;
                let entry_path = prefix.join(relative);

                if walk_tx
                    .send(FileEntry {
                        id,
                        path,
                        entry_path,
                    })
                    .is_err()
                {
                    break;
                }
                id += 1;
            }
        }

        Ok::<_, anyhow::Error>(())
    });

    let mut compress_set = JoinSet::new();
    let file_group_scheduler = ScheduledFileGroup::new(pubk);
    for _ in 0..config.threads {
        let mut file_group_handle = file_group_scheduler.new_handle().await;
        let walk_rx = walk_rx.clone_async();
        compress_set.spawn(async move {
            let mut compressor =
                Compressor::new(config.compression).context("Failed to create compressor")?;

            loop {
                let Ok(FileEntry { id, path, entry_path }) = walk_rx.recv().await else {
                    break;
                };

                let file = File::open(&path)
                    .await
                    .with_context(|| format!("Failed to open file: {}", path.display()))?;
                let meta = file.metadata().await.with_context(|| {
                    format!("Failed to fetch metadata for file: {}", path.display())
                })?;
                // SAFETY: TODO
                let mmap = unsafe { Mmap::map(&file) }.context("Failed to mmap file")?;
                drop(file);
                mmap.advise(memmap2::Advice::Sequential)
                    .context("Failed to advise mmap")?;

                let mut header = tar::Header::new_gnu();
                header.set_metadata(&meta);

                let max_compressed_size = max_compressed_size(meta.len(), &entry_path)?;

                if max_compressed_size > BLOCK {
                    let file_group = file_group_handle.wait_for(id).await;
                    task::block_in_place(|| {
                        compressor
                            .stream(file_group, &mmap, header, entry_path)
                            .with_context(|| {
                                format!("Failed to stream compress {}", path.display())
                            })
                    })?;
                } else {
                    task::block_in_place(|| {
                        compressor
                            .bulk(&mmap, header, entry_path)
                            .with_context(|| format!("Failed to bulk compress {}", path.display()))
                    })?;

                    {
                        let mut file_group = file_group_handle.wait_for(id).await;
                        let buf = compressor.buf();
                        file_group.write(buf).await.with_context(|| {
                            format!(
                                "Failed to write bulk compressed file to file group: {}",
                                path.display()
                            )
                        })?;
                    }

                    compressor.reset();
                }
            }

            Ok::<_, anyhow::Error>(())
        });
    }
    drop(walk_rx);

    while let Some(task) = compress_set.join_next().await {
        match task {
            Ok(Err(e)) => return Err(e.context("Compress task failed")),
            Err(e) if e.is_panic() => panic::resume_unwind(e.into_panic()),
            Ok(Ok(_)) | Err(_) => {}
        }
    }

    match walker.await {
        Ok(Err(e)) => Err(e.context("Walker task failed")),
        Err(e) if e.is_panic() => panic::resume_unwind(e.into_panic()),
        Ok(Ok(_)) | Err(_) => Ok(()),
    }
}

struct FileEntry {
    id: usize,
    path: PathBuf,
    entry_path: PathBuf,
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
