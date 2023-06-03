use std::{
    ops::{Deref, DerefMut},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Weak,
    },
};

use anyhow::{anyhow, Context};
use chacha20poly1305::{AeadCore, AeadInPlace, KeyInit, XChaCha20Poly1305, XNonce};
use rsa::{Pkcs1v15Encrypt, RsaPublicKey};
use tokio::{
    fs::File,
    io::AsyncWriteExt,
    sync::{Mutex, Notify, OwnedMutexGuard},
    task,
};
use ulid::Ulid;

use crate::{
    encryption::{ZeroizeKey, RSA_BITS},
    util, BLOCK, BLOCK_USIZE, TMP_DIR,
};

const NONCE_LEN: usize = 24;
// constant division of power of 2s by 8
#[allow(clippy::integer_division)]
const ENCRYPTED_KEY_LEN: usize = RSA_BITS / 8;
const BLOCK_HEADER: usize = NONCE_LEN + ENCRYPTED_KEY_LEN;
// Reserved for
// a) block header
// b) block footer (TODO) - 63 bit counter + 1 bit flag
// c) AEAD authentication
pub const BLOCK_RESERVED: usize = BLOCK_HEADER + 8 + 16;

pub struct FileGroup {
    file_buf: Vec<u8>,
    pubk: RsaPublicKey,
    len: u64,
}

struct GroupKeyNonce {
    key: ZeroizeKey,
    nonce: XNonce,
}

impl GroupKeyNonce {
    fn generate() -> Self {
        let mut rng = rand::thread_rng();
        Self {
            key: XChaCha20Poly1305::generate_key(&mut rng).into(),
            nonce: XChaCha20Poly1305::generate_nonce(&mut rng),
        }
    }
}

pub enum HasReset {
    Yes,
    No,
}

impl HasReset {
    pub const fn has_reset(&self) -> bool {
        matches!(*self, Self::Yes)
    }
}

impl FileGroup {
    fn new(pubk: RsaPublicKey) -> Self {
        let mut file_buf = Vec::with_capacity(BLOCK_USIZE + BLOCK_RESERVED);
        file_buf.resize(BLOCK_HEADER, 0);
        Self {
            file_buf,
            pubk,
            len: 0,
        }
    }

    pub async fn write(&mut self, buf: &[u8]) -> anyhow::Result<HasReset> {
        let buf_len = util::usize_to_u64(buf.len());
        let has_reset = if buf_len + self.len > BLOCK {
            self.reset().await.context("Failed to reset file group")?;
            HasReset::Yes
        } else {
            HasReset::No
        };

        assert!(
            buf_len <= BLOCK - self.len,
            "write_all buf too big for a single block"
        );

        self.len += buf_len;
        self.file_buf.extend_from_slice(buf);
        Ok(has_reset)
    }

    pub async fn reset(&mut self) -> anyhow::Result<()> {
        if !self.file_buf.is_empty() {
            let key_nonce = GroupKeyNonce::generate();
            let cipher = XChaCha20Poly1305::new(&key_nonce.key);

            let tag = cipher
                .encrypt_in_place_detached(
                    &key_nonce.nonce,
                    b"",
                    self.file_buf
                        .get_mut(BLOCK_HEADER..)
                        .expect("file_buf length should be greater than the block header"),
                )
                .map_err(|e| anyhow!("Failed to encrypt file_buf: {e}"))?;
            self.file_buf.extend_from_slice(&tag);

            let encryped_key = self
                .pubk
                .encrypt(
                    &mut rand::thread_rng(),
                    Pkcs1v15Encrypt,
                    key_nonce.key.as_slice(),
                )
                .map_err(|e| anyhow!("Failed to encrypto XChaCha20-Poly1305 key: {e}"))?;

            self.file_buf
                .get_mut(0..NONCE_LEN)
                .expect("file_buf length should be greater than the block header")
                .copy_from_slice(key_nonce.nonce.as_slice());
            self.file_buf
                .get_mut(NONCE_LEN..(NONCE_LEN + ENCRYPTED_KEY_LEN))
                .expect("file_buf length should be greater than the block header")
                .copy_from_slice(&encryped_key);

            let path = TMP_DIR.join(Ulid::new().to_string());
            let mut file = File::create(&path)
                .await
                .context("Failed to create file group temp file")?;

            file.write_all(&self.file_buf)
                .await
                .context("Failed to write file_buf to file group file")?;
        }

        self.file_buf.truncate(BLOCK_HEADER);
        self.len = 0;
        Ok(())
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
    pub fn new(pubk: RsaPublicKey) -> Self {
        Self {
            file_group: Arc::new(Mutex::new(ScheduledFileGroupInner {
                notifiers: Vec::new(),
                file_group: FileGroup::new(pubk),
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
