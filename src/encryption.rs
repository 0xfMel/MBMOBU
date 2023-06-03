use std::{
    fs::{self, File},
    io::{self, BufRead, BufReader, BufWriter, ErrorKind, Read, Write},
    mem,
    ops::{Deref, DerefMut},
    path::Path,
    process::Command,
    thread,
    time::Duration,
};

use anyhow::{anyhow, bail, Context};
use chacha20poly1305::{AeadCore, AeadInPlace, Key, KeyInit, XChaCha20Poly1305, XNonce};
use rand::RngCore;
use rsa::{
    pkcs1::{DecodeRsaPrivateKey, EncodeRsaPrivateKey},
    pkcs8::{DecodePublicKey, EncodePublicKey},
    RsaPrivateKey, RsaPublicKey,
};
use zeroize::{Zeroize, ZeroizeOnDrop, Zeroizing};

use crate::config::PassConfig;

const SALT_LEN: usize = 32;
pub const RSA_BITS: usize = 4096;

const KEYS_DIR: &str = "keys";
const PUBKEY_FILE: &str = "keys/pub.der";
const PRIVKEY_FILE: &str = "keys/priv.bin";

pub fn get_pubk<P: AsRef<Path>>(config: &PassConfig, data_dir: P) -> anyhow::Result<RsaPublicKey> {
    let pubk_file = data_dir.as_ref().join(PUBKEY_FILE);
    match File::open(&pubk_file) {
        Ok(mut pubk_file) => {
            let mut buf = Vec::new();
            pubk_file
                .read_to_end(&mut buf)
                .context("Failed to read from public key file")?;
            let pubk = RsaPublicKey::from_public_key_der(&buf)
                .map_err(|e| anyhow!("Invalid public key: {e}"))?;
            Ok(pubk)
        }
        Err(e) if e.kind() == ErrorKind::NotFound => {
            let pk_file = data_dir.as_ref().join(PRIVKEY_FILE);
            match File::open(pk_file)
                .map_err(Into::into)
                .and_then(|f| load_pk(&f))
            {
                Ok(pk) => {
                    let pubk = pk.to_public_key();
                    save_pubk(&pubk_file, &pubk).context("Failed to save public key")?;
                    Ok(pubk)
                }
                Err(_) => {
                    gen_key(config, data_dir, pubk_file).context("Failed to generate new keypair")
                }
            }
        }
        Err(e) => Err(anyhow!("Failed to open public key file: {e}")),
    }
}

pub fn get_pk<P: AsRef<Path>>(data_dir: P) -> anyhow::Result<RsaPrivateKey> {
    match File::open(data_dir.as_ref().join(PRIVKEY_FILE)) {
        Ok(file) => load_pk(&file),
        Err(e) if e.kind() == ErrorKind::NotFound => bail!("Private key not found"),
        Err(e) => Err(e).context("Failed to open private key file"),
    }
}

#[derive(ZeroizeOnDrop, Default)]
pub struct ZeroizeKey {
    key: Key,
}

impl Zeroize for ZeroizeKey {
    fn zeroize(&mut self) {
        self.key.as_mut_slice().zeroize();
    }
}

impl From<Key> for ZeroizeKey {
    fn from(value: Key) -> Self {
        Self { key: value }
    }
}

impl Deref for ZeroizeKey {
    type Target = Key;

    fn deref(&self) -> &Self::Target {
        &self.key
    }
}

impl DerefMut for ZeroizeKey {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.key
    }
}

fn load_pk(pk_file: &File) -> anyhow::Result<RsaPrivateKey> {
    // constant division of power of 2s by 8
    #[allow(clippy::integer_division)]
    #[derive(Default)]
    struct ChaCha {
        t_cost: [u8; (u32::BITS / 8) as usize],
        m_cost: [u8; (u32::BITS / 8) as usize],
        parallel: [u8; (u32::BITS / 8) as usize],
        salt: [u8; SALT_LEN],
        nonce: XNonce,
        pk: Vec<u8>,
    }

    let mut chacha = ChaCha::default();
    let mut pk_reader = BufReader::new(pk_file);
    pk_reader
        .read_exact(&mut chacha.t_cost)
        .context("Failed to read time cost from file")?;
    pk_reader
        .read_exact(&mut chacha.m_cost)
        .context("Failed to read memory cost from file")?;
    pk_reader
        .read_exact(&mut chacha.parallel)
        .context("Failed to read parallelism from file")?;
    pk_reader
        .read_exact(&mut chacha.salt)
        .context("Failed to read salt from file")?;
    pk_reader
        .read_exact(chacha.nonce.as_mut_slice())
        .context("Failed to read nonce from file")?;
    pk_reader
        .read_to_end(&mut chacha.pk)
        .context("Failed to read encrypted private key from file")?;

    let mut buf = Zeroizing::new(Vec::with_capacity(chacha.pk.len()));

    let buf = loop {
        let mut pass =
            get_pass("Enter encryption password: ", false).context("Failed to get password")?;
        let mut key = ZeroizeKey::default();
        let mut argon2_ctx = get_argon_ctx(
            &mut key,
            &mut pass,
            &mut chacha.salt,
            Argon2Params {
                t_cost: u32::from_le_bytes(chacha.t_cost),
                m_cost: u32::from_le_bytes(chacha.m_cost),
                parallelism: u32::from_le_bytes(chacha.parallel),
            },
        );

        eprint!("Hashing password...");
        argon2::id_ctx(&mut argon2_ctx).map_err(|e| anyhow!("Failed to hash password: {e:?}"))?;
        eprintln!();

        eprint!("Decrypting private key...");
        let chacha_cipher = XChaCha20Poly1305::new_from_slice(&key)
            .map_err(|e| anyhow!("Failed to create XChaCha20-Poly1305 cipher: {e}"))?;
        buf.extend_from_slice(&chacha.pk);
        let decrypt = chacha_cipher.decrypt_in_place(&chacha.nonce, b"", &mut *buf);
        eprintln!();

        if decrypt.is_ok() {
            break buf;
        }

        eprint!("Incorrect password.");
        thread::sleep(Duration::from_secs(3));
        eprintln!("\n");

        buf.clear();
    };

    eprintln!("Done");

    RsaPrivateKey::from_pkcs1_der(&buf)
        .map_err(|e| anyhow!("Failed to create private key from decrypted der format: {e}"))
}

fn gen_key<P1: AsRef<Path>, P2: AsRef<Path>>(
    config: &PassConfig,
    data_dir: P1,
    pubk_file: P2,
) -> anyhow::Result<RsaPublicKey> {
    let mut pass =
        get_pass("Enter new encryption password: ", true).context("Failed to get password")?;

    let mut salt = [0; SALT_LEN];
    let mut rng = rand::thread_rng();
    rng.fill_bytes(&mut salt);

    let mut key = ZeroizeKey::default();
    let mut argon2_ctx = get_argon_ctx(
        &mut key,
        &mut pass,
        &mut salt,
        Argon2Params {
            t_cost: config.time_cost,
            m_cost: config.mem_cost,
            parallelism: config.parallelism,
        },
    );

    eprint!("Hashing password...");
    argon2::id_ctx(&mut argon2_ctx).map_err(|e| anyhow!("Failed to hash password: {e:?}"))?;
    eprintln!();

    eprint!("Generating RSA key...");
    let pk = RsaPrivateKey::new(&mut rng, RSA_BITS)
        .map_err(|e| anyhow!("Failed to generate RSA key: {e}"))?;
    let pubk = pk.to_public_key();
    eprintln!();

    eprint!("Encrypting private key...");
    let chacha_cipher = XChaCha20Poly1305::new_from_slice(&key)
        .map_err(|e| anyhow!("Failed to create XChaCha20-Poly1305 cipher: {e}"))?;
    let nonce = XChaCha20Poly1305::generate_nonce(&mut rng);
    let pk_buf = {
        let der = pk
            .to_pkcs1_der()
            .map_err(|e| anyhow!("Failed to get private key in der format: {e}"))?;
        let der = der.to_bytes();
        let mut buf = Zeroizing::new(Vec::with_capacity(der.len() + 16));
        buf.extend_from_slice(&der);
        chacha_cipher
            .encrypt_in_place(&nonce, b"", &mut *buf)
            .map_err(|e| anyhow!("Failed to encrypt private key: {e}"))?;
        buf
    };
    drop(pk);
    eprintln!();

    let keys_dir = data_dir.as_ref().join(KEYS_DIR);
    match fs::create_dir(keys_dir) {
        Err(e) if e.kind() == ErrorKind::AlreadyExists => {}
        e => e.context("Failed to create keys directory")?,
    }

    let mut priv_file = BufWriter::new(
        File::create(data_dir.as_ref().join(PRIVKEY_FILE))
            .context("Failed to create private key file")?,
    );

    priv_file
        .write_all(&u32::to_le_bytes(config.time_cost))
        .context("Failed to write time cost to file")?;
    priv_file
        .write_all(&u32::to_le_bytes(config.mem_cost))
        .context("Failed to write memory cost to file")?;
    priv_file
        .write_all(&u32::to_le_bytes(config.parallelism))
        .context("Failed to write parallelism to file")?;
    priv_file
        .write_all(&salt)
        .context("Failed to write salt to file")?;
    priv_file
        .write_all(nonce.as_slice())
        .context("Failed to write nonce to file")?;
    priv_file
        .write_all(&pk_buf)
        .context("Failed to write encrypted private key to file")?;
    priv_file
        .flush()
        .context("Failed to flush private key file")?;
    drop(priv_file);

    save_pubk(pubk_file, &pubk).context("Failed to save public key")?;
    eprintln!("Done");
    Ok(pubk)
}

fn save_pubk<P: AsRef<Path>>(pubk_file: P, pubk: &RsaPublicKey) -> anyhow::Result<()> {
    let pubk_der = pubk
        .to_public_key_der()
        .map_err(|e| anyhow!("Failed to get public key in der format: {e}"))?;
    File::create(pubk_file.as_ref())
        .context("Failed to create public key file")?
        .write_all(pubk_der.as_bytes())
        .context("Failed to write to public key file")
}

fn get_pass(prompt: &'static str, check: bool) -> anyhow::Result<Zeroizing<Vec<u8>>> {
    let pass = loop {
        let pass = read_line(prompt).context("Failed to get password")?;

        if check {
            let check_pass =
                read_line("Confirm password: ").context("Failed to get confirmation password")?;

            if *pass != *check_pass {
                eprintln!("Passwords don't match\n");
                continue;
            }
        }

        break pass;
    };

    let pass_bytes = pass.as_bytes();
    let mut pass = Zeroizing::new(Vec::with_capacity(pass_bytes.len()));
    pass.extend_from_slice(pass_bytes);
    Ok(pass)
}

fn read_line(prompt: &'static str) -> anyhow::Result<Zeroizing<String>> {
    let mut stdin = io::stdin().lock();
    let mut pass = Zeroizing::new(String::new());
    let echo_guard = NoEchoGuard::new()?;
    eprint!("{prompt}");
    stdin
        .read_line(&mut pass)
        .context("Failed to read password from stdin")?;
    drop(stdin);
    eprintln!();
    echo_guard.reenable()?;
    Ok(pass)
}

struct NoEchoGuard;

impl NoEchoGuard {
    fn new() -> anyhow::Result<Self> {
        echo(false).context("Failed to disable terminal echo")?;
        Ok(Self)
    }

    fn reenable(self) -> anyhow::Result<()> {
        echo(true).context("Failed to reenable terminal echo")?;
        mem::forget(self);
        Ok(())
    }
}

impl Drop for NoEchoGuard {
    fn drop(&mut self) {
        drop(echo(true));
    }
}

fn echo(on: bool) -> io::Result<()> {
    let echo = if on { "echo" } else { "-echo" };
    Command::new("stty").arg(echo).status()?;
    Ok(())
}

#[derive(Clone, Copy)]
struct Argon2Params {
    t_cost: u32,
    m_cost: u32,
    parallelism: u32,
}

fn get_argon_ctx<'o, 'p, 'sa, 'a>(
    key: &'o mut [u8],
    pass: &'p mut [u8],
    salt: &'sa mut [u8],
    params: Argon2Params,
) -> argon2::Context<'o, 'p, 'sa, 'a, 'a> {
    argon2::Context {
        out: key,
        pwd: Some(pass),
        salt: Some(salt),
        secret: None,
        ad: None,
        t_cost: params.t_cost,
        m_cost: params.m_cost,
        lanes: params.parallelism,
        threads: params.parallelism,
        version: argon2::Version::Version13,
        flags: argon2::Flags::CLEAR_PASSWORD,
    }
}
