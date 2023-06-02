use std::{
    io::ErrorKind,
    path::{Path, PathBuf},
};

use anyhow::Context;
use serde::{Deserialize, Serialize};
use tokio::fs;

const CONFIG_FILE: &str = "config.toml";
const DEFAULT_CFG: &str = "\
threads = 6
version = 0

#[[paths]]
#prefix = \"path1\"
#path = \"/example/absolute/path1\"

#[[paths]]
#prefix = \"path2\"
#path = \"/example/absolute/path2\"

[compression]
level = 19
long = true

[password]
time_cost = 12
mem_cost = 262144
parallelism = 8
";

#[derive(Serialize, Deserialize)]
pub struct PassConfig {
    pub time_cost: u32,
    pub mem_cost: u32,
    pub parallelism: u32,
}

#[derive(Serialize, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub paths: Vec<BackupPath>,
    pub threads: usize,
    pub compression: Compression,
    pub password: PassConfig,
    pub version: u8,
}

#[derive(Serialize, Deserialize)]
pub struct BackupPath {
    pub prefix: String,
    pub path: PathBuf,
}

#[derive(Clone, Copy, Serialize, Deserialize)]
pub struct Compression {
    pub level: i32,
    pub long: bool,
}

impl Default for Config {
    fn default() -> Self {
        toml::from_str(DEFAULT_CFG).expect("Failed to parse default config")
    }
}

pub async fn get_config<P: AsRef<Path> + Send>(data_dir: P) -> anyhow::Result<Config> {
    let path = data_dir.as_ref().join(CONFIG_FILE);
    let cfg = match fs::read_to_string(&path).await {
        Ok(c) => toml::from_str(&c).context("Failed to parse config")?,
        Err(e) if e.kind() == ErrorKind::NotFound => {
            let cfg = Config::default();
            fs::write(path, DEFAULT_CFG)
                .await
                .context("Failed to write defaults to config file")?;
            cfg
        }
        Err(e) => return Err(e).context("Error opening config file"),
    };

    Ok(cfg)
}
