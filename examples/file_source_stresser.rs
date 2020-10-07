//! This example creates a stressful scenario for the file source.
//!
//! > *Note:* Please be cautious of disk space here.
//!
//! It does this by creating some `FILE_SOURCE_STRESSER_FILE_NUMBER` files approximately
//! `FILE_SOURCE_STRESSER_FILE_SIZE` large, optionally (`FILE_SOURCE_STRESSER_GZIP`)
//! gzip'd, in `FILE_SOURCE_PATH`.
//!
//! Then, it runs Vector with the file source (with some optional given
//! `FILE_SOURCE_STRESSER_SOURCE`) into a blackhole.

use structopt::StructOpt;
use std::{
    path::{Path, PathBuf},
};
use tracing::{instrument, info};
use vector::{Result, sources::file::FileConfig};
use tokio::{io::{AsyncRead, AsyncWrite, AsyncReadExt}, fs::{create_dir_all, File}};

#[derive(StructOpt, Debug)]
struct Config {
    #[structopt(short, long, env = "FILE_SOURCE_STRESSER_FILE_NUMBER")]
    number: u64,
    #[structopt(short, long, env = "FILE_SOURCE_STRESSER_FILE_SIZE")]
    size: u64,
    #[structopt(short, long, env = "FILE_SOURCE_STRESSER_GZIP")]
    gzip: bool,
    #[structopt(short, long, env = "FILE_SOURCE_PATH", parse(from_os_str))]
    path: PathBuf,
    #[structopt(short, long, env = "FILE_SOURCE_STRESSER_SOURCE", parse(from_os_str))]
    source: PathBuf,
}

#[derive(Debug)]
struct Stresser {
    config: Config,
}

impl From<Config> for Stresser {
    fn from(config: Config) -> Self {
        Stresser {
            config
        }
    }
}

impl Stresser {
    /// Start the stressor.
    ///
    /// This will read configs, create folders, and start creating files.
    #[instrument(skip(self))]
    async fn execute(self) -> vector::Result<()> {
        let mut buf = String::new();
        File::open(self.config.source).await?
            .read_to_string(&mut buf).await?;
        let config: FileConfig = toml::from_str(&buf)?;
        info!(?config, "Source config loaded.");
        
        create_dir_all(&self.config.path);
        info!(path = ?self.config.path, "Directory created.");

        let x = (0..self.config.number).map(|i| {
            let handle = File::create(self.config.path.join(i.to_string()))?;
            handle.write()
        });

        Ok(())
    }

    /// Create 
    async fn populate(&self, path: &Path) -> vector::Result<()> {

        Ok(())
    }
}

#[tokio::main]
async fn main() -> vector::Result<()> {
    tracing_subscriber::fmt::init();

    let config = Config::from_args();
    info!(config = ?config, "Starting Stresser");
    let stresser = Stresser::from(config);
    stresser.execute().await
}