//! An utility to keep a cursor of the already processed blocks
//!
//! This is a helper to maintain stateful cursors of the blocks already
//! processed by a pipeline. A source should use this utility to check the
//! initial point from where it should start reading. A sink should use this
//! utility to persist the position once a block has been processed.

use r2d2_redis::{
    r2d2::{self, Pool},
    redis::Commands,
    RedisConnectionManager,
};
use std::{
    sync::RwLock,
    time::{Duration, Instant},
};

use serde::Deserialize;

use crate::Error;

pub use crate::sources::PointArg;

pub(crate) trait CanStore {
    fn read_cursor(&self) -> Result<PointArg, Error>;
    fn write_cursor(&self, point: PointArg) -> Result<(), Error>;
}

/// Configuration for the file-based storage implementation
#[derive(Debug, Deserialize)]
pub struct FileConfig {
    pub path: String,
}

#[derive(Debug, Deserialize)]
pub struct RedisConfig {
    pub url: String,
    pub key: String,
}

/// A cursor provider that uses the file system as the source for persistence
pub(crate) struct FileStorage(FileConfig);

/// An ephemeral cursor that lives only in memory
pub(crate) struct MemoryStorage(PointArg);

pub(crate) struct RedisStorage(RedisConfig);

enum Storage {
    File(FileStorage),
    Memory(MemoryStorage),
    Redis(RedisStorage),
}

impl CanStore for Storage {
    fn read_cursor(&self) -> Result<PointArg, Error> {
        match self {
            Storage::File(x) => x.read_cursor(),
            Storage::Memory(x) => x.read_cursor(),
            Storage::Redis(x) => x.read_cursor(),
        }
    }

    fn write_cursor(&self, point: PointArg) -> Result<(), Error> {
        match self {
            Storage::File(x) => x.write_cursor(point),
            Storage::Memory(x) => x.write_cursor(point),
            Storage::Redis(x) => x.write_cursor(point),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
pub enum Config {
    File(FileConfig),
    Memory(PointArg),
    Redis(RedisConfig),
}

#[derive(Clone)]
enum State {
    Unknown,
    Invalid,
    AtPoint { point: PointArg, reached: Instant },
}

pub struct Provider {
    storage: Storage,
    state: RwLock<State>,
}

impl Provider {
    fn new(config: Config) -> Self {
        Self {
            state: RwLock::new(State::Unknown),
            storage: match config {
                Config::File(x) => Storage::File(FileStorage(x)),
                Config::Memory(x) => Storage::Memory(MemoryStorage(x)),
                Config::Redis(x) => Storage::Redis(RedisStorage(x)),
            },
        }
    }

    pub fn initialize(config: Config) -> Self {
        let new = Provider::new(config);
        new.load_cursor();

        new
    }

    fn load_cursor(&self) {
        let mut guard = self.state.write().expect("error prior to acquiring lock");

        let maybe_point = self.storage.read_cursor();

        if let Err(error) = &maybe_point {
            log::warn!("failure reading cursor from storage: {}", error);
        }

        let state = match maybe_point {
            Ok(point) => State::AtPoint {
                point,
                reached: Instant::now(),
            },
            Err(_) => State::Invalid,
        };

        *guard = state;
    }

    pub fn get_cursor(&self) -> Option<PointArg> {
        let guard = self.state.read().expect("error prior to acquiring lock");

        match &*guard {
            State::AtPoint { point, .. } => Some(point.clone()),
            _ => None,
        }
    }

    pub fn set_cursor(&self, point: PointArg) -> Result<(), Error> {
        let mut guard = self.state.write().unwrap();

        let should_update = match &*guard {
            State::AtPoint { reached, .. } => reached.elapsed() > Duration::from_secs(10),
            _ => true,
        };

        if should_update {
            self.storage.write_cursor(point.clone())?;

            *guard = State::AtPoint {
                reached: Instant::now(),
                point,
            };
        }

        Ok(())
    }
}

impl CanStore for FileStorage {
    fn read_cursor(&self) -> Result<PointArg, Error> {
        let file = std::fs::read_to_string(&self.0.path)?;
        file.parse()
    }

    fn write_cursor(&self, point: PointArg) -> Result<(), Error> {
        // we save to a tmp file and then rename to make it an atomic operation. If the
        // write were to fail, the only affected file will be the temporal one.
        let tmp_file = format!("{}.tmp", self.0.path);
        std::fs::write(&tmp_file, point.to_string().as_bytes())?;
        std::fs::rename(&tmp_file, &self.0.path)?;

        Ok(())
    }
}

impl CanStore for MemoryStorage {
    fn read_cursor(&self) -> Result<PointArg, Error> {
        Ok(self.0.clone())
    }

    fn write_cursor(&self, _point: PointArg) -> Result<(), Error> {
        // No operation, memory storage doesn't persist anything
        Ok(())
    }
}

impl RedisStorage {
    pub fn get_pool(&self) -> Result<Pool<RedisConnectionManager>, Error> {
        let manager = RedisConnectionManager::new(self.0.url.clone())?;
        let pool = r2d2::Pool::builder().build(manager)?;
        Ok(pool)
    }
}

impl CanStore for RedisStorage {
    fn read_cursor(&self) -> Result<PointArg, Error> {
        let pool = self.get_pool()?;
        let mut conn = pool.get()?;
        // let data: String = conn.get("oura-cursor")?;
        let data: String = conn.get(self.0.key.clone())?;
        let point: PointArg = serde_json::from_str(&data)?;
        Ok(point)
    }

    fn write_cursor(&self, point: PointArg) -> Result<(), Error> {
        let pool = self.get_pool()?;
        let mut conn = pool.get()?;
        let data_to_write = serde_json::to_string(&point)?;
        // conn.set("oura-cursor", data_to_write)?;
        conn.set(self.0.key.clone(), data_to_write)?;
        Ok(())
    }
}
