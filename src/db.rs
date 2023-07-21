//use crate::sqlite::SQLite;
//use crate::rocksdb::RocksDB;
use crate::postgres::Postgres;

use anyhow::{anyhow, Result};
use std::collections::HashMap;
use std::sync::Arc;
use std::cell::UnsafeCell;
use tokio::sync::{Mutex, RwLock};

use async_trait::async_trait;

pub type DBType = Postgres;

#[async_trait]
pub trait DB: Send + Sync {
    async fn init(&mut self) -> Result<()>;
    async fn insert(&mut self, table: &str, key: &str, values: &HashMap<&str, String>) -> Result<()>;
    async fn update(&mut self, table: &str, key: &str, values: &HashMap<&str, String>) -> Result<()>;
    async fn read(&mut self, table: &str, key: &str, result: &mut HashMap<String, String>) -> Result<()>;
}

pub async fn create_db(db: &str) -> Result<DBType> {
    match db {
        //"sqlite" => Ok(Rc::new(RefCell::new(SQLite::new()?))),
        //"rocksdb" => Ok(Rc::new(RocksDB::new()?)),
        "postgres" => Ok(Postgres::new().await?),
        db => Err(anyhow!("{} is an invalid database name", db)),
    }
}
