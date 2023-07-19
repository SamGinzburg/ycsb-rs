use crate::sqlite::SQLite;
//use crate::rocksdb::RocksDB;
use crate::postgres::Postgres;

use anyhow::{anyhow, Result};
use std::collections::HashMap;
use std::rc::Rc;
use std::cell::RefCell;

pub trait DB {
    fn init(&mut self) -> Result<()>;
    fn insert(&mut self, table: &str, key: &str, values: &HashMap<&str, String>) -> Result<()>;
    fn update(&mut self, table: &str, key: &str, values: &HashMap<&str, String>) -> Result<()>;
    fn read(&mut self, table: &str, key: &str, result: &mut HashMap<String, String>) -> Result<()>;
}

pub fn create_db(db: &str) -> Result<Rc<RefCell<dyn DB>>> {
    match db {
        "sqlite" => Ok(Rc::new(RefCell::new(SQLite::new()?))),
        //"rocksdb" => Ok(Rc::new(RocksDB::new()?)),
        "postgres" => Ok(Rc::new(RefCell::new(Postgres::new()?))),
        db => Err(anyhow!("{} is an invalid database name", db)),
    }
}
