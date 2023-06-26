use rocksdb::DB as RDB;
use crate::db::DB;
use anyhow::Result;
use std::collections::HashMap;

pub struct RocksDB {
    db: RDB,
}

impl RocksDB {
    pub fn new() -> Result<Self> {
        let path = "rocksdbtemp/";
        let db = RDB::open_default(path).unwrap();
        Ok(RocksDB { db })
    }
}


impl DB for RocksDB {
    fn init(&self) -> Result<()> {

        Ok(())
    }


    fn insert(&self, table: &str, key: &str, values: &HashMap<&str, String>)
              -> Result<()> {

        // marshal values into byte array

        // put byte array
        Ok(())
    }

    fn update(&self, table: &str, key: &str, values: &HashMap<&str, String>)
              -> Result<()> {

        Ok(())
    }

    fn read(&self, table: &str, key: &str, result: &mut HashMap<String, String>)
            -> Result<()> {
        Ok(())
    }
}
