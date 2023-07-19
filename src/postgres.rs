use crate::db::DB;

use anyhow::Result;
use sql_builder::SqlBuilder;
use postgres::{Client, NoTls};
use std::collections::HashMap;
use postgres::types::ToSql;

const PRIMARY_KEY: &str = "y_id";

pub struct Postgres {
    conn: Client,
}

impl Postgres {
    pub fn new() -> Result<Self> {
        let client = Client::connect("postgresql://dboperator:password@localhost:5243/postgres", NoTls)?;
        Ok(Postgres { conn: client })
    }
}

impl DB for Postgres {
    fn init(&mut self) -> Result<()> {

        let mut query = String::from(r#"
            CREATE TABLE IF NOT EXISTS usertable
(
    primary_key VARCHAR(64) PRIMARY KEY,
    field1 VARCHAR(255),
    field2 VARCHAR(255),
    field3 VARCHAR(255),
    field4 VARCHAR(255),
    field5 VARCHAR(255),
    field6 VARCHAR(255),
    field7 VARCHAR(255),
    field8 VARCHAR(255),
    field9 VARCHAR(255)
);
"#);
        println!("{}", query);
        self.conn.execute(&query, &[]).unwrap();

        Ok(())
    }

    fn insert(&mut self, table: &str, key: &str, values: &HashMap<&str, String>) -> Result<()> {
        // TODO: cache prepared statement
        let mut sql = SqlBuilder::insert_into(table);
        let mut vals: Vec<String> = Vec::new();
        sql.field(PRIMARY_KEY);
        vals.push(format!(":{}", PRIMARY_KEY));
        for key in values.keys() {
            sql.field(key);
            let marker = format!(":{}", key);
            vals.push(marker);
        }
        sql.values(&vals);
        let sql = sql.sql()?;
        //println!("{}", sql);
        self.conn.execute(&sql, &[])?;
        Ok(())
    }

    fn update(&mut self, table: &str, key: &str, values: &HashMap<&str, String>) -> Result<()> {
        //dbg!("{}, {:?}", key, values);
        let mut sql = format!("
            UPDATE usertable
            SET field0 = $1, field1 = $2, field2 = $3, field3 = $4, field4 = $5, field5 = $6, field6 = $7, field7 = $8, field8 = $9, field9 = $10
            WHERE y_id = $11;
        ");
        //println!("{}", sql);
        let mut params: Vec<&(dyn ToSql + Sync)> = vec![];
        for (_, value) in values {
            params.push(value);
        }
        let key = String::from(key);
        params.push(&key);

        self.conn.query(&sql, params.as_slice())?;

        Ok(())
    }

    fn read(&mut self, table: &str, key: &str, result: &mut HashMap<String, String>) -> Result<()> {
        // TODO: cache prepared statement
        let mut sql = SqlBuilder::select_from(table);
        sql.field("*");
        // TODO: fields
        sql.and_where(format!("{} = $1", PRIMARY_KEY));
        let sql = sql.sql()?;
        let mut query = self.conn.prepare(&sql)?;
        let mut rows = self.conn.query(&query, &[&PRIMARY_KEY])?;

        for count in 0..rows.len() {
            for col in query.columns() {
                let key = col.name();
                let value: String = rows.get(count).unwrap().get::<_, String>(key);
                result.insert(key.to_string(), value);
            }
        }

        // TODO: results
        Ok(())
    }
}
