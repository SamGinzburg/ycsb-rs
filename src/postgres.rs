use crate::db::DB;

use anyhow::Result;
use sql_builder::SqlBuilder;
use tokio_postgres::{Config, Client, NoTls};
use std::collections::HashMap;
use tokio_postgres::types::ToSql;
use std::time::Duration;
use tokio::task;
use tokio::*;

const PRIMARY_KEY: &str = "y_id";

pub struct Postgres {
    conn: Client,
    runtime: tokio::runtime::Runtime,
}

impl Postgres {
    pub fn new() -> Result<Self> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let client = runtime.block_on(async {
            let mut config = Config::new();
            let (client, connection) = config
                           .tcp_user_timeout(Duration::from_millis(500))
                           .connect_timeout(Duration::from_millis(500))
                           .user("dboperator")
                           .dbname("postgres")
                           .password("password")
                           .host("localhost")
                           .port(5243)
                           .connect(NoTls).await.unwrap();
            dbg!(client.is_closed());
            tokio::spawn( async move { connection.await.unwrap() } );
            dbg!(client.is_closed());

            client
        });

        Ok(Postgres { conn: client, runtime: runtime })
    }
}

impl DB for Postgres {
    fn init(&mut self) -> Result<()> {

        let mut query = String::from(r#"
CREATE TABLE IF NOT EXISTS usertable
(
    y_id VARCHAR(64) PRIMARY KEY,
    field0 VARCHAR(255),
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

        self.runtime.block_on(async {
            //self.conn.execute("DROP TABLE IF EXISTS usertable;", &[]).await.unwrap();
            self.conn.execute(&query, &[]).await.unwrap();
        });
        Ok(())
    }

    fn insert(&mut self, table: &str, key: &str, values: &HashMap<&str, String>) -> Result<()> {
        // TODO: cache prepared statement
        let mut sql = format!("
            INSERT INTO usertable (y_id, field0, field1, field2, field3, field4, field5, field6, field7, field8, field9)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (y_id)
            DO NOTHING;
        ");
        //println!("{}", sql);
        let mut params: Vec<&(dyn ToSql + Sync)> = vec![];
        let key = String::from(key);
        params.push(&key);

        for (_, value) in values {
            params.push(value);
        }
        //let key = String::from(key);
        //params.push(&key);

        self.runtime.block_on(async {
            self.conn.query(&sql, params.as_slice()).await.unwrap();
        });

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

        self.runtime.block_on(async {
            self.conn.query(&sql, params.as_slice()).await.unwrap();
        });

        Ok(())
    }

    fn read(&mut self, table: &str, key: &str, result: &mut HashMap<String, String>) -> Result<()> {
        // TODO: cache prepared statement
        let mut sql = SqlBuilder::select_from(table);
        sql.field("*");
        // TODO: fields
        sql.and_where(format!("{} = $1", PRIMARY_KEY));
        let sql = sql.sql()?;

        let (query, rows) = self.runtime.block_on(async {
            let query = self.conn.prepare(&sql).await.unwrap();
            let rows = self.conn.query(&query, &[&key]).await.unwrap();
            (query, rows)
        });

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
