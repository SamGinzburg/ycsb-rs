mod core_workload;

pub use core_workload::CoreWorkload;

use crate::db::DB;
use std::rc::Rc;
use std::cell::RefCell;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::db::DBType;

#[async_trait]
pub trait Workload: Sync + Send {
    async fn do_insert(&self, db: DBType);
    async fn do_update(&self, db: DBType);
    async fn do_transaction(&self, db: DBType);
}
