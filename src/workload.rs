mod core_workload;

pub use core_workload::CoreWorkload;

use crate::db::DB;
use std::rc::Rc;
use std::cell::RefCell;

pub trait Workload {
    fn do_insert(&self, db: Rc<RefCell<dyn DB>>);
    fn do_update(&self, db: Rc<RefCell<dyn DB>>);
    fn do_transaction(&self, db: Rc<RefCell<dyn DB>>);
}
