use crate::db::DB;
use crate::workload::Workload;
use rand::distributions::{Alphanumeric, DistString};
use rand::rngs::SmallRng;
use rand::SeedableRng;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
//use std::cell::RefCell;
use async_trait::async_trait;
use std::borrow::BorrowMut;
use crate::db::DBType;
use tokio::time::{timeout, Duration};

use crate::generator::{
    AcknowledgedCounterGenerator, ConstantGenerator, CounterGenerator, DiscreteGenerator,
    Generator, UniformLongGenerator, WeightPair, ZipfianGenerator,
};
use crate::properties::Properties;

#[derive(Copy, Clone, Debug)]
pub enum CoreOperation {
    Read,
    Update,
    Insert,
    Scan,
    ReadModifyWrite,
}

impl std::fmt::Display for CoreOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[allow(dead_code)]
pub struct CoreWorkload {
    rng: std::sync::Mutex<SmallRng>,
    table: String,
    field_count: u64,
    field_names: Vec<String>,
    field_length_generator: std::sync::Mutex<Box<dyn Generator<u64> + Send>>,
    read_all_fields: bool,
    write_all_fields: bool,
    data_integrity: bool,
    key_sequence: std::sync::Mutex<Box<dyn Generator<u64> + Send>>,
    operation_chooser: std::sync::Mutex<DiscreteGenerator<CoreOperation>>,
    key_chooser: std::sync::Mutex<Box<dyn Generator<u64> + Send>>,
    //field_chooser: Box<dyn Generator<String>>,
    transaction_insert_key_sequence: std::sync::Mutex<AcknowledgedCounterGenerator>,
    //scan_length: Box<dyn Generator<u64>>,
    ordered_inserts: bool,
    record_count: usize,
    zero_padding: usize,
    insertion_retry_limit: u64,
    insertion_retry_interval: u64,
    timeout: u64,
    retries: u64,
    sender: crate::SenderType,
}

impl CoreWorkload {
    pub fn new(prop: &Properties, opt: &crate::Opt, sender: crate::SenderType) -> Self {
        let rng = SmallRng::from_entropy();
        let field_name_prefix = "field";
        let field_count = 10;
        let mut field_names = vec![];
        for i in 0..field_count {
            field_names.push(format!("{}{}", field_name_prefix, i));
        }
        CoreWorkload {
            rng: std::sync::Mutex::new(rng),
            table: String::from("usertable"),
            field_count,
            field_names,
            field_length_generator: std::sync::Mutex::new(get_field_length_generator(prop)),
            read_all_fields: true,
            write_all_fields: true,
            data_integrity: true,
            key_sequence: std::sync::Mutex::new(Box::new(CounterGenerator::new(prop.insert_start))),
            operation_chooser: std::sync::Mutex::new(create_operation_generator(prop)),
            key_chooser: std::sync::Mutex::new(get_key_chooser_generator(prop)),
            //field_chooser: Box<dyn Generator<String>>,
            transaction_insert_key_sequence: std::sync::Mutex::new(AcknowledgedCounterGenerator::new(1)),
            //scan_length: Box<dyn Generator<u64>>,
            ordered_inserts: true,
            record_count: 1,
            zero_padding: 1,
            insertion_retry_limit: 0,
            insertion_retry_interval: 0,
            timeout: opt.timeout, // ms
            retries: opt.retries,
            sender: sender,
        }
    }

    async fn do_transaction_read(&self, mut db: DBType) {
        let keynum = self.next_key_num();
        let dbkey = format!("{}", fnvhash64(keynum));
        let mut result = HashMap::new();
        let mut retry = self.retries;
        while retry > 0 {
            let now = std::time::Instant::now();
            let fut = db.read(&self.table, &dbkey, &mut result);
            match timeout(Duration::from_millis(self.timeout), fut).await {
                Err(_) => {
                    //println!("read timeout 100ms");
                    self.sender.send(crate::Request { latency: now.elapsed().as_millis(), success: false }).unwrap();
                    retry -= 1;
                },
                Ok(result) => {
                    result.unwrap();
                    // read succeeded
                    //
                    // If we previously failed, continue until retry == 0 to simulate workload amplification
                    self.sender.send(crate::Request { latency: now.elapsed().as_millis(), success: true }).unwrap();
                    if retry != self.retries {
                        retry -= 1;
                        continue;
                    }
                    break;
                },
            }
        }

        // TODO: verify rows
    }

    fn next_key_num(&self) -> u64 {
        // FIXME: Handle case where keychooser is an ExponentialGenerator.
        // FIXME: Handle case where keynum is > transactioninsertkeysequence's last value
        self.key_chooser
            .lock()
            .unwrap()
            .next_value(&mut self.rng.lock().unwrap())
    }
}


#[async_trait]
impl Workload for CoreWorkload {
    async fn do_insert(&self, mut db: DBType) {
        let dbkey = self
            .key_sequence
            .lock()
            .unwrap()
            .next_value(&mut self.rng.lock().unwrap());
        let dbkey = format!("{}", fnvhash64(dbkey));
        let mut values = HashMap::new();
        for field_name in &self.field_names {
            let field_len = self
                .field_length_generator
                .lock()
                .unwrap()
                .next_value(&mut self.rng.lock().unwrap());
            let s = Alphanumeric
                .sample_string::<SmallRng>(&mut self.rng.lock().unwrap(), field_len as usize);
            values.insert(&field_name[..], s);
        }
        //db.insert(&self.table, &dbkey, &values).await.unwrap();

        let mut retry = self.retries;
        while retry > 0 {
            let now = std::time::Instant::now();
            let fut = db.insert(&self.table, &dbkey, &values);
            match timeout(Duration::from_millis(self.timeout), fut).await {
                Err(_) => {
                    //println!("insert timeout 100ms");
                    self.sender.send(crate::Request { latency: now.elapsed().as_millis(), success: false }).unwrap();
                    retry -= 1;
                },
                Ok(result) => {
                    result.unwrap();
                    // read succeeded
                    //
                    // If we previously failed, continue until retry == 0 to simulate workload amplification
                    self.sender.send(crate::Request { latency: now.elapsed().as_millis(), success: true }).unwrap();
                    if retry != self.retries {
                        retry -= 1;
                        continue;
                    }
                    break;
                },
            }
        }
    }

    async fn do_update(&self, mut db: DBType) {
        let dbkey = self
            .key_sequence
            .lock()
            .unwrap()
            .next_value(&mut self.rng.lock().unwrap());
        let dbkey = format!("{}", fnvhash64(dbkey));
        let mut values = HashMap::new();
        for field_name in &self.field_names {
            let field_len = self
                .field_length_generator
                .lock()
                .unwrap()
                .next_value(&mut self.rng.lock().unwrap());
            let s = Alphanumeric
                .sample_string::<SmallRng>(&mut self.rng.lock().unwrap(), field_len as usize);
            values.insert(&field_name[..], s);
        }
        //db.update(&self.table, &dbkey, &values).await.unwrap();

        let mut retry = self.retries;
        while retry > 0 {
            let now = std::time::Instant::now();
            let fut = db.update(&self.table, &dbkey, &values);
            match timeout(Duration::from_millis(self.timeout), fut).await {
                Err(_) => {
                    //println!("update timeout 100ms");
                    self.sender.send(crate::Request { latency: now.elapsed().as_millis(), success: false }).unwrap();
                    retry -= 1;
                },
                Ok(result) => {
                    result.unwrap();
                    // read succeeded
                    self.sender.send(crate::Request { latency: now.elapsed().as_millis(), success: true }).unwrap();
                    if retry != self.retries {
                        retry -= 1;
                        continue;
                    }
                    break;
                },
            }
        }
    }

    async fn do_transaction(&self, db: DBType) {
        let op = self
            .operation_chooser
            .lock()
            .unwrap()
            .next_value(&mut self.rng.lock().unwrap());
        //dbg!(&op);
        match op {
            CoreOperation::Read => {
                self.do_transaction_read(db).await;
            }
            CoreOperation::Update => {
                self.do_update(db).await;
            }
            CoreOperation::Insert => {
                self.do_insert(db).await;
            }
            _ => todo!(),
        }
    }
}

// http://en.wikipedia.org/wiki/Fowler_Noll_Vo_hash
fn fnvhash64(val: u64) -> u64 {
    let mut val = val;
    let prime = 0xcbf29ce484222325;
    let mut hashval = prime;
    for _ in 0..8 {
        let octet = val & 0x00ff;
        val >>= 8;
        hashval ^= octet;
        hashval = hashval.wrapping_mul(prime);
    }
    hashval
}

fn get_field_length_generator(prop: &Properties) -> Box<dyn Generator<u64> + Send> {
    match prop.field_length_distribution.to_lowercase().as_str() {
        "constant" => Box::new(ConstantGenerator::new(prop.field_length)),
        "uniform" => Box::new(UniformLongGenerator::new(1, prop.field_length)),
        "zipfian" => Box::new(ZipfianGenerator::from_range(1, prop.field_length)),
        "histogram" => unimplemented!(),
        _ => panic!(
            "unknown field length distribution {}",
            prop.field_length_distribution
        ),
    }
}

fn get_key_chooser_generator(prop: &Properties) -> Box<dyn Generator<u64> + Send> {
    let insert_count = if prop.insert_count > 1 {
        prop.insert_count
    } else {
        prop.record_count - prop.insert_start
    };
    assert!(insert_count > 1);
    match prop.request_distribution.to_lowercase().as_str() {
        "uniform" => Box::new(UniformLongGenerator::new(
            prop.insert_start,
            prop.insert_start + insert_count - 1,
        )),
        "zipfian" => Box::new(ZipfianGenerator::from_range(prop.insert_start,
                              prop.insert_start + insert_count - 1)),
        _ => todo!(),
    }
}

fn create_operation_generator(prop: &Properties) -> DiscreteGenerator<CoreOperation> {
    let mut pairs = vec![];
    if prop.read_proportion > 0.0 {
        pairs.push(WeightPair::new(prop.read_proportion, CoreOperation::Read));
    }
    if prop.update_proportion > 0.0 {
        pairs.push(WeightPair::new(
            prop.update_proportion,
            CoreOperation::Update,
        ));
    }
    if prop.insert_proportion > 0.0 {
        pairs.push(WeightPair::new(
            prop.insert_proportion,
            CoreOperation::Insert,
        ));
    }
    if prop.scan_proportion > 0.0 {
        pairs.push(WeightPair::new(prop.scan_proportion, CoreOperation::Scan));
    }
    if prop.read_modify_write_proportion > 0.0 {
        pairs.push(WeightPair::new(
            prop.read_modify_write_proportion,
            CoreOperation::ReadModifyWrite,
        ));
    }

    DiscreteGenerator::new(pairs)
}
