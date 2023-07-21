use crate::db::DB;
use crate::workload::Workload;
use anyhow::{bail, Result};
use properties::Properties;
use std::fs;
use std::rc::Rc;
use std::thread;
use std::time::Instant;
use structopt::StructOpt;
use workload::CoreWorkload;
use std::sync::{Arc};
use tokio::sync::Mutex;
use std::borrow::BorrowMut;
use tokio::sync::mpsc::{channel, Sender};

pub mod db;
pub mod generator;
pub mod properties;
//pub mod sqlite;
pub mod postgres;
//pub mod rocksdb;
pub mod workload;

#[derive(StructOpt, Debug)]
#[structopt(name = "ycsb")]
struct Opt {
    #[structopt(name = "COMMANDS")]
    commands: Vec<String>,
    #[structopt(short, long)]
    database: String,
    #[structopt(short, long)]
    workload: String,
    #[structopt(short, long, default_value = "1")]
    threads: usize,
}

async fn load(wl: Arc<CoreWorkload>, db: Arc<Mutex<dyn DB>>, operation_count: usize) {
    for _ in 0..operation_count {
        let db = db.clone();
        let wl = wl.clone();
        wl.do_insert(db.clone()).await;
    }
}

async fn run(wl: Arc<CoreWorkload>, db: Arc<Mutex<dyn DB>>, operation_count: usize) {
    for _ in 0..operation_count {
        let db = db.clone();
        let wl = wl.clone();
        let now = Instant::now();
        wl.do_transaction(db.clone()).await;
        //println!("{}", now.elapsed().as_millis());
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt = Opt::from_args();

    let raw_props = fs::read_to_string(&opt.workload)?;

    let props: Properties = toml::from_str(&raw_props)?;

    let props = Arc::new(props);

    let wl = Arc::new(CoreWorkload::new(&props));

    if opt.commands.is_empty() {
        bail!("no command specified");
    }

    let database = opt.database.clone();
    let thread_operation_count = props.operation_count as usize / opt.threads;
    //for cmd in opt.commands {
        let start = Instant::now();
        /*
        let db = db::create_db(&database).await.unwrap().clone();

        db.lock().await.init().await?;

        match &opt.commands[0][..] {
            "load" => load(wl.clone(), db.clone(), thread_operation_count as usize).await,
            "run" => run(wl.clone(), db.clone(), thread_operation_count as usize).await,
            cmd => panic!("invalid command: {}", cmd),
        };
        */

        let mut threads = vec![];
        //let db = db::create_db(&database).await.unwrap();
        for _ in 0..opt.threads {
            let database = database.clone();
            let wl = wl.clone();
            let cmd = opt.commands[0].clone();
            //let db = db.clone();
            //let db = db::create_db(&database).await.unwrap();
            threads.push(tokio::spawn(async move {
                let db = db::create_db(&database).await.unwrap();
                db.lock().await.init().await.unwrap();

                match &cmd[..] {
                    "load" => load(wl.clone(), db, thread_operation_count as usize).await,
                    "run" => run(wl.clone(), db, thread_operation_count as usize).await,
                    cmd => panic!("invalid command: {}", cmd),
                };
            }));
        }
        for t in threads {
            let _ = t.await;
        }
        let runtime = start.elapsed().as_millis();
        println!("[OVERALL], ThreadCount, {}", opt.threads);
        println!("[OVERALL], RunTime(ms), {}", runtime);
        let throughput = props.operation_count as f64 / (runtime as f64 / 1000.0);
        println!("[OVERALL], Throughput(ops/sec), {}", throughput);
    //}

    Ok(())
}
