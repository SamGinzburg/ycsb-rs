use crate::db::DB;
use crate::workload::Workload;
use anyhow::{bail, Result};
use futures::channel::mpsc::{UnboundedSender, UnboundedReceiver};
use properties::Properties;
use std::fs;
use std::rc::Rc;
use std::thread;
use std::time::{Instant, Duration};
use structopt::StructOpt;
use workload::CoreWorkload;
use std::sync::{Arc};
use tokio::sync::Mutex;
use std::borrow::BorrowMut;
use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::Semaphore;
use tokio::sync::mpsc::*;
use hdrhistogram::*;

pub mod db;
pub mod generator;
pub mod properties;
//pub mod sqlite;
pub mod postgres;
//pub mod rocksdb;
pub mod workload;

#[derive(StructOpt, Debug)]
#[structopt(name = "ycsb")]
pub struct Opt {
    #[structopt(name = "COMMANDS")]
    commands: Vec<String>,
    #[structopt(short, long)]
    database: String,
    #[structopt(short, long)]
    workload: String,
    #[structopt(short, long, default_value = "1")]
    threads: usize,
    #[structopt(short, long, default_value = "300")]
    timeout: u64,
    #[structopt(short, long, default_value = "100")]
    retries: u64,
}

#[derive(Debug)]
pub struct Request {
    latency: u128,
    success: bool
}

pub type SenderType = Arc<tokio::sync::mpsc::UnboundedSender<Request>>;

async fn load(wl: Arc<CoreWorkload>, db: db::DBType, operation_count: usize) {
    let mut joins = vec![];
    // Use the semaphore to make sure we don't issue too many requests
    // For each "thread", we can have X requests in flight, so 10 threads = 10x concurrent reqs
    let semaphore = Arc::new(Semaphore::new(300));
    for _ in 0..operation_count {
        let db = db.clone();
        let wl = wl.clone();
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let join = tokio::task::spawn(async move {
            wl.do_insert(db.clone()).await;
            drop(permit);
        });
        joins.push(join);
    }

    for join in joins {
        join.await.unwrap();
    }
}

async fn run(wl: Arc<CoreWorkload>, db: db::DBType, operation_count: usize) {

    let mut joins = vec![];
    // Use the semaphore to make sure we don't issue too many requests
    let semaphore = Arc::new(Semaphore::new(300));
    for _ in 0..operation_count {
        let db = db.clone();
        let wl = wl.clone();
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let join = tokio::task::spawn(async move {
            wl.do_transaction(db.clone()).await;
            drop(permit);
        });
        joins.push(join);
    }

    for join in joins {
        join.await.unwrap();
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt = Opt::from_args();

    let raw_props = fs::read_to_string(&opt.workload)?;

    let props: Properties = toml::from_str(&raw_props)?;

    let props = Arc::new(props);
    let (tx, mut rx): (tokio::sync::mpsc::UnboundedSender<Request>, tokio::sync::mpsc::UnboundedReceiver<Request>) = unbounded_channel();
    let tx = Arc::new(tx);

    let wl = Arc::new(CoreWorkload::new(&props, &opt, tx.clone()));

    if opt.commands.is_empty() {
        bail!("no command specified");
    }

    // Create a thread to track latency / failure stats
    std::thread::spawn(move || {
        let waker = futures::task::noop_waker();
        let mut cx = std::task::Context::from_waker(&waker);

        let mut histogram = Histogram::<u64>::new_with_bounds(1, 60 * 60 * 1000, 2).unwrap();

        let mut fail_count = 0;
        let mut total = 0;
        let mut now = std::time::Instant::now();
        while true {
            match rx.poll_recv(&mut cx) {
                core::task::Poll::Ready(Some(msg)) => {
                    //dbg!(&msg);
                    histogram.record(msg.latency as u64).unwrap();
                    if msg.success {
                        fail_count += 1;
                    }
                    total += 1;
                },
                _ => {

                }
            }

            // Emit percentiles every ~ X ms
            let ellapsed = now.elapsed().as_millis();
            if ellapsed < 100 {
                continue;
            }
            // Reset the timer and emit percentiles
            now = std::time::Instant::now();
            println!("Success rate: {}, total reqs: {}", fail_count as f64 / total as f64, total as f64);
            fail_count = 0;
            total = 0;

            println!("P25: {}", histogram.value_at_quantile(0.25));
            println!("P50: {}", histogram.value_at_quantile(0.50));
            println!("P75: {}", histogram.value_at_quantile(0.75));
            println!("P99: {}", histogram.value_at_quantile(0.99));
            histogram.clear();
        }
    });

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
        let mut db = db::create_db(&database).await.unwrap();
        db.init().await.unwrap();
        for _ in 0..opt.threads {
            //let database = database.clone();
            let tx = tx.clone();
            let wl = wl.clone();
            let cmd = opt.commands[0].clone();
            let db = db.clone();
            //let db = db::create_db(&database).await.unwrap();
            threads.push(tokio::spawn(async move {
                //let mut db = db::create_db(&database).await.unwrap();
                //db.init().await.unwrap();

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
