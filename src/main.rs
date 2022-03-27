#![allow(unused_variables, unused_mut, unused_imports, unused_must_use, dead_code)]

extern crate sled;
pub mod config;
pub mod util;

use kompact::prelude::{promise, Ask, FutureCollection};
use omnipaxos_core::{
    ballot_leader_election::{BLEConfig, BallotLeaderElection, Ballot},
    sequence_paxos::{CompactionErr, ReconfigurationRequest, SequencePaxos, SequencePaxosConfig},
    storage::{memory_storage::MemoryStorage, Snapshot},
    util::LogEntry,
    messages::Message,
};
use crate::util::Value;
use rand::Rng;

use sled::Transactional;
use sled::{Config, Result};
use sled::IVec;
use std::{thread, time};
use config::TestConfig;
use util::TestSystem;

//use std::ops::FromResidual::<std::result::Result<std::convert::Infallible, sled::Error>>
use std::collections::HashMap;

#[derive(Clone, Debug)] //k-v store
pub struct KeyValue {
    pub key: String,
    pub value: u64,
}

#[derive(Clone, Debug)] // snapshot
pub struct KVSnapshot {
    snapshotted: HashMap<String, u64>,
}

impl Snapshot<KeyValue> for KVSnapshot {
    fn create(entries: &[KeyValue]) -> Self {
        let mut snapshotted = HashMap::new();
        for e in entries {
            let KeyValue { key, value } = e;
            snapshotted.insert(key.clone(), *value);
        }
        Self { snapshotted }
    }

    fn merge(&mut self, delta: Self) {
        for (k, v) in delta.snapshotted {
            self.snapshotted.insert(k, v);
        }
    }

    fn use_snapshots() -> bool {
        true
    }
}

fn main() {

    let cfg = TestConfig::load("test").expect("Test config loaded"); 
    let sys = TestSystem::with(cfg.num_nodes, cfg.ble_hb_delay, cfg.num_threads);
    let (ble, sq) = sys.ble_paxos_nodes().get(&1).unwrap();

    let (kprom_ble, kfuture_ble) = promise::<Ballot>();
    ble.on_definition(|x| x.add_ask(Ask::new(kprom_ble, ())));

    //let m = Message::with(self.pid, *pid, PaxosMsg::PrepareReq);
    
    let mut vec_proposals = vec![];
    let mut futures = vec![];
    let one_sec = time::Duration::from_millis(1000);
    for i in 0..cfg.num_proposals {
        
        let (kprom, kfuture) = promise::<Value>();
        vec_proposals.push(Value(i));
        sq.on_definition(|x| {
            x.propose(Value(i));
            x.add_ask(Ask::new(kprom, ()))
        });
        futures.push(kfuture);
        thread::sleep(one_sec);
    }

    sys.start_all_nodes();

    let elected_leader = kfuture_ble
        .wait_timeout(cfg.wait_timeout)
        .expect("No leader has been elected in the allocated time!");
    println!("elected: {:?}", elected_leader);

    match FutureCollection::collect_with_timeout::<Vec<_>>(futures, cfg.wait_timeout) {
        Ok(_) => {}
        Err(e) => panic!("Error on collecting futures of decided proposals: {}", e),
    }

    //let seq_paxos = sq::paxos;
    
    // let configuration_id = 1;
    // let _cluster = vec![1, 2, 3];

    // // create the replica 2 in this cluster (other replica instances are created similarly with pid 1 and 3 on other servers)
    // let my_pid = 2;
    // let my_peers = vec![1, 3];

    // let mut sp_config = SequencePaxosConfig::default();
    // sp_config.set_configuration_id(configuration_id);
    // sp_config.set_pid(my_pid);
    // sp_config.set_peers(my_peers.clone());

    // let storage = MemoryStorage::<KeyValue, KVSnapshot>::default();
     let mut seq_paxos = SequencePaxos::with(sp_config, storage);
   
    // let mut ble_config = BLEConfig::default();
    // ble_config.set_pid(my_pid);
    // ble_config.set_peers(my_peers.clone());
    // ble_config.set_hb_delay(40); // a leader timeout of 100 ticks
    
    // let write_entry = KeyValue {
    //     key: String::from("KV - K"),
    //     value: 123,
    // };

    // let mut ble = BallotLeaderElection::with(ble_config);
    
    // seq_paxos.append(write_entry).expect("Failed to append");

    // println!(" the value in the in memory store is {:?}", seq_paxos.read_entries(..));


    // // every 10ms call the following
    // if let Some(leader) = ble.tick() {
    //     // a new leader is elected, pass it to SequencePaxos.
    //     seq_paxos.handle_leader(leader);
    //     println!("the current leader is: {:?}", seq_paxos.get_current_leader());
    // }

    

    let db_config = sled::Config::default()
        .path("./storage_sled".to_owned())
        .cache_capacity(10_000_000_000);

    let recovered_storage = db_config.open().expect("cannot open the database");

    // some persistent storage
    // recovered_storage.insert("k1", IVec::from("my value"));
    match recovered_storage.get("k1") { // successful persistent 
        Ok(Some(value)) => println!("retrieved value {:?}", String::from_utf8(value.to_vec()).unwrap()),
        Ok(None) => println!("value not found"),
        Err(e) => println!("operational problem encountered: {}", e),
    }
    
    // let (_, px) = sys.ble_paxos_nodes().get(&proposal_node).unwrap();

    // match sys.kompact_system.shutdown() {
    //     Ok(_) => {}
    //     Err(e) => panic!("Error on kompact shutdown: {}", e),
    // };

       


    // let mut i:u32 = 1;
    // let ten_millis = time::Duration::from_millis(1000);
    
    // loop {
    //     i += 1; 
    //     let write_entry = KeyValue {
    //         key: String::from("a"),
    //         value: 123,
    //     };
    //     seq_paxos.append(write_entry).expect("Failed to append");
        
    //     if i == 20 {
    //         println!("OK, that's enough");
    //         break;
    //     } 
    //     thread::sleep(ten_millis);
    // }
    

//------------------------------
    //sled_begin();
    


}

fn sled_begin()  {

    let _config = sled::Config::default()
        .path("/path/to/data".to_owned())
        .cache_capacity(10_000_000_000)
        .flush_every_ms(Some(1000));

    // this directory will be created if it does not exist
    let path = "./storage_sled";
    let db: sled::Db = sled::open(path).unwrap();

    // insert and get
    db.insert("yo!", vec![0]);
    println!("{:?}", db.get("yo!"));


    // Atomic compare-and-swap.
    db.compare_and_swap(
        "yo!",      // key
        Some([0]), // old value, None for not present
        Some("v2"), // new value, None for delete
    )
    .unwrap();

    // Iterates over key-value pairs, starting at the given key.
    let scan_key: &[u8] = b"a non-present key before yo!";
    let mut iter = db.range(scan_key..);


    // db.remove("yo!");
    // assert_eq!(db.get("yo!"), Ok(None));

    let other_tree: sled::Tree = db.open_tree(b"cool db facts").unwrap();
    other_tree.insert(
        "k1",
        IVec::from("my value"),
    ).unwrap();

    match other_tree.get("k1") {
        Ok(Some(value)) => println!("retrieved value {:?}", String::from_utf8(value.to_vec()).unwrap()),
        Ok(None) => println!("value not found"),
        Err(e) => println!("operational problem encountered: {}", e),
    }
    // match other_tree.get("k1") {
    //     Ok(status) => match status {
    //         Some(value) => println!("value is: {:?}", value),
    //         _ => println!(" "),
    //     }
    //     Err(e) => println!("{}", e),
    // }
    println!("size on disk is {:?}", db.size_on_disk());
    // let db = sled::open(path)?;

    // // key and value types can be `Vec<u8>`, `[u8]`, or `str`.
    // let key = "my key";

    // // `generate_id`
    // let value = IVec::from("value");

    // dbg!(
    //     db.insert(key, &value)?, // as in BTreeMap::insert
    //     db.get(key)?,            // as in BTreeMap::get
    //     //db.remove(key)?,         // as in BTreeMap::remove
    // );

    // Ok(())

    // works like std::fs::open
    // let tree = sled::open(path).expect("open");

    // // insert and get, similar to std's BTreeMap
    // tree.insert("KEY1", "VAL1");
    // assert_eq!(tree.get(&"KEY1"), Ok(Some(IVec::from("VAL1"))));

    // // range queries
    // for kv in tree.range("KEY1".."KEY9") {
    //     //println!("{:?}", &kv.get(&"KEY1"), )
    // }

    // // deletion
    // tree.remove(&"KEY1");

    // // atomic compare and swap
    // tree.compare_and_swap("KEY1", Some("VAL1"), Some("VAL2"));

    // // block until all operations are stable on disk
    // // (flush_async also available to get a Future)
    // tree.flush();
}