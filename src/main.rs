#![allow(unused_variables, unused_mut, unused_imports, unused_must_use, dead_code)]
use kompact::prelude::{promise, Ask, FutureCollection};

use omnipaxos_core::{
    ballot_leader_election::{BallotLeaderElection, BLEConfig, messages::BLEMessage},
    sequence_paxos::{CompactionErr, ReconfigurationRequest, SequencePaxos, SequencePaxosConfig},
    storage::{//memory_storage::MemoryStorage, 
        Snapshot},
    util::LogEntry::{self, Decided},
    messages::Message,
};
use tokio::{
    net::{TcpListener, TcpStream},
    io::{self, AsyncReadExt, AsyncWriteExt},
    sync::mpsc,
};
extern crate sled;
pub mod config;
pub mod util;
pub mod persistent_storage;

use persistent_storage::persistent_storage::PersistentStorage;

use crate::util::Value;
use rand::Rng;
use sled::Transactional;
use sled::{Config, Result};
use sled::IVec;
use std::{thread, time};
use structopt::StructOpt;
use config::TestConfig;
use util::TestSystem;
use serde::{Serialize, Deserialize};
use std::collections::HashMap;

#[derive(Clone, Debug, Serialize, Deserialize)]//k-v store
pub struct KeyValue {
    pub key: String,
    pub value: u64,
}

#[derive(Clone, Debug)] // snapshot
pub struct KVSnapshot {
    snapshotted: HashMap<String, u64>,
}

#[derive(Debug, StructOpt, Serialize, Deserialize)]
struct Node {
    #[structopt(long)]
    pid: u64,
    #[structopt(long)]
    peers: Vec<u64>,
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

#[tokio::main]
async fn main() {

    let node = Node::from_args();    
    // let node = Node{
    //     pid: 1,
    //     peers: vec![2,3],
    // };
    let node_pid = node.pid;
    let peers = node.peers;
    println!("pid: {:?}, peers: {:?}", node_pid, peers);
   
    let storeage_path = "./storage_sled".to_owned() + &node_pid.to_string();
    // sled db for each node
    let persistestoreage = sled::Config::default()
                                        .path(storeage_path.to_owned())
                                        .cache_capacity(10_000_000_000)
                                        .flush_every_ms(Some(50000))
                                        .open().expect("cannot open the database");

    let mut sp_config = SequencePaxosConfig::default();
    sp_config.set_configuration_id(node_pid.try_into().unwrap());
    sp_config.set_pid(node_pid);
    sp_config.set_peers(peers.to_vec());

    let mut ble_config = BLEConfig::default();
    ble_config.set_pid(node_pid);
    ble_config.set_peers(peers.to_vec());
    ble_config.set_hb_delay(40);
    
    let storage = PersistentStorage::<KeyValue, ()>::new(persistestoreage);
    let ble = BallotLeaderElection::with(ble_config); 
    let mut sp = SequencePaxos::with(sp_config, storage);
    
    //Channels used for SequencePaxos
    let (sender, receiver) = mpsc::channel(32);
    let sp_sender_outgoing = sender.clone();
    let ble_sender_outgoing = sender.clone();
    let sender_read = sender.clone();
    let sender_bletimer = sender.clone();
    let sender_blenet = sender.clone();
    let sender_recovery = sender.clone();
    // let mut recovered_storage = db_config.open().expect("cannot open the database");
    //sp = SequencePaxos::with(sp_config, MemoryStorage::<recovered_storage,()>::default());

    tokio::spawn(async move {
        periodically_send_messages(sp_sender_outgoing, ble_sender_outgoing).await;
    });
    tokio::spawn(async move {
        messages_handler(sp, ble, receiver, sender).await;
    });
    tokio::spawn(async move {
        ble_network_communication(sender_blenet, &node_pid).await;
    });
    tokio::spawn(async move {
        ble_timer_check(sender_bletimer).await;
    });

    tokio::spawn(async move {
        //storage.persist().await;
        //persist(sp).await;
        thread::sleep(time::Duration::from_secs(15));
        persist(sender_recovery, &node_pid);
    });
    
    //Set up connection
    let mut addr = "127.0.0.1:".to_owned();
    let port: u64 = 50000 + node_pid;
    addr.push_str(&port.to_string().to_owned()); 
    
    let read_listener = TcpListener::bind(addr).await.unwrap();
    
    // Reads have to be handled periodically
    loop {
        let (socket, _) = read_listener.accept().await.unwrap();
        let sender_x = sender_read.clone();
        tokio::spawn(async move {
            read_handler(socket, node_pid, sender_x).await;
        });   
    }
}
// //persist the variable
async fn persist(sender: mpsc::Sender<(&str, Vec<u8>)>, pid: &u64) {
    sender.send(("fail-recovery", vec![])).await.unwrap();
}

async fn ble_network_communication(sender: mpsc::Sender<(&str, Vec<u8>)>, pid: &u64) {
    let mut address: String = "127.0.0.1:".to_owned();
    let port_for_node: u64 = 60000 + pid;
    address.push_str(&port_for_node.to_string().to_owned()); 
    
    let listener = TcpListener::bind(address).await.unwrap();
    
    loop {
        let (connection, _) = listener.accept().await.unwrap();    
        let (mut connection_reader, _) = io::split(connection); 
        let mut buffer = vec![1; 128];
        loop {
            let n = connection_reader.read(&mut buffer).await.unwrap();
            //incorrect buffer size
            if n == 0 || n == 127 {break;}
            //Send ble handle message
            sender.send(("handle_ble", (&buffer[..n]).to_vec())).await.unwrap();
        }
    }
}

//The ble_periodic_timer function handles ble time checks; sets off the leader handling process for ble in a set interval
async fn ble_timer_check(sender: mpsc::Sender<(&str, Vec<u8>)>) {
    loop {
        //20 millisecond timer since this time is used in other portions of the code
        thread::sleep(time::Duration::from_millis(20));
        sender.send(("leader_ble", vec![])).await.unwrap();
    }
}
 
// send the outgoing message to all periodically
async fn periodically_send_messages(sp_sender: mpsc::Sender<(&str, Vec<u8>)>, ble_sender: mpsc::Sender<(&str, Vec<u8>)>) {
    loop {
        thread::sleep(time::Duration::from_millis(1));
        ble_sender.send(("outgoing", vec![])).await.unwrap();
        sp_sender.send(("outgoing", vec![])).await.unwrap();
    }
}

//The read_handler function sends incoming messages to SequencePaxos handle
async fn read_handler(socket_to_read: TcpStream, _id: u64, sender: mpsc::Sender<(&str, Vec<u8>)>) {
    //Establish connection
    let (mut connection, _) = io::split(socket_to_read);
    let mut buffer = vec![1; 128];
    //Loop through messages
    loop {
        let n = connection.read(&mut buffer).await.unwrap();
        if n == 0 || n == 127 {break;}
        sender.send(("handle_sp", (&buffer[..n]).to_vec())).await.unwrap();
    }
}

//The handle_ble_messages function handles messages related to the BallotLeaderElection functionality
async fn messages_handler(mut sp: SequencePaxos<KeyValue, (), PersistentStorage<KeyValue, ()>>, 
                        mut ble: BallotLeaderElection, 
                        mut receiver: mpsc::Receiver<(&str, Vec<u8>)>, 
                        sender: mpsc::Sender<(&str, Vec<u8>)>) {
    //handle received messages
    while let Some(action) = receiver.recv().await {
        match (action.0, action.1) {
            //a ble tick and a SequencePaxos handle leader request 
            ("leader_ble", ..) => {
                if let Some(leader) = ble.tick() {
                    //Re-serialize the message
                    let serialized_message: Vec<u8> = bincode::serialize(&leader).unwrap();
                    //Send on to SequencePaxos
                    sender.send(("sp_leader", serialized_message)).await.unwrap();
                }
            },
            //handle Heartbeat Message
            ("handle_ble", serialized_message) => {
                let deserialized_message: BLEMessage = bincode::deserialize(&serialized_message).unwrap();
                ble.handle(deserialized_message);
            },
            //Send the outgoing messages
            ("outgoing", ..) => {
                for outgoing_message in ble.get_outgoing_msgs() {
                    let receiver = outgoing_message.to;
                    //Connect to the correct address
                    match TcpStream::connect(format!("127.0.0.1:{}", 60000 + receiver)).await {
                        Err(_) => println!("ERROR: Bad connection - retrying next round"),
                        Ok(stream) => {
                            //Get writer for the connection
                            let (_reader, mut writer) = io::split(stream);
                            //Serialize and send the messages
                            let serialized_message: Vec<u8> = bincode::serialize(&outgoing_message).unwrap();
                            writer.write_all(&serialized_message).await.unwrap();
                        },
                    }
                }

                for outgoing_message in sp.get_outgoing_msgs() {
                    //Connect to the correct address
                    let receiver = outgoing_message.to;
                    match TcpStream::connect(format!("127.0.0.1:{}", 50000 + receiver)).await {
                        Err(_) => println!("ERROR: Bad connection - retrying next round"),
                        Ok(stream) => {
                            //Get writer for the connection
                            let (_reader, mut writer) = io::split(stream);
                            //Serialize and send the messages
                            let serialized_message: Vec<u8> = bincode::serialize(&outgoing_message).unwrap();
                            writer.write_all(&serialized_message).await.unwrap();
                        },
                    }
                }
            },
            
            ("sp_leader", serialized_message) => {
                sp.handle_leader(bincode::deserialize(&serialized_message).unwrap());
            },
            //SP match message type
            ("handle_sp", serialized_message) => {
                let deserialized_message: Message<KeyValue, ()> = bincode::deserialize(&serialized_message).unwrap();
                sp.handle(deserialized_message);
            },
            ("reconnected", ..) => {
                sp.reconnected(3);
            },
            ("fail-recovery", ..) => {
                sp.reconnected(3);
                sp.fail_recovery();
            },
            _ => {
                println!("message type is out of scope.");
            }
        }
    }
}
// // fn main() {

// //     // let cfg = TestConfig::load("test").expect("Test config loaded"); 
// //     // let sys = TestSystem::with(cfg.num_nodes, cfg.ble_hb_delay, cfg.num_threads);
// //     // let (ble, sq) = sys.ble_paxos_nodes().get(&1).unwrap();

// //     // let (kprom_ble, kfuture_ble) = promise::<Ballot>();
// //     // ble.on_definition(|x| x.add_ask(Ask::new(kprom_ble, ())));

// //     // //let m = Message::with(self.pid, *pid, PaxosMsg::PrepareReq);
    
// //     // let mut vec_proposals = vec![];
// //     // let mut futures = vec![];
// //     // let one_sec = time::Duration::from_millis(1000);
// //     // for i in 0..cfg.num_proposals {
        
// //     //     let (kprom, kfuture) = promise::<Value>();
// //     //     vec_proposals.push(Value(i));
// //     //     sq.on_definition(|x| {
// //     //         x.propose(Value(i));
// //     //         x.add_ask(Ask::new(kprom, ()))
// //     //     });
// //     //     futures.push(kfuture);
// //     //     thread::sleep(one_sec);
// //     // }

// //     // sys.start_all_nodes();

// //     // let elected_leader = kfuture_ble
// //     //     .wait_timeout(cfg.wait_timeout)
// //     //     .expect("No leader has been elected in the allocated time!");
// //     // println!("elected: {:?}", elected_leader);

// //     // match FutureCollection::collect_with_timeout::<Vec<_>>(futures, cfg.wait_timeout) {
// //     //     Ok(_) => {}
// //     //     Err(e) => panic!("Error on collecting futures of decided proposals: {}", e),
// //     // }
    
// //     //let seq_paxos = sq::paxos;
    
// //     // let configuration_id = 1;
// //     // let _cluster = vec![1, 2, 3];

// //     // // create the replica 2 in this cluster (other replica instances are created similarly with pid 1 and 3 on other servers)
// //     // let my_pid = 2;
// //     // let my_peers = vec![1, 3];

// //     // let mut sp_config = SequencePaxosConfig::default();
// //     // sp_config.set_configuration_id(configuration_id);
// //     // sp_config.set_pid(my_pid);
// //     // sp_config.set_peers(my_peers.clone());

// //     // let storage = MemoryStorage::<KeyValue, KVSnapshot>::default();
// //     // let mut seq_paxos = SequencePaxos::with(sp_config, storage);
   
// //     // let mut ble_config = BLEConfig::default();
// //     // ble_config.set_pid(my_pid);
// //     // ble_config.set_peers(my_peers.clone());
// //     // ble_config.set_hb_delay(40); // a leader timeout of 100 ticks
    
// //     // let write_entry = KeyValue {
// //     //     key: String::from("KV - K"),
// //     //     value: 123,
// //     // };

// //     // let mut ble = BallotLeaderElection::with(ble_config);
    
// //     // seq_paxos.append(write_entry).expect("Failed to append");

// //     // println!(" the value in the in memory store is {:?}", seq_paxos.read_entries(..));

// //     // // every 10ms call the following
// //     // if let Some(leader) = ble.tick() {
// //     //     // a new leader is elected, pass it to SequencePaxos.
// //     //     seq_paxos.handle_leader(leader);
// //     //     println!("the current leader is: {:?}", seq_paxos.get_current_leader());
// //     // }

// //     // some persistent storage
// //     // recovered_storage.insert("k1", IVec::from("my value"));
// //     match recovered_storage.get("k1") { // successful persistent 
// //         Ok(Some(value)) => println!("retrieved value {:?}", String::from_utf8(value.to_vec()).unwrap()),
// //         Ok(None) => println!("value not found"),
// //         Err(e) => println!("operational problem encountered: {}", e),
// //     }
    
// //     // let (_, px) = sys.ble_paxos_nodes().get(&proposal_node).unwrap();

// //     // match sys.kompact_system.shutdown() {
// //     //     Ok(_) => {}
// //     //     Err(e) => panic!("Error on kompact shutdown: {}", e),
// //     // };

// //     // let mut i:u32 = 1;
// //     // let ten_millis = time::Duration::from_millis(1000);
    
// //     // loop {
// //     //     i += 1; 
// //     //     let write_entry = KeyValue {
// //     //         key: String::from("a"),
// //     //         value: 123,
// //     //     };
// //     //     seq_paxos.append(write_entry).expect("Failed to append");
        
// //     //     if i == 20 {
// //     //         println!("OK, that's enough");
// //     //         break;
// //     //     } 
// //     //     thread::sleep(ten_millis);
// //     // }
    

// // //------------------------------
// //     //sled_begin();
    


// // }

// // fn sled_begin()  {

// //     let _config = sled::Config::default()
// //         .path("/path/to/data".to_owned())
// //         .cache_capacity(10_000_000_000)
// //         .flush_every_ms(Some(1000));

// //     // this directory will be created if it does not exist
// //     let path = "./storage_sled";
// //     let db: sled::Db = sled::open(path).unwrap();

// //     // insert and get
// //     db.insert("yo!", vec![0]);
// //     println!("{:?}", db.get("yo!"));


// //     // Atomic compare-and-swap.
// //     db.compare_and_swap(
// //         "yo!",      // key
// //         Some([0]), // old value, None for not present
// //         Some("v2"), // new value, None for delete
// //     )
// //     .unwrap();

// //     // Iterates over key-value pairs, starting at the given key.
// //     let scan_key: &[u8] = b"a non-present key before yo!";
// //     let mut iter = db.range(scan_key..);


// //     // db.remove("yo!");
// //     // assert_eq!(db.get("yo!"), Ok(None));

// //     let other_tree: sled::Tree = db.open_tree(b"cool db facts").unwrap();
// //     other_tree.insert(
// //         "k1",
// //         IVec::from("my value"),
// //     ).unwrap();

// //     match other_tree.get("k1") {
// //         Ok(Some(value)) => println!("retrieved value {:?}", String::from_utf8(value.to_vec()).unwrap()),
// //         Ok(None) => println!("value not found"),
// //         Err(e) => println!("operational problem encountered: {}", e),
// //     }

// //     println!("size on disk is {:?}", db.size_on_disk());
// //     // let db = sled::open(path)?;

// //     // // key and value types can be `Vec<u8>`, `[u8]`, or `str`.
// //     // let key = "my key";

// //     // // `generate_id`
// //     // let value = IVec::from("value");

// //     // dbg!(
// //     //     db.insert(key, &value)?, // as in BTreeMap::insert
// //     //     db.get(key)?,            // as in BTreeMap::get
// //     //     //db.remove(key)?,         // as in BTreeMap::remove
// //     // );

// //     // Ok(())

// //     // works like std::fs::open
// //     // let tree = sled::open(path).expect("open");

// //     // // insert and get, similar to std's BTreeMap
// //     // tree.insert("KEY1", "VAL1");
// //     // assert_eq!(tree.get(&"KEY1"), Ok(Some(IVec::from("VAL1"))));

// //     // // range queries
// //     // for kv in tree.range("KEY1".."KEY9") {
// //     //     //println!("{:?}", &kv.get(&"KEY1"), )
// //     // }

// //     // // deletion
// //     // tree.remove(&"KEY1");

// //     // // atomic compare and swap
// //     // tree.compare_and_swap("KEY1", Some("VAL1"), Some("VAL2"));

// //     // // block until all operations are stable on disk
// //     // // (flush_async also available to get a Future)
// //     // tree.flush();
// // }