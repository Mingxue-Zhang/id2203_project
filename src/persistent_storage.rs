#[allow(missing_docs)]
pub mod persistent_storage {
    use omnipaxos_core::{
        ballot_leader_election::Ballot,
        storage::{Entry, Snapshot, StopSignEntry, Storage},
    };
    use std::{thread, time};
    use sled::IVec;
    // pub trait PStorage: Storage<T, S>
    // {
    //     fn set_promise(&mut self, n_prom: Ballot);
    //     fn get_snapshot(&self) -> Option<S>;
    // }

    /// An in-memory storage implementation for SequencePaxos.
    #[derive(Clone)]
    pub struct MemoryStorage<T, S>
    where
        T: Entry,
        S: Snapshot<T>,
    {
        /// Vector which contains all the logged entries in-memory.
        log: Vec<T>,
        /// Last promised round.
        n_prom: Ballot,
        /// Last accepted round.
        acc_round: Ballot,
        /// Length of the decided log.
        ld: u64,
        /// Garbage collected index.
        trimmed_idx: u64,
        /// Stored snapshot
        snapshot: Option<S>,
        /// Stored StopSign
        stopsign: Option<StopSignEntry>,
    }

    impl<T, S> Storage<T, S> for MemoryStorage<T, S>
    where
        T: Entry,
        S: Snapshot<T>,
    {
        fn append_entry(&mut self, entry: T) -> u64 {
            self.log.push(entry);
            self.get_log_len()
        }

        fn append_entries(&mut self, entries: Vec<T>) -> u64 {
            let mut e = entries;
            self.log.append(&mut e);
            self.get_log_len()
        }

        fn append_on_prefix(&mut self, from_idx: u64, entries: Vec<T>) -> u64 {
            self.log.truncate(from_idx as usize);
            self.append_entries(entries)
        }

        fn set_promise(&mut self, n_prom: Ballot) {
            self.n_prom = n_prom;
        }

        fn set_decided_idx(&mut self, ld: u64) {
            self.ld = ld;
        }

        fn get_decided_idx(&self) -> u64 {
            self.ld
        }

        fn set_accepted_round(&mut self, na: Ballot) {
            self.acc_round = na;
        }

        fn get_accepted_round(&self) -> Ballot {
            self.acc_round
        }

        fn get_entries(&self, from: u64, to: u64) -> &[T] {
            self.log.get(from as usize..to as usize).unwrap_or(&[])
        }

        fn get_log_len(&self) -> u64 {
            self.log.len() as u64
        }

        fn get_suffix(&self, from: u64) -> &[T] {
            match self.log.get(from as usize..) {
                Some(s) => s,
                None => &[],
            }
        }

        fn get_promise(&self) -> Ballot {
            self.n_prom
        }

        fn set_stopsign(&mut self, s: StopSignEntry) {
            self.stopsign = Some(s);
        }

        fn get_stopsign(&self) -> Option<StopSignEntry> {
            self.stopsign.clone()
        }

        fn trim(&mut self, trimmed_idx: u64) {
            self.log.drain(0..trimmed_idx as usize);
        }

        fn set_compacted_idx(&mut self, trimmed_idx: u64) {
            self.trimmed_idx = trimmed_idx;
        }

        fn get_compacted_idx(&self) -> u64 {
            self.trimmed_idx
        }

        fn set_snapshot(&mut self, snapshot: S) {
            self.snapshot = Some(snapshot);
        }

        fn get_snapshot(&self) -> Option<S> {
            self.snapshot.clone()
        }
    }

    impl<T: Entry, S: Snapshot<T>> Default for MemoryStorage<T, S> {
        fn default() -> Self {
            Self {
                log: vec![],
                n_prom: Ballot::default(),
                acc_round: Ballot::default(),
                ld: 0,
                trimmed_idx: 0,
                snapshot: None,
                stopsign: None,
            }
        }
    }
    #[derive(Clone, Debug)]
    pub struct PersistentStorage<T, S>  
    where
        T: Entry,
        S: Snapshot<T>,
    {
        log: Vec<T>, // the log.
        n_prom: Ballot, // the promised round.
        ld: u64, // the decided index
        acc_round: Ballot, // the latest round entries were accepted in.
        snapshot: Option<S>,
        stopsign: Option<StopSignEntry>,
        trimmed_idx: u64,
    }

    impl<T, S> Storage<T, S> for PersistentStorage<T, S> 
    where
        T: Entry,
        S: Snapshot<T>,
    {   
        fn append_entry(&mut self, entry: T) -> u64 {
            self.log.push(entry);
            self.get_log_len()
        }

        fn append_entries(&mut self, entries: Vec<T>) -> u64 {
            let mut e = entries;
            self.log.append(&mut e);
            self.get_log_len()
        }

        fn append_on_prefix(&mut self, from_idx: u64, entries: Vec<T>) -> u64 {
            self.log.truncate(from_idx as usize);
            self.append_entries(entries)
        }

        fn set_promise(&mut self, n_prom: Ballot) {
            self.n_prom = n_prom;
        }

        fn set_decided_idx(&mut self, ld: u64) {
            self.ld = ld;
        }

        fn get_decided_idx(&self) -> u64 {
            self.ld
        }

        fn set_accepted_round(&mut self, na: Ballot) {
            self.acc_round = na;
        }

        fn get_accepted_round(&self) -> Ballot {
            self.acc_round
        }

        fn get_entries(&self, from: u64, to: u64) -> &[T] {
            self.log.get(from as usize..to as usize).unwrap_or(&[])
        }

        fn get_log_len(&self) -> u64 {
            self.log.len() as u64
        }

        fn get_suffix(&self, from: u64) -> &[T] {
            match self.log.get(from as usize..) {
                Some(s) => s,
                None => &[],
            }
        }

        fn get_promise(&self) -> Ballot {
            self.n_prom
        }

        fn set_stopsign(&mut self, s: StopSignEntry) {
            self.stopsign = Some(s);
        }

        fn get_stopsign(&self) -> Option<StopSignEntry> {
            self.stopsign.clone()
        }

        fn trim(&mut self, trimmed_idx: u64) {
            self.log.drain(0..trimmed_idx as usize);
        }

        fn set_compacted_idx(&mut self, trimmed_idx: u64) {
            self.trimmed_idx = trimmed_idx;
        }

        fn get_compacted_idx(&self) -> u64 {
            self.trimmed_idx
        }

        fn set_snapshot(&mut self, snapshot: S) {
            self.snapshot = Some(snapshot);
        }

        fn get_snapshot(&self) -> Option<S> {
            self.snapshot.clone()
        }
        
    }
    
    impl<T: Entry, S: Snapshot<T>> PersistentStorage<T, S> { 
        pub fn new(db: sled::Db) -> Self {
            Self {
                log: match db.get("log"){
                    Ok(Some(value)) => vec![],
                        _ => vec![],
                        //Err(e) => println!("operational problem encountered: {}", e),
                },
                n_prom: match db.get("n_prom"){
                    Ok(Some(value)) =>  Ballot::default(),
                        //Ballot {n: value.split(" ")[0], priority: value.split(" ")[1], pid:value.split(" ")[2] },
                    _ => Ballot::default(),
                    //Err(e) => println!("operational problem encountered: {}", e),
                },
                acc_round: match db.get("acc_round"){
                    Ok(Some(value)) => Ballot::default(),
                    _ => Ballot::default(),
                    //Ok(None) => Ballot::default(),
                    //Err(e) => println!("operational problem encountered: {}", e),
                },
                ld: match db.get("ld"){
                    Ok(Some(value)) => 0,
                    _ => 0,
                    //Err(e) => println!("operational problem encountered: {}", e),
                },
                trimmed_idx: 0,
                snapshot: None,
                stopsign: None,
            }
        }
        //
        pub fn persist(self) {
            // let mut logstr = "";
            // for item in self.log {
            //     println!("{:?}", item);
            // }
            // loop {
            //     let sleep_every_duration = time::Duration::from_secs(3);
            //     db.insert("log", (self.log).to_vec());
            //     db.insert("n_prom", self.n_prom);
            //     db.insert("acc_round", self.acc_round);
            //     db.insert("ld", IVec::from(" "));
            //     thread::sleep(sleep_every_duration);
            // }
        }

    }
    impl<T: Entry, S: Snapshot<T>> Default for PersistentStorage<T, S> {
        fn default() -> Self {
            Self {
                log: vec![],
                n_prom: Ballot::default(),
                acc_round: Ballot::default(),
                ld: 0,
                trimmed_idx: 0,
                snapshot: None,
                stopsign: None,
            }
        }

        
    }
}