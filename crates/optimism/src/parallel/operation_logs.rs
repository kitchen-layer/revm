use crate::context::Context;
use crate::types::Address;
use ethers::{
    providers::JsonRpcClient,
    types::{BlockNumber, Filter, Log, SyncingStatus, U256},
};
use std::sync::{Arc, RwLock}; // Import the Context struct

pub struct OperationLog<'a, BLOCK, TX, CFG, DB, JOURNAL> {
    pub reads: Arc<RwLock<Vec<(Address, U256)>>>, // Storage slot reads
    pub writes: Arc<RwLock<Vec<(Address, U256, U256)>>>, // Storage slot writes with values
    pub version: u64,                             // State version reference
    pub tx_index: usize,                          // Transaction index for dependency tracking
    pub context: &'a Context<BLOCK, TX, CFG, DB, JOURNAL>, // Reference to the context
}

impl<'a, BLOCK, TX, CFG, DB, JOURNAL> OperationLog<'a, BLOCK, TX, CFG, DB, JOURNAL> {
    pub fn new(context: &'a Context<BLOCK, TX, CFG, DB, JOURNAL>) -> Self {
        Self {
            reads: Arc::new(RwLock::new(Vec::new())),
            writes: Arc::new(RwLock::new(Vec::new())),
            version: 0,
            tx_index: 0,
            context,
        }
    }

    pub fn record_read(&self, address: Address, slot: U256) {
        self.reads.write().unwrap().push((address, slot));
    }

    pub fn record_write(&self, address: Address, slot: U256, value: U256) {
        self.writes.write().unwrap().push((address, slot, value));
    }

    pub fn get_block_info(&self) -> &BLOCK {
        &self.context.block
    }

    pub fn get_tx_info(&self) -> &TX {
        &self.context.tx
    }

    pub fn get_db_info(&self) -> &DB {
        self.context.db_ref() // Access the database reference
    }

    pub fn check_conflicts(&self) -> Vec<(Address, U256)> {
        let reads = self.reads.read().unwrap();
        let writes = self.writes.read().unwrap();
        let mut conflicts = Vec::new();

        for (address, slot) in reads.iter() {
            for (write_address, write_slot, _) in writes.iter() {
                if address == write_address && slot == write_slot {
                    conflicts.push((*address, *slot));
                }
            }
        }
        conflicts
    }

    fn extract_read_write_sets<BLOCK, TX, CFG, DB, JOURNAL>(
        op_log: &OperationLog<BLOCK, TX, CFG, DB, JOURNAL>,
    ) -> (HashSet<(Address, U256)>, HashSet<(Address, U256)>) {
        let reads = op_log.reads.read().unwrap();
        let read_set = reads.iter().map(|(addr, slot)| (*addr, *slot)).collect();

        let writes = op_log.writes.read().unwrap();
        let write_set = writes
            .iter()
            .map(|(addr, slot, _)| (*addr, *slot))
            .collect();

        (read_set, write_set)
    }
}
