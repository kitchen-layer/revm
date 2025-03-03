use crate::types::Address;
use ethers::{
    providers::JsonRpcClient,
    types::{BlockNumber, Filter, Log, SyncingStatus, U256},
};

pub struct OperationLog {
    pub reads: Vec<(Address, U256)>,        // Storage slot reads
    pub writes: Vec<(Address, U256, U256)>, // Storage slot writes with values
    pub version: u64,                       // State version reference
    pub tx_index: usize,                    // Transaction index for dependency tracking
}

impl OperationLog {
    pub fn new() -> Self {
        Self {
            reads: Vec::new(),
            writes: Vec::new(),
            version: 0,
        }
    }

    pub fn record_read(&mut self, address: Address, slot: U256) {
        self.reads.push((address, slot));
    }

    pub fn record_write(&mut self, address: Address, slot: U256, value: U256) {
        self.writes.push((address, slot, value));
    }
}
