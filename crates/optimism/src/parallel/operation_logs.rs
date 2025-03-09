use crate::VersionedStateDB;
use revm::context::Context;
use revm::database_interface;
use revm::primitives::{Address, U256};
use std::collections::HashSet;
use std::sync::{Arc, RwLock}; // Import the Context struct

#[derive(Debug, Clone)]
pub enum Operation {
    Read {
        address: Address,
        slot: U256,
    },
    Write {
        address: Address,
        slot: U256,
        value: U256,
        original_value: U256,
    },
    AccountAccess {
        address: Address,
    },
    CodeAccess {
        address: Address,
    },
}

#[derive(Debug)]
pub struct OperationLog {
    operations: Vec<Operation>,
    accessed_slots: HashSet<(Address, U256)>,
    accessed_accounts: HashSet<Address>,
    version: u64,
    tx_index: usize,
    read_count: usize,
    write_count: usize,
    account_access_count: usize,
    reads: Arc<RwLock<Vec<(Address, U256)>>>,
    writes: Arc<RwLock<Vec<(Address, U256, U256)>>>,
}

impl<'a, BLOCK, TX, CFG, DB, JOURNAL> OperationLog<'a, BLOCK, TX, CFG, DB, JOURNAL> {
    pub fn new(context: &'a Context<BLOCK, TX, CFG, DB, JOURNAL>) -> Self {
        Self {
            operations: Vec::new(),
            accessed_slots: HashSet::new(),
            accessed_accounts: HashSet::new(),
            version: 0,
            tx_index: 0,
            read_count: 0,
            write_count: 0,
            account_access_count: 0,
            reads: Arc::new(RwLock::new(Vec::new())),
            writes: Arc::new(RwLock::new(Vec::new())),
            context,
        }
    }

    pub fn record_read(&self, address: Address, slot: U256) {
        self.reads.read().unwrap().push((address, slot));
    }

    pub fn record_write(&self, address: Address, slot: U256, value: U256) {
        self.writes.read().unwrap().push((address, slot, value));
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

    fn extract_read_write_sets(
        op_log: &OperationLog,
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

impl OperationLog {
    pub fn new(tx_index: usize, version: u64) -> Self {
        Self {
            operations: Vec::with_capacity(64), // Pre-allocate for common case
            accessed_slots: HashSet::new(),
            accessed_accounts: HashSet::new(),
            version,
            tx_index,
            read_count: 0,
            write_count: 0,
            account_access_count: 0,
            reads: Arc::new(RwLock::new(Vec::new())),
            writes: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub fn record_read(
        &mut self,
        address: Address,
        slot: U256,
        _db: &VersionedStateDB<impl database_interface::Database>,
    ) {
        self.operations.push(Operation::Read { address, slot });
        self.accessed_slots.insert((address, slot));
        self.read_count += 1;
    }

    pub fn record_write(
        &mut self,
        address: Address,
        slot: U256,
        value: U256,
        original_value: U256,
    ) {
        self.operations.push(Operation::Write {
            address,
            slot,
            value,
            original_value,
        });
        self.accessed_slots.insert((address, slot));
        self.write_count += 1;
    }

    pub fn record_account_access(&mut self, address: Address) {
        self.operations.push(Operation::AccountAccess { address });
        self.accessed_accounts.insert(address);
        self.account_access_count += 1;
    }

    pub fn record_code_access(&mut self, address: Address) {
        self.operations.push(Operation::CodeAccess { address });
        self.accessed_accounts.insert(address);
    }

    pub fn detect_conflicts(&self, other: &OperationLog) -> Option<Conflict> {
        // First check for overlapping accounts
        if !self.accessed_accounts.is_disjoint(&other.accessed_accounts) {
            // Find the first conflicting account access
            for account in self
                .accessed_accounts
                .intersection(&other.accessed_accounts)
            {
                return Some(Conflict::AccountAccess(*account));
            }
        }

        // Then check for overlapping storage slots
        if !self.accessed_slots.is_disjoint(&other.accessed_slots) {
            // Find the first conflicting storage access
            for (address, slot) in self.accessed_slots.intersection(&other.accessed_slots) {
                return Some(Conflict::StorageSlot(*address, *slot));
            }
        }

        None
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn tx_index(&self) -> usize {
        self.tx_index
    }

    pub fn operations(&self) -> &[Operation] {
        &self.operations
    }
}

#[derive(Debug)]
pub enum Conflict {
    StorageSlot(Address, U256),
    AccountAccess(Address),
}

#[derive(Clone)]
pub struct SharedOperationLog {
    inner: Arc<RwLock<OperationLog>>,
}

impl SharedOperationLog {
    pub fn new(tx_index: usize, version: u64) -> Self {
        Self {
            inner: Arc::new(RwLock::new(OperationLog::new(tx_index, version))),
        }
    }

    pub fn record_read(
        &self,
        address: Address,
        slot: U256,
        db: &VersionedStateDB<impl database_interface::Database>,
    ) {
        if let Ok(mut log) = self.inner.write() {
            log.record_read(address, slot, db);
        }
    }

    pub fn record_write(&self, address: Address, slot: U256, value: U256, original_value: U256) {
        if let Ok(mut log) = self.inner.write() {
            log.record_write(address, slot, value, original_value);
        }
    }

    pub fn record_account_access(&self, address: Address) {
        if let Ok(mut log) = self.inner.write() {
            log.record_account_access(address);
        }
    }

    pub fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }

    pub fn read(&self) -> std::sync::LockResult<std::sync::RwLockReadGuard<'_, OperationLog>> {
        self.inner.read()
    }

    pub fn write(&self) -> std::sync::LockResult<std::sync::RwLockWriteGuard<'_, OperationLog>> {
        self.inner.write()
    }

    pub fn tx_index(&self) -> usize {
        self.inner.read().unwrap().tx_index
    }
}
