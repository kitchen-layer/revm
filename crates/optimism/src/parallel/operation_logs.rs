use crate::VersionedStateDB;
use anyhow::{Ok, Result};
use revm::context::Context;
use revm::context_interface::Journal;
use revm::database_interface;
use revm::primitives::{Address, U256};
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock}; // Import the Context struct
use thiserror::Error;

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

#[derive(Error, Debug)]
pub enum OperationLogError {
    #[error("Operation attempted on finalized log")]
    LogFinalized,
    #[error("Duplicate operation detected")]
    DuplicateOperation,
    #[error("Invalid operation order")]
    InvalidOrder,
    #[error("Version mismatch")]
    VersionMismatch,
}

#[derive(Debug)]
pub struct OperationLog<'a, BLOCK, TX, CFG, DB: revm::Database, JOURNAL: Journal<Database = DB>> {
    operations: Vec<Operation>,
    accessed_slots: HashSet<(Address, U256)>,
    accessed_accounts: HashSet<Address>,
    version: AtomicU64,
    tx_index: usize,
    read_count: usize,
    write_count: usize,
    account_access_count: usize,
    reads: Arc<RwLock<Vec<(Address, U256)>>>,
    writes: Arc<RwLock<Vec<(Address, U256, U256)>>>,
    finalized: bool,
    // Track operation order for validation
    operation_order: Vec<OperationType>,
    context: &'a Context<BLOCK, TX, CFG, DB, JOURNAL>,
}

#[derive(Debug, Clone, PartialEq)]
enum OperationType {
    Read(Address, U256),
    Write(Address, U256),
}

impl<'a, BLOCK, TX, CFG, DB: revm::Database, JOURNAL: Journal<Database = DB>>
    OperationLog<'a, BLOCK, TX, CFG, DB, JOURNAL>
{
    pub fn new(
        tx_index: usize,
        estimated_ops: usize,
        context: &'a Context<BLOCK, TX, CFG, DB, JOURNAL>,
    ) -> Self {
        Self {
            operations: Vec::with_capacity(64), // Pre-allocate for common case
            accessed_slots: HashSet::new(),
            accessed_accounts: HashSet::new(),
            version: AtomicU64::new(0),
            tx_index,
            read_count: 0,
            write_count: 0,
            account_access_count: 0,
            reads: Arc::new(RwLock::new(Vec::with_capacity(estimated_ops))),
            writes: Arc::new(RwLock::new(Vec::with_capacity(estimated_ops / 2))),
            finalized: false,
            operation_order: Vec::with_capacity(estimated_ops),
            context: context,
        }
    }

    pub fn record_read(&self, address: Address, slot: U256) {
        self.reads.write().unwrap().push((address, slot));
    }

    pub fn record_read_versioned(
        &mut self,
        address: Address,
        slot: U256,
        _db: &VersionedStateDB<impl database_interface::Database>,
    ) -> anyhow::Result<()> {
        if self.finalized {
            return Err(OperationLogError::LogFinalized.into());
        }

        // Check for duplicate reads using write lock
        {
            let mut reads = self.reads.write().unwrap();
            if reads.iter().any(|&(addr, s)| addr == address && s == slot) {
                return Err(OperationLogError::DuplicateOperation.into());
            }
            reads.push((address, slot));
        }

        self.operations.push(Operation::Read { address, slot });
        self.accessed_slots.insert((address, slot));
        self.read_count += 1;
        self.operation_order
            .push(OperationType::Read(address, slot));
        Ok(())
    }

    pub fn record_write(&self, address: Address, slot: U256, value: U256) {
        self.writes.write().unwrap().push((address, slot, value));
    }

    pub fn record_write_versioned(
        &mut self,
        address: Address,
        slot: U256,
        value: U256,
        original_value: U256,
    ) -> Result<()> {
        if self.finalized {
            return Err(OperationLogError::LogFinalized.into());
        }

        self.operations.push(Operation::Write {
            address,
            slot,
            value,
            original_value,
        });
        self.accessed_slots.insert((address, slot));

        // Use write lock for modification
        self.writes.write().unwrap().push((address, slot, value));

        self.write_count += 1;
        self.operation_order
            .push(OperationType::Write(address, slot));
        Ok(())
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

    pub fn get_block_info(&self) -> &BLOCK {
        &self.context.block
    }

    pub fn get_tx_info(&self) -> &TX {
        &self.context.tx
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
        op_log: &OperationLog<'a, BLOCK, TX, CFG, DB, JOURNAL>,
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

    pub fn version(&self) -> u64 {
        self.version.load(Ordering::SeqCst)
    }

    pub fn tx_index(&self) -> usize {
        self.tx_index
    }

    pub fn operations(&self) -> &[Operation] {
        &self.operations
    }

    pub fn validate(&self) -> Result<()> {
        // Check for duplicate operations
        let mut seen = HashSet::new();
        for op in &self.operation_order {
            match op {
                OperationType::Read(addr, slot) => {
                    let key = (addr, slot, true); // true for read
                    if !seen.insert(key) {
                        return Err(OperationLogError::DuplicateOperation.into());
                    }
                }
                OperationType::Write(addr, slot) => {
                    let key = (addr, slot, false); // false for write
                    if !seen.insert(key) {
                        return Err(OperationLogError::DuplicateOperation.into());
                    }
                }
            }
        }

        // Verify read-before-write ordering
        let mut write_positions = HashSet::new();
        for (i, op) in self.operation_order.iter().enumerate() {
            if let OperationType::Write(addr, slot) = op {
                write_positions.insert((addr, slot, i));
            }
        }

        for (i, op) in self.operation_order.iter().enumerate() {
            if let OperationType::Read(addr, slot) = op {
                // Check if there's a write to the same address/slot before this read
                if write_positions
                    .iter()
                    .any(|(w_addr, w_slot, w_pos)| *w_addr == addr && *w_slot == slot && w_pos < &i)
                {
                    return Err(OperationLogError::InvalidOrder.into());
                }
            }
        }

        Ok(())
    }

    pub fn finalize(&mut self) -> Result<()> {
        if self.finalized {
            return Err(OperationLogError::LogFinalized.into());
        }

        self.validate()?;
        self.finalized = true;
        Ok(())
    }

    pub fn set_version(&self, version: u64) -> Result<()> {
        if self.finalized {
            return Err(OperationLogError::LogFinalized.into());
        }
        self.version.store(version, Ordering::SeqCst);
        Ok(())
    }
}

#[derive(Debug)]
pub enum Conflict {
    StorageSlot(Address, U256),
    AccountAccess(Address),
}

#[derive(Clone)]
pub struct SharedOperationLog<
    'a,
    BLOCK,
    TX,
    CFG,
    DB: revm::Database,
    JOURNAL: Journal<Database = DB>,
> {
    inner: Arc<RwLock<OperationLog<'a, BLOCK, TX, CFG, DB, JOURNAL>>>,
}

impl<'a, BLOCK, TX, CFG, DB: revm::Database, JOURNAL: Journal<Database = DB>>
    SharedOperationLog<'a, BLOCK, TX, CFG, DB, JOURNAL>
{
    pub fn new(context: &'a Context<BLOCK, TX, CFG, DB, JOURNAL>) -> Self {
        Self {
            inner: Arc::new(RwLock::new(OperationLog::new(0, 64, context))),
        }
    }

    pub fn record_read(&self, address: Address, slot: U256) -> Result<()> {
        let log = self
            .inner
            .write()
            .map_err(|_| OperationLogError::LogFinalized)?;
        log.record_read(address, slot);
        Ok(())
    }

    pub fn record_write(&self, address: Address, slot: U256, value: U256) -> Result<()> {
        let log = self
            .inner
            .write()
            .map_err(|_| OperationLogError::LogFinalized)?;
        log.record_write(address, slot, value);
        Ok(())
    }

    pub fn record_account_access(&self, address: Address) -> Result<()> {
        let mut log = self
            .inner
            .write()
            .map_err(|_| OperationLogError::LogFinalized)?;
        log.record_account_access(address);
        Ok(())
    }

    pub fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }

    pub fn read(
        &self,
    ) -> std::sync::LockResult<
        std::sync::RwLockReadGuard<'_, OperationLog<'a, BLOCK, TX, CFG, DB, JOURNAL>>,
    > {
        self.inner.read()
    }

    pub fn write(
        &self,
    ) -> std::sync::LockResult<
        std::sync::RwLockWriteGuard<'_, OperationLog<'a, BLOCK, TX, CFG, DB, JOURNAL>>,
    > {
        self.inner.write()
    }

    pub fn tx_index(&self) -> usize {
        self.inner.read().unwrap().tx_index
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use revm::primitives::{Address, U256};
    use std::str::FromStr;

    use crate::db::versioned::VersionedStateDB;
    use revm::database::CacheDB;
    use revm::database::EmptyDBTyped;
    use revm::specification::hardfork::SpecId;

    // Helper function to create a real context
    fn create_test_context() -> Context<
        (),
        (),
        (),
        VersionedStateDB<CacheDB<EmptyDBTyped<()>>>,
        revm::JournaledState<VersionedStateDB<CacheDB<EmptyDBTyped<()>>>>,
    > {
        // Create a real context with necessary parameters
        let db = VersionedStateDB::new(CacheDB::new(EmptyDBTyped::new()));
        let spec = SpecId::CANCUN;
        let journaled_state = revm::JournaledState::new(spec, db);
        Context::new(db)
    }

    #[test]
    fn test_basic_operation_recording() -> Result<()> {
        let context = create_test_context();
        let mut log = OperationLog::new(0, 64, &context);

        let addr = Address::from_str("0x1234567890123456789012345678901234567890").unwrap();
        let slot = U256::from(1);
        let value = U256::from(100);
        let original_value = U256::from(0);

        // Test read recording
        log.record_read_versioned(addr, slot, &context.db)?;
        assert_eq!(log.read_count, 1);

        // Test write recording
        log.record_write_versioned(addr, slot, value, original_value)?;
        assert_eq!(log.write_count, 1);

        // Test account access recording
        log.record_account_access(addr);
        assert_eq!(log.account_access_count, 1);

        Ok(())
    }

    #[test]
    fn test_conflict_detection() -> Result<()> {
        let context = create_test_context();
        let mut log = OperationLog::new(0, 64, &context);

        let addr = Address::from_str("0x1234567890123456789012345678901234567890").unwrap();
        let slot = U256::from(1);
        let value = U256::from(100);
        let original_value = U256::from(0);

        // Record read and write to same slot
        log.record_read_versioned(addr, slot, &context.db)?;
        log.record_write_versioned(addr, slot, value, original_value)?;

        let conflicts = log.check_conflicts();
        assert_eq!(conflicts.len(), 1);
        assert_eq!(conflicts[0], (addr, slot));

        Ok(())
    }

    #[test]
    fn test_validation_and_finalization() -> Result<()> {
        let context = create_test_context();
        let mut log = OperationLog::new(0, 64, &context);

        let addr = Address::from_str("0x1234567890123456789012345678901234567890").unwrap();
        let slot = U256::from(1);
        let value = U256::from(100);
        let original_value = U256::from(0);

        // Record operations in valid order
        log.record_read_versioned(addr, slot, &context.db)?;
        log.record_write_versioned(addr, slot, value, original_value)?;

        // Validate and finalize
        assert!(log.validate().is_ok());
        assert!(log.finalize().is_ok());

        // Attempt to record after finalization
        assert!(log.record_read_versioned(addr, slot, &context.db).is_err());

        Ok(())
    }

    #[test]
    fn test_shared_operation_log() -> Result<()> {
        let context = create_test_context();
        let shared_log = SharedOperationLog::new(&context);

        let addr = Address::from_str("0x1234567890123456789012345678901234567890").unwrap();
        let slot = U256::from(1);
        let value = U256::from(100);

        // Test concurrent access
        let shared_log_clone = shared_log.clone();

        // Record operations from different threads
        let handle = std::thread::spawn(move || {
            shared_log_clone.record_read(addr, slot).unwrap();
        });

        shared_log.record_write(addr, slot, value)?;
        handle.join().unwrap();

        // Verify the operations were recorded
        let log = shared_log.read().unwrap();
        let conflicts = log.check_conflicts();
        assert_eq!(conflicts.len(), 1);

        Ok(())
    }

    #[test]
    fn test_duplicate_operations() -> Result<()> {
        let context = create_test_context();
        let mut log = OperationLog::new(0, 64, &context);

        let addr = Address::from_str("0x1234567890123456789012345678901234567890").unwrap();
        let slot = U256::from(1);

        // First read should succeed
        log.record_read_versioned(addr, slot, &context.db)?;

        // Second read to same slot should fail
        assert!(log.record_read_versioned(addr, slot, &context.db).is_err());

        Ok(())
    }

    #[test]
    fn test_version_management() -> Result<()> {
        let context = create_test_context();
        let mut log = OperationLog::new(0, 64, &context);

        // Set initial version
        log.set_version(1)?;
        assert_eq!(log.version(), 1);

        // Finalize the log
        log.finalize()?;

        // Attempt to change version after finalization
        assert!(log.set_version(2).is_err());

        Ok(())
    }
}
