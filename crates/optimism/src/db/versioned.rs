// src/optimism/db/versioned.rs
use revm::database_interface::Database;
use revm::primitives::{Address, B256, U256};
use std::collections::{BTreeMap, HashMap};

#[derive(Debug)]
pub struct OperationMetadata {
    last_read_version: u64,
    last_write_version: u64,
    access_count: u32,
    is_hot_slot: bool,
}

#[derive(Debug)]
pub struct StateSnapshot {
    storage_changes: HashMap<(Address, U256), U256>,
    account_changes: HashMap<Address, AccountSnapshot>,
    parent_version: u64,
}

#[derive(Debug)]
pub struct AccountSnapshot {
    balance: U256,
    nonce: u64,
    code_hash: B256,
}

pub struct VersionedStateDB<DB: Database> {
    base: DB,
    versions: BTreeMap<u64, StateSnapshot>,
    current_version: u64,
    operation_metadata: HashMap<(Address, U256), OperationMetadata>,
}

impl<DB: Database> VersionedStateDB<DB> {
    pub fn new(database: DB) -> Self {
        Self {
            base: database,
            versions: BTreeMap::new(),
            current_version: 0,
            operation_metadata: HashMap::new(),
        }
    }

    pub fn create_snapshot(&mut self) -> u64 {
        let new_version = self.current_version + 1;
        let snapshot = StateSnapshot {
            storage_changes: HashMap::new(),
            account_changes: HashMap::new(),
            parent_version: self.current_version,
        };

        self.versions.insert(new_version, snapshot);
        self.current_version = new_version;
        new_version
    }

    pub fn switch_to_version(&mut self, version: u64) -> Result<(), DB::Error> {
        if !self.versions.contains_key(&version) {
            return Err(DB::Error::Custom("Version does not exist".into()));
        }
        self.current_version = version;
        Ok(())
    }

    pub fn read_storage(&mut self, address: Address, slot: U256) -> Result<U256, DB::Error> {
        // Check versions from current back to base for the value
        let mut current = self.current_version;

        while current > 0 {
            if let Some(snapshot) = self.versions.get(&current) {
                if let Some(value) = snapshot.storage_changes.get(&(address, slot)) {
                    // Update metadata
                    self.update_read_metadata(address, slot, current);
                    return Ok(*value);
                }
                current = snapshot.parent_version;
            }
        }

        // If not found in versions, read from base DB
        let value = self.base.storage(address, slot)?;
        self.update_read_metadata(address, slot, 0);
        Ok(value)
    }

    pub fn write_storage(
        &mut self,
        address: Address,
        slot: U256,
        value: U256,
    ) -> Result<(), DB::Error> {
        if let Some(snapshot) = self.versions.get_mut(&self.current_version) {
            snapshot.storage_changes.insert((address, slot), value);
            self.update_write_metadata(address, slot, self.current_version);
            Ok(())
        } else {
            Err(DB::Error::Custom("No active version".into()))
        }
    }

    fn update_read_metadata(&mut self, address: Address, slot: U256, version: u64) {
        let metadata =
            self.operation_metadata
                .entry((address, slot))
                .or_insert(OperationMetadata {
                    last_read_version: version,
                    last_write_version: 0,
                    access_count: 0,
                    is_hot_slot: false,
                });

        metadata.last_read_version = version;
        metadata.access_count += 1;
        metadata.is_hot_slot = metadata.access_count > 10; // Threshold for hot slots
    }

    fn update_write_metadata(&mut self, address: Address, slot: U256, version: u64) {
        let metadata =
            self.operation_metadata
                .entry((address, slot))
                .or_insert(OperationMetadata {
                    last_read_version: 0,
                    last_write_version: version,
                    access_count: 0,
                    is_hot_slot: false,
                });

        metadata.last_write_version = version;
        metadata.access_count += 1;
        metadata.is_hot_slot = metadata.access_count > 10;
    }

    pub fn commit_version(&mut self, version: u64) -> Result<(), DB::Error> {
        if let Some(snapshot) = self.versions.remove(&version) {
            // Apply storage changes to base DB
            for ((address, slot), value) in snapshot.storage_changes {
                self.base.set_storage(address, slot, value)?;
            }

            // Apply account changes
            for (address, account) in snapshot.account_changes {
                // Implementation needed for account updates
                // This would update balance, nonce, and code hash in the base DB
            }
            Ok(())
        } else {
            Err(DB::Error::Custom("Version not found".into()))
        }
    }
}

impl Database for VersionedStateDB<dyn Database> {
    fn get(&self, address: &Address, slot: U256) -> Result<Option<U256>, DatabaseError> {
        let version = self.current_version;
        match self.versions.get(&version) {
            Some(snapshot) => snapshot.get(address, slot),
            None => Err(DatabaseError::VersionNotFound(version)),
        }
    }

    fn insert(&mut self, address: Address, slot: U256, value: U256) -> Result<(), DatabaseError> {
        let version = self.current_version;
        let snapshot = self
            .versions
            .entry(version)
            .or_insert_with(StateSnapshot::new);
        snapshot.insert(address, slot, value);
        Ok(())
    }
}
