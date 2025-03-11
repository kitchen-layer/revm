// src/optimism/db/versioned.rs
use revm::database_interface::{Database, DatabaseCommit};
use revm::primitives::hash_map::HashMap;
use revm::primitives::{Address, B256, U256};
use revm::state::{Account, AccountInfo, Bytecode, EvmStorageSlot};
use std::collections::BTreeMap;
use thiserror::Error;

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

#[derive(Debug, thiserror::Error)]
pub enum VersionedStateDBError {
    #[error("Version {0} not found")]
    VersionNotFound(u64),
    #[error("Database error: {0}")]
    DatabaseError(String),
    #[error("No active version")]
    NoActiveVersion,
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

    pub fn switch_to_version(&mut self, version: u64) -> Result<(), VersionedStateDBError> {
        if !self.versions.contains_key(&version) {
            return Err(VersionedStateDBError::VersionNotFound(version));
        }
        self.current_version = version;
        Ok(())
    }

    pub fn read_storage(
        &mut self,
        address: Address,
        slot: U256,
    ) -> Result<U256, VersionedStateDBError> {
        // Check versions from current back to base for the value
        let mut current = self.current_version;

        while current > 0 {
            if let Some(snapshot) = self.versions.get(&current) {
                if let Some(value) = snapshot.storage_changes.get(&(address, slot)) {
                    // Store the current value before mutable borrow
                    let return_value = *value;
                    // Update metadata
                    self.update_read_metadata(address, slot, current);
                    return Ok(return_value);
                }
                current = snapshot.parent_version;
            }
        }

        // If not found in versions, read from base DB
        let value = self
            .base
            .storage(address, slot)
            .map_err(|e| VersionedStateDBError::DatabaseError(e.to_string()))?;
        self.update_read_metadata(address, slot, 0);
        Ok(value)
    }

    pub fn write_storage(
        &mut self,
        address: Address,
        slot: U256,
        value: U256,
    ) -> Result<(), VersionedStateDBError> {
        if let Some(snapshot) = self.versions.get_mut(&self.current_version) {
            snapshot.storage_changes.insert((address, slot), value);
            self.update_write_metadata(address, slot, self.current_version);
            Ok(())
        } else {
            Err(VersionedStateDBError::NoActiveVersion)
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

    pub fn commit_version(&mut self, version: u64) -> Result<(), VersionedStateDBError>
    where
        DB: DatabaseCommit,
    {
        if let Some(snapshot) = self.versions.remove(&version) {
            // Group storage changes by address to minimize database commits
            let mut storage_changes: HashMap<Address, Vec<(U256, U256)>> = HashMap::new();
            for ((address, slot), value) in snapshot.storage_changes {
                storage_changes
                    .entry(address)
                    .or_default()
                    .push((slot, value));
            }

            // Apply storage changes to base DB, grouped by address
            for (address, slots) in storage_changes {
                let mut account = Account::default();
                // Update storage slots for this account
                for (slot, value) in slots {
                    account.storage.insert(slot, EvmStorageSlot::new(value));
                }

                // Commit the account with its storage changes
                self.base.commit([(address, account)].into())
            }

            // Apply account changes
            for (address, account_snapshot) in snapshot.account_changes {
                let mut account = Account::default();
                account.info = AccountInfo {
                    balance: account_snapshot.balance,
                    nonce: account_snapshot.nonce,
                    code_hash: account_snapshot.code_hash,
                    code: None, // Code will be loaded on-demand
                };

                // Commit the account changes
                self.base.commit([(address, account)].into())
            }

            // Clean up operation metadata for the committed version
            self.operation_metadata.retain(|_, metadata| {
                metadata.last_read_version != version && metadata.last_write_version != version
            });

            Ok(())
        } else {
            Err(VersionedStateDBError::VersionNotFound(version))
        }
    }
}

impl<DB: Database> Database for VersionedStateDB<DB> {
    type Error = DB::Error;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        // Implementation here
        Ok(None)
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        // Implementation here
        Ok(Bytecode::default())
    }

    fn storage(&mut self, address: Address, index: U256) -> Result<U256, Self::Error> {
        // Implementation here
        Ok(U256::from(0))
    }

    fn block_hash(&mut self, number: u64) -> Result<B256, Self::Error> {
        // Implementation here
        Ok(B256::default())
    }
}
