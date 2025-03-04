// src/optimism/db/versioned.rs
pub struct VersionedStateDB {
    base: Box<dyn Database>,                // Base state reference
    versions: BTreeMap<u64, StateSnapshot>, // Delta-based snapshots
    current_version: u64,                   // Active version ID
    operation_metadata: HashMap<(Address, U256), OperationMetadata>, // Access tracking
}

// Added transaction-aware metadata structure
#[derive(Clone, Debug)]
struct OperationMetadata {
    last_version: u64,        // Version of last access
    last_transaction: u64,    // Transaction ID of last modifier
    read_count: AtomicU32,    // Concurrent read tracking
    write_locked: AtomicBool, // Write conflict detection
}

impl VersionedStateDB {
    pub fn new(base: Box<dyn Database>) -> Self {
        Self {
            base,
            versions: BTreeMap::new(),
            current_version: 0,
            operation_metadata: HashMap::new(),
        }
    }

    // Modified insert to track transaction context
    pub fn insert(
        &mut self,
        address: Address,
        slot: U256,
        value: U256,
        transaction_id: u64,
    ) -> Result<(), DatabaseError> {
        let version = self.current_version;
        let snapshot = self
            .versions
            .entry(version)
            .or_insert_with(StateSnapshot::new);

        // Track transaction-specific metadata
        self.operation_metadata
            .entry((address, slot))
            .and_modify(|meta| {
                meta.last_transaction = transaction_id;
                meta.last_version = version;
            })
            .or_insert(OperationMetadata {
                last_version: version,
                last_transaction: transaction_id,
                read_count: AtomicU32::new(0),
                write_locked: AtomicBool::new(false),
            });

        snapshot.insert(address, slot, value);
        Ok(())
    }
}

impl Database for VersionedStateDB {
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
