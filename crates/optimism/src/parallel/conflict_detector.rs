use super::operation_logs::{Operation, OperationLog};
use revm::primitives::{Address, U256};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone)]
pub enum ConflictType {
    TransactionLevel {
        tx1_index: usize,
        tx2_index: usize,
        conflicting_addresses: HashSet<Address>,
    },
    OperationLevel {
        tx_index: usize,
        op_index: usize,
        address: Address,
        slot: Option<U256>,
    },
}

#[derive(Default)]
pub struct ConflictDetector {
    // Track transaction-level access patterns
    tx_read_sets: HashMap<usize, HashSet<(Address, U256)>>,
    tx_write_sets: HashMap<usize, HashSet<(Address, U256)>>,

    // Track operation-level details
    operation_history: HashMap<(Address, U256), Vec<(usize, usize, bool)>>, // (tx_idx, op_idx, is_write)

    // Cache for quick conflict lookups
    conflict_cache: HashMap<(usize, usize), Vec<ConflictType>>,
}

impl ConflictDetector {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn analyze_operation_log(&mut self, log: &OperationLog) {
        let tx_idx = log.tx_index();
        let mut read_set = HashSet::new();
        let mut write_set = HashSet::new();

        // Process each operation and build access sets
        for (op_idx, op) in log.operations().iter().enumerate() {
            match op {
                Operation::Read { address, slot } => {
                    read_set.insert((*address, *slot));
                    self.record_operation(*address, *slot, tx_idx, op_idx, false);
                }
                Operation::Write {
                    address,
                    slot,
                    value: _,
                    original_value: _,
                } => {
                    write_set.insert((*address, *slot));
                    self.record_operation(*address, *slot, tx_idx, op_idx, true);
                }
                Operation::AccountAccess { address } => {
                    read_set.insert((*address, U256::ZERO));
                    self.record_operation(*address, U256::ZERO, tx_idx, op_idx, false);
                }
                Operation::CodeAccess { address } => {
                    read_set.insert((*address, U256::ZERO));
                    self.record_operation(*address, U256::ZERO, tx_idx, op_idx, false);
                }
            }
        }

        self.tx_read_sets.insert(tx_idx, read_set);
        self.tx_write_sets.insert(tx_idx, write_set);
    }

    fn record_operation(
        &mut self,
        address: Address,
        slot: U256,
        tx_idx: usize,
        op_idx: usize,
        is_write: bool,
    ) {
        self.operation_history
            .entry((address, slot))
            .or_default()
            .push((tx_idx, op_idx, is_write));
    }

    pub fn detect_conflicts(&mut self, tx_idx: usize) -> Vec<ConflictType> {
        // Check cache first
        if let Some(cached_conflicts) = self.get_cached_conflicts(tx_idx) {
            return cached_conflicts;
        }

        let mut conflicts = Vec::new();

        // Transaction-level conflict detection
        if let Some(current_reads) = self.tx_read_sets.get(&tx_idx) {
            if let Some(current_writes) = self.tx_write_sets.get(&tx_idx) {
                for (&other_tx_idx, other_writes) in &self.tx_write_sets {
                    if other_tx_idx >= tx_idx {
                        continue;
                    }

                    let mut conflicting_addresses = HashSet::new();

                    // Check read-write conflicts
                    for (addr, slot) in current_reads {
                        if other_writes.contains(&(*addr, *slot)) {
                            conflicting_addresses.insert(*addr);
                        }
                    }

                    // Check write-write conflicts
                    for (addr, slot) in current_writes {
                        if other_writes.contains(&(*addr, *slot)) {
                            conflicting_addresses.insert(*addr);
                        }
                    }

                    if !conflicting_addresses.is_empty() {
                        conflicts.push(ConflictType::TransactionLevel {
                            tx1_index: tx_idx,
                            tx2_index: other_tx_idx,
                            conflicting_addresses,
                        });
                    }
                }
            }
        }

        // Operation-level conflict detection
        if let Some(current_reads) = self.tx_read_sets.get(&tx_idx) {
            for (addr, slot) in current_reads {
                if let Some(history) = self.operation_history.get(&(*addr, *slot)) {
                    self.detect_operation_conflicts(
                        tx_idx,
                        *addr,
                        Some(*slot),
                        history,
                        &mut conflicts,
                    );
                }
            }
        }

        // Cache the results
        self.cache_conflicts(tx_idx, &conflicts);
        conflicts
    }

    fn detect_operation_conflicts(
        &self,
        tx_idx: usize,
        address: Address,
        slot: Option<U256>,
        history: &[(usize, usize, bool)],
        conflicts: &mut Vec<ConflictType>,
    ) {
        let mut last_write: Option<(usize, usize)> = None;

        for &(hist_tx_idx, hist_op_idx, is_write) in history {
            if hist_tx_idx >= tx_idx {
                continue;
            }

            if is_write {
                last_write = Some((hist_tx_idx, hist_op_idx));
            }
        }

        if let Some((write_tx_idx, write_op_idx)) = last_write {
            conflicts.push(ConflictType::OperationLevel {
                tx_index: write_tx_idx,
                op_index: write_op_idx,
                address,
                slot,
            });
        }
    }

    fn get_cached_conflicts(&self, tx_idx: usize) -> Option<Vec<ConflictType>> {
        self.conflict_cache.get(&(tx_idx, 0)).cloned()
    }

    fn cache_conflicts(&mut self, tx_idx: usize, conflicts: &[ConflictType]) {
        self.conflict_cache.insert((tx_idx, 0), conflicts.to_vec());
    }

    pub fn clear_cache(&mut self) {
        self.conflict_cache.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_conflict_detection() {
        let mut detector = ConflictDetector::new();

        // Create two operation logs with conflicting operations
        let mut log1 = OperationLog::new(0, 1);
        log1.record_write(
            Address::default(),
            U256::from(1),
            U256::from(100),
            U256::from(0),
        );

        let mut log2 = OperationLog::new(1, 1);
        log2.record_read(Address::default(), U256::from(1));

        detector.analyze_operation_log(&log1);
        detector.analyze_operation_log(&log2);

        let conflicts = detector.detect_conflicts(1);
        assert!(!conflicts.is_empty());
    }
}
