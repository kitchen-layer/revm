use super::operation_logs::Operation;
use revm::primitives::{Address, U256};
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
struct AccessPattern {
    frequency: u32,
    last_accessed: Instant,
    avg_gas_cost: u64,
    conflict_rate: f64,
    typical_dependencies: HashSet<Address>,
}

#[derive(Debug)]
pub struct DependencyPredictor {
    // Track patterns per address and storage slot
    storage_patterns: HashMap<(Address, U256), AccessPattern>,
    // Track patterns per contract
    contract_patterns: HashMap<Address, AccessPattern>,
    // Track common transaction sequences
    sequence_patterns: HashMap<Vec<Address>, u32>,
    // Recent transaction history
    recent_transactions: VecDeque<(Address, Vec<Address>)>,
    // Configuration
    pattern_timeout: Duration,
    max_history: usize,
}

impl DependencyPredictor {
    pub fn new(pattern_timeout: Duration, max_history: usize) -> Self {
        Self {
            storage_patterns: HashMap::new(),
            contract_patterns: HashMap::new(),
            sequence_patterns: HashMap::new(),
            recent_transactions: VecDeque::new(),
            pattern_timeout,
            max_history,
        }
    }

    pub fn analyze_operation_log(&mut self, log: &Operation, gas_used: u64) {
        let mut accessed_addresses = HashSet::new();
        let mut accessed_slots = HashSet::new();

        // Analyze operations
        match log {
            Operation::Read { address, slot } => {
                accessed_addresses.insert(Address::from_slice(address.as_slice()));
                accessed_slots.insert((Address::from_slice(address.as_slice()), slot.clone()));
                self.update_storage_pattern(
                    Address::from_slice(address.as_slice()),
                    slot.clone(),
                    gas_used,
                    false,
                );
            }
            Operation::Write { address, slot, .. } => {
                accessed_addresses.insert(Address::from_slice(address.as_slice()));
                accessed_slots.insert((Address::from_slice(address.as_slice()), slot.clone()));
                self.update_storage_pattern(
                    Address::from_slice(address.as_slice()),
                    slot.clone(),
                    gas_used,
                    true,
                );
            }
            Operation::AccountAccess { address } | Operation::CodeAccess { address } => {
                accessed_addresses.insert(Address::from_slice(address.as_slice()));
                self.update_contract_pattern(Address::from_slice(address.as_slice()), gas_used);
            }
        }

        // Update sequence patterns
        self.update_sequence_patterns(accessed_addresses);
    }

    fn update_storage_pattern(
        &mut self,
        address: Address,
        slot: U256,
        gas_used: u64,
        is_write: bool,
    ) {
        let pattern = self
            .storage_patterns
            .entry((address, slot))
            .or_insert_with(|| AccessPattern {
                frequency: 0,
                last_accessed: Instant::now(),
                avg_gas_cost: 0,
                conflict_rate: 0.0,
                typical_dependencies: HashSet::new(),
            });

        pattern.frequency += 1;
        pattern.last_accessed = Instant::now();
        pattern.avg_gas_cost = (pattern.avg_gas_cost + gas_used) / 2;
        if is_write {
            pattern.conflict_rate = (pattern.conflict_rate * 0.9) + 0.1; // Increase conflict probability
        }
    }

    fn update_contract_pattern(&mut self, address: Address, gas_used: u64) {
        let pattern = self
            .contract_patterns
            .entry(address)
            .or_insert_with(|| AccessPattern {
                frequency: 0,
                last_accessed: Instant::now(),
                avg_gas_cost: 0,
                conflict_rate: 0.0,
                typical_dependencies: HashSet::new(),
            });

        pattern.frequency += 1;
        pattern.last_accessed = Instant::now();
        pattern.avg_gas_cost = (pattern.avg_gas_cost + gas_used) / 2;
    }

    fn update_sequence_patterns(&mut self, addresses: HashSet<Address>) {
        // Add new transaction to history
        if let Some(last_tx) = self.recent_transactions.back() {
            let sequence: Vec<Address> = last_tx
                .1
                .iter()
                .cloned()
                .chain(addresses.iter().cloned())
                .collect();

            *self.sequence_patterns.entry(sequence).or_insert(0) += 1;
        }

        // Update recent transactions
        self.recent_transactions.push_back((
            *addresses.iter().next().unwrap_or(&Address::default()),
            addresses.into_iter().collect(),
        ));

        // Maintain history size
        while self.recent_transactions.len() > self.max_history {
            self.recent_transactions.pop_front();
        }
    }

    pub fn predict_dependencies(&self, addresses: &HashSet<Address>) -> HashSet<Address> {
        let mut predicted = HashSet::new();

        // Check sequence patterns
        if let Some(recent) = self.recent_transactions.back() {
            for sequence in self.sequence_patterns.keys() {
                if sequence.starts_with(&recent.1) {
                    predicted.extend(sequence.iter().skip(recent.1.len()));
                }
            }
        }

        // Check contract patterns
        for address in addresses {
            if let Some(pattern) = self.contract_patterns.get(address) {
                predicted.extend(&pattern.typical_dependencies);
            }
        }

        predicted
    }

    pub fn predict_conflict_probability(&self, address: Address, slot: U256) -> f64 {
        self.storage_patterns
            .get(&(address, slot))
            .map(|pattern| pattern.conflict_rate)
            .unwrap_or(0.0)
    }

    pub fn cleanup_old_patterns(&mut self) {
        let now = Instant::now();

        // Clean up storage patterns
        self.storage_patterns
            .retain(|_, pattern| now.duration_since(pattern.last_accessed) < self.pattern_timeout);

        // Clean up contract patterns
        self.contract_patterns
            .retain(|_, pattern| now.duration_since(pattern.last_accessed) < self.pattern_timeout);

        // Clean up sequence patterns with low frequency
        self.sequence_patterns.retain(|_, freq| *freq > 1);
    }
}
