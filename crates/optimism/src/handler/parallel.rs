use super::operation_logs::{Conflict, OperationLog, SharedOperationLog};
use crate::db::versioned::VersionedStateDB;
use context::{ContextTr, Evm};
use crossbeam::channel::{bounded, Receiver, Sender};
use handler::{EvmTr, Frame, Handler};
use interpreter::{Host, InterpreterResult};
use metrics::{register_counter, register_gauge, register_histogram, Counter, Gauge, Histogram};
use primitives::{Address, U256};
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

// Tracks gas usage across parallel execution threads
pub struct ParallelGasTracker {
    total_gas_used: AtomicU64,
    gas_limit: u64,
}

impl ParallelGasTracker {
    pub fn new(gas_limit: u64) -> Self {
        Self {
            total_gas_used: AtomicU64::new(0),
            gas_limit,
        }
    }

    pub fn add_gas(&self, amount: u64) -> bool {
        let new_total = self.total_gas_used.fetch_add(amount, Ordering::Relaxed) + amount;
        new_total <= self.gas_limit
    }
}

// Tracks dependencies between transactions
#[derive(Default)]
struct DependencyTracker {
    // Maps transaction index to its dependencies
    dependencies: HashMap<usize, HashSet<usize>>,
    // Maps storage slots to transactions that access them
    slot_access: HashMap<(Address, U256), Vec<usize>>,
    // Maps accounts to transactions that access them
    account_access: HashMap<Address, Vec<usize>>,
}

impl DependencyTracker {
    fn add_dependency(&mut self, from_tx: usize, to_tx: usize) {
        self.dependencies.entry(from_tx).or_default().insert(to_tx);
    }

    fn get_dependencies(&self, tx_idx: usize) -> HashSet<usize> {
        self.dependencies.get(&tx_idx).cloned().unwrap_or_default()
    }

    fn record_slot_access(&mut self, address: Address, slot: U256, tx_idx: usize) {
        self.slot_access
            .entry((address, slot))
            .or_default()
            .push(tx_idx);
    }

    fn record_account_access(&mut self, address: Address, tx_idx: usize) {
        self.account_access.entry(address).or_default().push(tx_idx);
    }
}

// Enhanced execution result with metadata
#[derive(Clone)]
struct EnhancedExecutionResult {
    result: InterpreterResult,
    gas_used: u64,
    status: ExecutionStatus,
}

#[derive(Clone, PartialEq)]
enum ExecutionStatus {
    Success,
    Failed,
    Conflicted,
    NeedsReexecution,
}

// Predictive scheduling structures
#[derive(Default)]
struct AccessPattern {
    frequency: u32,
    last_accessed: u64,
    conflicting_txs: HashSet<usize>,
    avg_gas_cost: u64,
}

#[derive(Default)]
struct PredictiveScheduler {
    // Track access patterns for storage slots
    storage_patterns: HashMap<(Address, U256), AccessPattern>,
    // Track access patterns for accounts
    account_patterns: HashMap<Address, AccessPattern>,
    // Track transaction execution costs
    tx_costs: HashMap<usize, u64>,
    // Current block number for decay calculations
    current_block: u64,
}

impl PredictiveScheduler {
    fn new(current_block: u64) -> Self {
        Self {
            storage_patterns: HashMap::new(),
            account_patterns: HashMap::new(),
            tx_costs: HashMap::new(),
            current_block,
        }
    }

    fn update_storage_pattern(
        &mut self,
        address: Address,
        slot: U256,
        tx_idx: usize,
        gas_cost: u64,
    ) {
        let pattern = self.storage_patterns.entry((address, slot)).or_default();

        pattern.frequency += 1;
        pattern.last_accessed = self.current_block;
        pattern.conflicting_txs.insert(tx_idx);

        // Update moving average of gas cost
        pattern.avg_gas_cost = (pattern.avg_gas_cost + gas_cost) / 2;
    }

    fn update_account_pattern(&mut self, address: Address, tx_idx: usize, gas_cost: u64) {
        let pattern = self.account_patterns.entry(address).or_default();

        pattern.frequency += 1;
        pattern.last_accessed = self.current_block;
        pattern.conflicting_txs.insert(tx_idx);
        pattern.avg_gas_cost = (pattern.avg_gas_cost + gas_cost) / 2;
    }

    fn predict_conflicts(&self, tx_idx: usize) -> HashSet<usize> {
        let mut likely_conflicts = HashSet::new();

        // Collect all transactions that historically conflicted
        for pattern in self.storage_patterns.values() {
            if pattern.conflicting_txs.contains(&tx_idx) {
                likely_conflicts.extend(&pattern.conflicting_txs);
            }
        }

        for pattern in self.account_patterns.values() {
            if pattern.conflicting_txs.contains(&tx_idx) {
                likely_conflicts.extend(&pattern.conflicting_txs);
            }
        }

        likely_conflicts
    }

    fn estimate_execution_cost(&self, tx_idx: usize) -> u64 {
        self.tx_costs.get(&tx_idx).copied().unwrap_or(0)
    }
}

// Performance metrics for adaptive tuning
#[derive(Default)]
struct ExecutionMetrics {
    batch_execution_times: Histogram,
    conflict_rate: Gauge,
    successful_txs: Counter,
    failed_txs: Counter,
    reexecuted_txs: Counter,
    avg_batch_size: Gauge,
    total_gas_used: Counter,
}

impl ExecutionMetrics {
    fn new() -> Self {
        Self {
            batch_execution_times: register_histogram!("batch_execution_times"),
            conflict_rate: register_gauge!("conflict_rate"),
            successful_txs: register_counter!("successful_txs"),
            failed_txs: register_counter!("failed_txs"),
            reexecuted_txs: register_counter!("reexecuted_txs"),
            avg_batch_size: register_gauge!("avg_batch_size"),
            total_gas_used: register_counter!("total_gas_used"),
        }
    }
}

// Adaptive batch sizing configuration
#[derive(Clone)]
struct BatchConfig {
    min_size: usize,
    max_size: usize,
    target_execution_time: Duration,
    conflict_threshold: f64,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            min_size: 1,
            max_size: 256,
            target_execution_time: Duration::from_millis(100),
            conflict_threshold: 0.2, // 20% conflict rate threshold
        }
    }
}

// Transaction priority and scheduling
#[derive(Clone, Debug)]
struct TransactionPriority {
    tx_index: usize,
    gas_price: U256,
    estimated_gas: u64,
    predicted_conflicts: usize,
    priority_score: f64,
}

impl TransactionPriority {
    fn calculate_priority_score(&mut self) {
        // Higher gas price and lower conflict probability leads to higher priority
        let conflict_factor = 1.0 / (1.0 + self.predicted_conflicts as f64);
        let gas_price_factor = self.gas_price.as_u64() as f64;
        self.priority_score = gas_price_factor * conflict_factor;
    }
}

// Historical performance tracking
#[derive(Debug)]
struct PerformanceWindow {
    timestamp: u64,
    execution_time: Duration,
    conflict_rate: f64,
    gas_used: u64,
    transactions_processed: usize,
}

#[derive(Debug)]
struct PerformanceHistory {
    windows: VecDeque<PerformanceWindow>,
    max_windows: usize,
    total_execution_time: Duration,
    total_conflicts: usize,
    total_transactions: usize,
}

impl PerformanceHistory {
    fn new(max_windows: usize) -> Self {
        Self {
            windows: VecDeque::with_capacity(max_windows),
            max_windows,
            total_execution_time: Duration::default(),
            total_conflicts: 0,
            total_transactions: 0,
        }
    }

    fn add_window(&mut self, window: PerformanceWindow) {
        if self.windows.len() >= self.max_windows {
            if let Some(old) = self.windows.pop_front() {
                self.total_execution_time -= old.execution_time;
                self.total_conflicts -=
                    (old.conflict_rate * old.transactions_processed as f64) as usize;
                self.total_transactions -= old.transactions_processed;
            }
        }

        self.total_execution_time += window.execution_time;
        self.total_conflicts +=
            (window.conflict_rate * window.transactions_processed as f64) as usize;
        self.total_transactions += window.transactions_processed;

        self.windows.push_back(window);
    }

    fn get_average_metrics(&self) -> (Duration, f64) {
        if self.total_transactions == 0 {
            return (Duration::default(), 0.0);
        }

        let avg_execution_time = self.total_execution_time / self.windows.len() as u32;
        let avg_conflict_rate = self.total_conflicts as f64 / self.total_transactions as f64;

        (avg_execution_time, avg_conflict_rate)
    }
}

// Enhanced thread pool management
struct AdaptiveThreadPool {
    pool: rayon::ThreadPool,
    current_size: AtomicUsize,
    min_threads: usize,
    max_threads: usize,
}

impl AdaptiveThreadPool {
    fn new(min_threads: usize, max_threads: usize) -> Result<Self, rayon::ThreadPoolBuildError> {
        let pool = ThreadPoolBuilder::new().num_threads(min_threads).build()?;

        Ok(Self {
            pool,
            current_size: AtomicUsize::new(min_threads),
            min_threads,
            max_threads,
        })
    }

    fn adjust_size(&self, performance_history: &PerformanceHistory) {
        let (avg_execution_time, avg_conflict_rate) = performance_history.get_average_metrics();
        let current = self.current_size.load(Ordering::Relaxed);

        let new_size = if avg_conflict_rate > 0.3 {
            // High conflict rate - reduce threads
            (current as f64 * 0.8) as usize
        } else if avg_execution_time > Duration::from_millis(200) {
            // Slow execution - increase threads
            (current as f64 * 1.2) as usize
        } else {
            current
        };

        let new_size = new_size.clamp(self.min_threads, self.max_threads);
        self.current_size.store(new_size, Ordering::Relaxed);

        // Rebuild pool if size changed
        if new_size != current {
            if let Ok(new_pool) = ThreadPoolBuilder::new().num_threads(new_size).build() {
                self.pool = new_pool;
            }
        }
    }
}

// Enhanced transaction priority scoring
#[derive(Debug, Clone)]
struct TransactionMetrics {
    gas_price: U256,
    estimated_gas: u64,
    sender_historical_conflicts: usize,
    accessed_hot_slots: usize,
    waiting_time: Duration,
}

impl TransactionPriority {
    fn new(tx_index: usize, metrics: TransactionMetrics) -> Self {
        let mut priority = Self {
            tx_index,
            gas_price: metrics.gas_price,
            estimated_gas: metrics.estimated_gas,
            predicted_conflicts: 0,
            priority_score: 0.0,
        };
        priority.calculate_priority_score_enhanced(&metrics);
        priority
    }

    fn calculate_priority_score_enhanced(&mut self, metrics: &TransactionMetrics) {
        // Base priority from gas price
        let gas_price_factor = self.gas_price.as_u64() as f64;

        // Conflict probability penalty
        let conflict_penalty = 1.0 / (1.0 + metrics.sender_historical_conflicts as f64);

        // Hot slot access penalty
        let hot_slot_penalty = 1.0 / (1.0 + metrics.accessed_hot_slots as f64);

        // Waiting time bonus (prevent starvation)
        let waiting_bonus = metrics.waiting_time.as_secs_f64() / 60.0; // Normalize to minutes

        // Combine factors with weights
        self.priority_score =
            gas_price_factor * conflict_penalty * hot_slot_penalty * (1.0 + waiting_bonus);
    }
}

pub struct ParallelExecutionHandler<DB: database_interface::Database> {
    gas_tracker: Arc<ParallelGasTracker>,
    versioned_db: Arc<parking_lot::RwLock<VersionedStateDB<DB>>>,
    operation_logs: Vec<SharedOperationLog>,
    max_parallel_threads: usize,
    conflict_channel: (Sender<Conflict>, Receiver<Conflict>),
    metrics: ExecutionMetrics,
    batch_config: BatchConfig,
    performance_history: PerformanceHistory,
    thread_pool: AdaptiveThreadPool,
}

impl<DB: database_interface::Database + Clone + Send + Sync + 'static>
    ParallelExecutionHandler<DB>
{
    pub fn new(db: DB, gas_limit: u64, max_threads: usize) -> Self {
        let versioned_db = Arc::new(parking_lot::RwLock::new(VersionedStateDB::new(db)));
        let (conflict_sender, conflict_receiver) = bounded(1024);

        Self {
            gas_tracker: Arc::new(ParallelGasTracker::new(gas_limit)),
            versioned_db,
            operation_logs: Vec::new(),
            max_parallel_threads: max_threads,
            conflict_channel: (conflict_sender, conflict_receiver),
            metrics: ExecutionMetrics::new(),
            batch_config: BatchConfig::default(),
            performance_history: PerformanceHistory::new(100), // Keep last 100 windows
            thread_pool: AdaptiveThreadPool::new(1, max_threads).unwrap(),
        }
    }

    pub fn execute_transactions_parallel<CTX, INSP, I, P>(
        &mut self,
        transactions: Vec<CTX::Tx>,
        evm: &mut Evm<CTX, INSP, I, P>,
    ) -> Result<Vec<InterpreterResult>, <CTX::Db as database_interface::Database>::Error>
    where
        CTX: ContextTr + Host + Send + Sync + Clone + 'static,
        INSP: Send + Sync + Clone + 'static,
        I: Send + Sync + Clone + 'static,
        P: Send + Sync + Clone + 'static,
    {
        // Create a new version for this batch
        let base_version = self.versioned_db.write().create_snapshot();

        // Initialize operation logs for each transaction
        self.operation_logs = transactions
            .iter()
            .enumerate()
            .map(|(idx, _)| SharedOperationLog::new(idx, base_version))
            .collect();

        // Clone necessary components for parallel execution
        let versioned_db = Arc::clone(&self.versioned_db);
        let gas_tracker = Arc::clone(&self.gas_tracker);
        let operation_logs = self.operation_logs.clone();
        let conflict_sender = self.conflict_channel.0.clone();

        // Execute transactions in parallel using rayon
        let results: Vec<_> = transactions
            .into_par_iter()
            .enumerate()
            .map(|(tx_idx, tx)| {
                let mut tx_evm = evm.clone();
                tx_evm.set_tx(tx);

                let op_log = &operation_logs[tx_idx];

                // Create a new version for this transaction
                let tx_version = versioned_db.write().create_snapshot();

                // Execute the transaction
                let result = self.execute_single_transaction(
                    &mut tx_evm,
                    op_log,
                    &versioned_db,
                    &gas_tracker,
                    &conflict_sender,
                );

                (tx_idx, result)
            })
            .collect();

        // Process results and handle conflicts
        self.handle_conflicts(&results)
    }

    fn execute_single_transaction<CTX, INSP, I, P>(
        &self,
        evm: &mut Evm<CTX, INSP, I, P>,
        op_log: &SharedOperationLog,
        versioned_db: &Arc<parking_lot::RwLock<VersionedStateDB<DB>>>,
        gas_tracker: &Arc<ParallelGasTracker>,
        conflict_sender: &Sender<Conflict>,
    ) -> InterpreterResult
    where
        CTX: ContextTr + Host,
    {
        // Wrap the execution with operation logging
        let result = {
            let _db_guard = versioned_db.read();
            // Execute the transaction
            evm.transact_previous()
        };

        // Check for conflicts with previously executed transactions
        if let Some(conflict) = self.check_conflicts(op_log) {
            conflict_sender.send(conflict).ok();
        }

        // Convert the execution result to InterpreterResult
        // This is a simplified conversion - actual implementation would need proper mapping
        InterpreterResult::default() // Placeholder - implement actual result conversion
    }

    fn check_conflicts(&self, op_log: &SharedOperationLog) -> Option<Conflict> {
        // Compare against all previous operation logs
        for prev_log in &self.operation_logs {
            if let Ok(current_log) = op_log.inner.read() {
                if let Ok(prev_log) = prev_log.inner.read() {
                    if let Some(conflict) = current_log.detect_conflicts(&prev_log) {
                        return Some(conflict);
                    }
                }
            }
        }
        None
    }

    fn handle_conflicts(
        &mut self,
        results: &[(usize, InterpreterResult)],
    ) -> Result<Vec<InterpreterResult>, <DB as database_interface::Database>::Error> {
        let mut final_results = vec![InterpreterResult::default(); results.len()];
        let mut conflicts = Vec::new();

        // Collect all conflicts
        while let Ok(conflict) = self.conflict_channel.1.try_recv() {
            conflicts.push(conflict);
        }

        if conflicts.is_empty() {
            // No conflicts - commit all transactions
            for (tx_idx, result) in results {
                final_results[*tx_idx] = result.clone();
            }
        } else {
            // Handle conflicts by re-executing conflicting transactions sequentially
            self.resolve_conflicts(&conflicts, &mut final_results)?;
        }

        Ok(final_results)
    }

    fn resolve_conflicts(
        &mut self,
        conflicts: &[Conflict],
        final_results: &mut [InterpreterResult],
    ) -> Result<(), <DB as database_interface::Database>::Error> {
        let mut dependency_tracker = DependencyTracker::default();
        let mut reexecution_queue = HashSet::new();

        // Build dependency graph from conflicts
        for conflict in conflicts {
            match conflict {
                Conflict::StorageSlot(address, slot) => {
                    // Find all transactions that accessed this slot
                    if let Some(log) = self.find_conflicting_logs(address, slot) {
                        for tx_idx in log {
                            reexecution_queue.insert(tx_idx);
                            // Record dependencies for future conflict prevention
                            dependency_tracker.record_slot_access(*address, *slot, tx_idx);
                        }
                    }
                }
                Conflict::AccountAccess(address) => {
                    // Find all transactions that accessed this account
                    if let Some(log) = self.find_conflicting_account_logs(address) {
                        for tx_idx in log {
                            reexecution_queue.insert(tx_idx);
                            dependency_tracker.record_account_access(*address, tx_idx);
                        }
                    }
                }
            }
        }

        // Sort transactions by dependencies
        let reexecution_order =
            self.determine_reexecution_order(&dependency_tracker, &reexecution_queue);

        // Re-execute conflicting transactions sequentially
        self.reexecute_transactions(reexecution_order, final_results)?;

        Ok(())
    }

    fn find_conflicting_logs(&self, address: &Address, slot: &U256) -> Option<Vec<usize>> {
        let mut conflicting_txs = Vec::new();
        for log in &self.operation_logs {
            if let Ok(op_log) = log.inner.read() {
                if op_log.accessed_slots.contains(&(*address, *slot)) {
                    conflicting_txs.push(op_log.tx_index());
                }
            }
        }
        Some(conflicting_txs)
    }

    fn find_conflicting_account_logs(&self, address: &Address) -> Option<Vec<usize>> {
        let mut conflicting_txs = Vec::new();
        for log in &self.operation_logs {
            if let Ok(op_log) = log.inner.read() {
                if op_log.accessed_accounts.contains(address) {
                    conflicting_txs.push(op_log.tx_index());
                }
            }
        }
        Some(conflicting_txs)
    }

    fn determine_reexecution_order(
        &self,
        dependency_tracker: &DependencyTracker,
        reexecution_queue: &HashSet<usize>,
    ) -> Vec<usize> {
        let mut ordered = Vec::new();
        let mut visited = HashSet::new();
        let mut temp_visited = HashSet::new();

        // Topological sort with cycle detection
        for &tx_idx in reexecution_queue {
            if !visited.contains(&tx_idx) {
                self.topological_sort(
                    tx_idx,
                    dependency_tracker,
                    &mut ordered,
                    &mut visited,
                    &mut temp_visited,
                );
            }
        }

        ordered
    }

    fn topological_sort(
        &self,
        tx_idx: usize,
        dependency_tracker: &DependencyTracker,
        ordered: &mut Vec<usize>,
        visited: &mut HashSet<usize>,
        temp_visited: &mut HashSet<usize>,
    ) {
        if temp_visited.contains(&tx_idx) {
            // Cycle detected, break dependency
            return;
        }
        if visited.contains(&tx_idx) {
            return;
        }

        temp_visited.insert(tx_idx);

        for &dep_tx in &dependency_tracker.get_dependencies(tx_idx) {
            self.topological_sort(dep_tx, dependency_tracker, ordered, visited, temp_visited);
        }

        temp_visited.remove(&tx_idx);
        visited.insert(tx_idx);
        ordered.push(tx_idx);
    }

    fn reexecute_transactions(
        &mut self,
        reexecution_order: Vec<usize>,
        final_results: &mut [InterpreterResult],
    ) -> Result<(), <DB as database_interface::Database>::Error> {
        let mut current_version = self.versioned_db.write().create_snapshot();

        for tx_idx in reexecution_order {
            // Switch to a new version for this transaction
            self.versioned_db
                .write()
                .switch_to_version(current_version)?;

            // Clear previous operation log
            if let Some(log) = self.operation_logs.get(tx_idx) {
                if let Ok(mut op_log) = log.inner.write() {
                    *op_log = OperationLog::new(tx_idx, current_version);
                }
            }

            // Re-execute transaction
            // Note: This is a placeholder - actual implementation would need to re-execute the transaction
            // using the original transaction data and EVM instance

            // Create new version for next transaction
            current_version = self.versioned_db.write().create_snapshot();
        }

        Ok(())
    }

    // Add method to track hot storage slots for optimization
    fn update_hot_slots(&self) {
        for log in &self.operation_logs {
            if let Ok(op_log) = log.inner.read() {
                let stats = op_log.get_statistics();
                // Implement hot slot detection logic based on access patterns
                // This could be used to optimize future execution scheduling
            }
        }
    }

    // Add predictive scheduler to the handler
    fn reexecute_transaction<CTX, INSP, I, P>(
        &mut self,
        tx_idx: usize,
        evm: &mut Evm<CTX, INSP, I, P>,
        current_version: u64,
    ) -> Result<EnhancedExecutionResult, <DB as database_interface::Database>::Error>
    where
        CTX: ContextTr + Host + Clone,
        INSP: Clone,
        I: Clone,
        P: Clone,
    {
        // Switch to the correct version
        self.versioned_db
            .write()
            .switch_to_version(current_version)?;

        // Clear and reinitialize operation log
        if let Some(log) = self.operation_logs.get(tx_idx) {
            if let Ok(mut op_log) = log.inner.write() {
                *op_log = OperationLog::new(tx_idx, current_version);
            }
        }

        // Execute transaction with enhanced monitoring
        let start_gas = self.gas_tracker.total_gas_used.load(Ordering::Relaxed);
        let result = evm.transact_previous();
        let gas_used = self.gas_tracker.total_gas_used.load(Ordering::Relaxed) - start_gas;

        let enhanced_result = EnhancedExecutionResult {
            result,
            gas_used,
            status: ExecutionStatus::Success,
        };

        Ok(enhanced_result)
    }

    fn reexecute_transactions_with_prediction<CTX, INSP, I, P>(
        &mut self,
        reexecution_order: Vec<usize>,
        final_results: &mut [InterpreterResult],
        evm: &mut Evm<CTX, INSP, I, P>,
        scheduler: &mut PredictiveScheduler,
    ) -> Result<(), <DB as database_interface::Database>::Error>
    where
        CTX: ContextTr + Host + Clone + Send + Sync + 'static,
        INSP: Clone + Send + Sync + 'static,
        I: Clone + Send + Sync + 'static,
        P: Clone + Send + Sync + 'static,
    {
        let mut current_version = self.versioned_db.write().create_snapshot();
        let mut batch_results = Vec::new();

        // Group transactions by predicted conflicts
        let mut execution_batches = self.create_execution_batches(&reexecution_order, scheduler);

        // Execute batches sequentially, but transactions within batches in parallel
        for batch in execution_batches {
            let batch_version = current_version;

            // Execute transactions in the batch in parallel
            let results: Vec<_> = batch
                .into_par_iter()
                .map(|tx_idx| {
                    let mut tx_evm = evm.clone();
                    self.reexecute_transaction(tx_idx, &mut tx_evm, batch_version)
                })
                .collect::<Result<Vec<_>, _>>()?;

            // Update scheduler with new execution data
            for (tx_idx, result) in results.iter().enumerate() {
                scheduler.tx_costs.insert(tx_idx, result.gas_used);

                // Update access patterns if we have operation logs
                if let Some(log) = self.operation_logs.get(tx_idx) {
                    if let Ok(op_log) = log.inner.read() {
                        for (addr, slot) in &op_log.accessed_slots {
                            scheduler.update_storage_pattern(*addr, *slot, tx_idx, result.gas_used);
                        }
                        for addr in &op_log.accessed_accounts {
                            scheduler.update_account_pattern(*addr, tx_idx, result.gas_used);
                        }
                    }
                }
            }

            batch_results.extend(results);
            current_version = self.versioned_db.write().create_snapshot();
        }

        // Update final results
        for (idx, result) in batch_results.into_iter().enumerate() {
            final_results[idx] = result.result;
        }

        Ok(())
    }

    fn create_execution_batches(
        &self,
        reexecution_order: &[usize],
        scheduler: &PredictiveScheduler,
    ) -> Vec<Vec<usize>> {
        let mut batches = Vec::new();
        let mut current_batch = Vec::new();
        let mut current_conflicts = HashSet::new();

        for &tx_idx in reexecution_order {
            let predicted_conflicts = scheduler.predict_conflicts(tx_idx);

            // If this transaction conflicts with current batch, start a new batch
            if !current_conflicts.is_disjoint(&predicted_conflicts) {
                if !current_batch.is_empty() {
                    batches.push(current_batch);
                    current_batch = Vec::new();
                    current_conflicts.clear();
                }
            }

            current_batch.push(tx_idx);
            current_conflicts.extend(predicted_conflicts);
        }

        // Add final batch
        if !current_batch.is_empty() {
            batches.push(current_batch);
        }

        batches
    }

    fn execute_batch_adaptive<CTX, INSP, I, P>(
        &mut self,
        transactions: Vec<TransactionPriority>,
        evm: &mut Evm<CTX, INSP, I, P>,
        scheduler: &mut PredictiveScheduler,
    ) -> Result<Vec<EnhancedExecutionResult>, <DB as database_interface::Database>::Error>
    where
        CTX: ContextTr + Host + Clone + Send + Sync + 'static,
        INSP: Clone + Send + Sync + 'static,
        I: Clone + Send + Sync + 'static,
        P: Clone + Send + Sync + 'static,
    {
        let start_time = Instant::now();
        let batch_size = self.calculate_optimal_batch_size();

        // Sort transactions by priority
        let mut prioritized_txs = transactions;
        prioritized_txs.sort_by(|a, b| b.priority_score.partial_cmp(&a.priority_score).unwrap());

        // Execute in batches
        let mut results = Vec::new();
        for chunk in prioritized_txs.chunks(batch_size) {
            let batch_start = Instant::now();

            // Execute batch
            let batch_results = self.execute_transaction_batch(chunk, evm, scheduler)?;

            // Update metrics
            let batch_duration = batch_start.elapsed();
            self.metrics
                .batch_execution_times
                .record(batch_duration.as_secs_f64());

            // Calculate conflict rate
            let conflicts = batch_results
                .iter()
                .filter(|r| r.status == ExecutionStatus::Conflicted)
                .count();
            let conflict_rate = conflicts as f64 / chunk.len() as f64;
            self.metrics.conflict_rate.set(conflict_rate);

            // Adjust batch size based on performance
            self.adjust_batch_size(batch_duration, conflict_rate);

            results.extend(batch_results);
        }

        // Update final metrics
        self.update_execution_metrics(&results);

        Ok(results)
    }

    fn calculate_optimal_batch_size(&self) -> usize {
        let current_conflict_rate = self.metrics.conflict_rate.get();
        let avg_execution_time = self.metrics.batch_execution_times.mean();

        let mut optimal_size = if current_conflict_rate > self.batch_config.conflict_threshold {
            // Reduce batch size when conflict rate is high
            self.batch_config.min_size
        } else if avg_execution_time < self.batch_config.target_execution_time.as_secs_f64() {
            // Increase batch size when execution is fast
            self.batch_config.max_size
        } else {
            // Adjust based on current performance
            let current_size = self.metrics.avg_batch_size.get() as usize;
            let time_factor =
                self.batch_config.target_execution_time.as_secs_f64() / avg_execution_time;
            (current_size as f64 * time_factor) as usize
        };

        // Clamp to configured limits
        optimal_size = optimal_size.clamp(self.batch_config.min_size, self.batch_config.max_size);

        optimal_size
    }

    fn adjust_batch_size(&mut self, duration: Duration, conflict_rate: f64) {
        let target_duration = self.batch_config.target_execution_time;
        let current_size = self.metrics.avg_batch_size.get();

        let mut new_size = if conflict_rate > self.batch_config.conflict_threshold {
            // Reduce size when conflict rate is high
            current_size * 0.8
        } else if duration > target_duration {
            // Reduce size when execution is slow
            current_size * 0.9
        } else if duration < target_duration / 2.0 {
            // Increase size when execution is very fast
            current_size * 1.2
        } else {
            // Maintain current size
            current_size
        };

        // Clamp to configured limits
        new_size = new_size.clamp(
            self.batch_config.min_size as f64,
            self.batch_config.max_size as f64,
        );

        self.metrics.avg_batch_size.set(new_size);
    }

    fn update_execution_metrics(&mut self, results: &[EnhancedExecutionResult]) {
        for result in results {
            match result.status {
                ExecutionStatus::Success => self.metrics.successful_txs.increment(1),
                ExecutionStatus::Failed => self.metrics.failed_txs.increment(1),
                ExecutionStatus::Conflicted | ExecutionStatus::NeedsReexecution => {
                    self.metrics.reexecuted_txs.increment(1)
                }
            }
            self.metrics
                .total_gas_used
                .increment(result.gas_used as u64);
        }
    }

    fn execute_transaction_batch<CTX, INSP, I, P>(
        &mut self,
        transactions: &[TransactionPriority],
        evm: &mut Evm<CTX, INSP, I, P>,
        scheduler: &mut PredictiveScheduler,
    ) -> Result<Vec<EnhancedExecutionResult>, <DB as database_interface::Database>::Error>
    where
        CTX: ContextTr + Host + Clone + Send + Sync + 'static,
        INSP: Clone + Send + Sync + 'static,
        I: Clone + Send + Sync + 'static,
        P: Clone + Send + Sync + 'static,
    {
        let batch_version = self.versioned_db.write().create_snapshot();

        let results: Vec<_> = transactions
            .par_iter()
            .map(|tx| {
                let mut tx_evm = evm.clone();
                self.reexecute_transaction(tx.tx_index, &mut tx_evm, batch_version)
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(results)
    }

    fn update_performance_history(&mut self, batch_metrics: PerformanceWindow) {
        self.performance_history.add_window(batch_metrics);
        self.thread_pool.adjust_size(&self.performance_history);
    }

    fn execute_batch_with_metrics<CTX, INSP, I, P>(
        &mut self,
        transactions: Vec<TransactionPriority>,
        evm: &mut Evm<CTX, INSP, I, P>,
        scheduler: &mut PredictiveScheduler,
    ) -> Result<Vec<EnhancedExecutionResult>, <DB as database_interface::Database>::Error>
    where
        CTX: ContextTr + Host + Clone + Send + Sync + 'static,
        INSP: Clone + Send + Sync + 'static,
        I: Clone + Send + Sync + 'static,
        P: Clone + Send + Sync + 'static,
    {
        let start_time = Instant::now();
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let results = self.execute_batch_adaptive(transactions, evm, scheduler)?;

        // Collect metrics for this batch
        let execution_time = start_time.elapsed();
        let conflict_count = results
            .iter()
            .filter(|r| r.status == ExecutionStatus::Conflicted)
            .count();
        let conflict_rate = conflict_count as f64 / results.len() as f64;
        let gas_used: u64 = results.iter().map(|r| r.gas_used).sum();

        // Update performance history
        self.update_performance_history(PerformanceWindow {
            timestamp,
            execution_time,
            conflict_rate,
            gas_used,
            transactions_processed: results.len(),
        });

        Ok(results)
    }
}
