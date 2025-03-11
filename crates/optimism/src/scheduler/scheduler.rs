// use crate::parallel::conflict_detector::{ConflictDetector, ConflictType};
// use crate::parallel::operation_logs::OperationLog;
// use core::cmp::Ordering;
// use revm::primitives::{Address, U256};
// use std::cmp::Reverse;
// use std::collections::{BinaryHeap, HashMap, HashSet};

// pub struct Scheduler {
//     transactions: Vec<TransactionData>,
// }

// pub struct TransactionData {
//     read_set: HashSet<(Address, U256)>,
//     write_set: HashSet<(Address, U256)>,
//     original_index: usize,
// }

// pub struct ConflictGraph {
//     adjacency: Vec<Vec<usize>>,
// }

// #[derive(Debug, Clone)]
// pub struct TransactionSchedulingInfo {
//     tx_index: usize,
//     gas_price: U256,
//     estimated_gas: u64,
//     dependencies: HashSet<usize>,
//     priority_score: f64,
//     waiting_since: std::time::Instant,
// }

// impl PartialEq for TransactionSchedulingInfo {
//     fn eq(&self, other: &Self) -> bool {
//         self.priority_score.eq(&other.priority_score)
//     }
// }

// impl Eq for TransactionSchedulingInfo {}

// impl PartialOrd for TransactionSchedulingInfo {
//     fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
//         self.priority_score.partial_cmp(&other.priority_score)
//     }
// }

// impl Ord for TransactionSchedulingInfo {
//     fn cmp(&self, other: &Self) -> Ordering {
//         self.partial_cmp(other).unwrap_or(Ordering::Equal)
//     }
// }

// pub struct TransactionScheduler {
//     conflict_detector: ConflictDetector,
//     pending_transactions: BinaryHeap<TransactionSchedulingInfo>,
//     scheduled_transactions: HashSet<usize>,
//     access_history: HashMap<(Address, U256), Vec<usize>>,
//     tx_dependencies: HashMap<usize, HashSet<usize>>,
// }

// impl Scheduler {
//     pub fn new() -> Self {
//         Scheduler {
//             transactions: Vec::new(),
//         }
//     }

//     // Adds a transaction with its read/write sets and original index
//     pub fn add_transaction(
//         &mut self,
//         read_set: HashSet<(Address, U256)>,
//         write_set: HashSet<(Address, U256)>,
//         original_index: usize,
//     ) {
//         self.transactions.push(TransactionData {
//             read_set,
//             write_set,
//             original_index,
//         });
//     }

//     // Builds a conflict graph based on read/write overlaps
//     pub fn build_conflict_graph(&self) -> ConflictGraph {
//         let n = self.transactions.len();
//         let mut adjacency = vec![vec![]; n];

//         for i in 0..n {
//             for j in (i + 1)..n {
//                 let ti = &self.transactions[i];
//                 let tj = &self.transactions[j];

//                 let wr_conflict = !ti.write_set.is_disjoint(&tj.read_set);
//                 let rw_conflict = !ti.read_set.is_disjoint(&tj.write_set);
//                 let ww_conflict = !ti.write_set.is_disjoint(&tj.write_set);

//                 // Handle WR and WW: Ti must come before Tj
//                 if wr_conflict || ww_conflict {
//                     adjacency[i].push(j);
//                 }
//                 // Handle RW: Tj must come before Ti
//                 if rw_conflict {
//                     adjacency[j].push(i);
//                 }
//             }
//         }

//         ConflictGraph { adjacency }
//     }

//     // Determines optimal execution order using topological sort
//     pub fn schedule(&self) -> Vec<usize> {
//         let graph = self.build_conflict_graph();
//         let n = self.transactions.len();
//         let mut in_degree = vec![0; n];

//         // Calculate in-degrees
//         for edges in &graph.adjacency {
//             for &node in edges {
//                 in_degree[node] += 1;
//             }
//         }

//         // Use min-heap to prioritize smallest index when choosing nodes
//         let mut heap = BinaryHeap::new();
//         for i in 0..n {
//             if in_degree[i] == 0 {
//                 heap.push(Reverse(i));
//             }
//         }

//         let mut order = Vec::with_capacity(n);
//         while let Some(Reverse(node)) = heap.pop() {
//             order.push(node);

//             // Decrement in-degree of neighbors
//             for &neighbor in &graph.adjacency[node] {
//                 in_degree[neighbor] -= 1;
//                 if in_degree[neighbor] == 0 {
//                     heap.push(Reverse(neighbor));
//                 }
//             }
//         }

//         // Handle cycles by appending remaining nodes in original order
//         if order.len() < n {
//             let mut remaining: Vec<usize> = (0..n).filter(|i| !order.contains(i)).collect();
//             remaining.sort_unstable(); // Fallback to original order
//             order.extend(remaining);
//         }

//         order
//     }
// }

// impl TransactionScheduler {
//     pub fn new() -> Self {
//         Self {
//             conflict_detector: ConflictDetector::new(),
//             pending_transactions: BinaryHeap::new(),
//             scheduled_transactions: HashSet::new(),
//             access_history: HashMap::new(),
//             tx_dependencies: HashMap::new(),
//         }
//     }

//     pub fn add_transaction(
//         &mut self,
//         tx_index: usize,
//         gas_price: U256,
//         estimated_gas: u64,
//         operation_log: &OperationLog,
//     ) {
//         // Analyze operation log for conflicts
//         self.conflict_detector.analyze_operation_log(operation_log);

//         // Detect conflicts and build dependencies
//         let conflicts = self.conflict_detector.detect_conflicts(tx_index);
//         let mut dependencies = HashSet::new();

//         for conflict in conflicts {
//             match conflict {
//                 ConflictType::TransactionLevel {
//                     tx1_index,
//                     tx2_index,
//                     ..
//                 } => {
//                     dependencies.insert(tx1_index.min(tx2_index));
//                 }
//                 ConflictType::OperationLevel {
//                     tx_index: dep_tx, ..
//                 } => {
//                     dependencies.insert(dep_tx);
//                 }
//             }
//         }

//         // Calculate initial priority score
//         let priority_score = self.calculate_priority_score(gas_price, estimated_gas, &dependencies);

//         // Create scheduling info
//         let scheduling_info = TransactionSchedulingInfo {
//             tx_index,
//             gas_price,
//             estimated_gas,
//             dependencies: dependencies.clone(),
//             priority_score,
//             waiting_since: std::time::Instant::now(),
//         };

//         self.pending_transactions.push(scheduling_info);
//         self.update_dependencies(tx_index, &dependencies);
//     }

//     fn calculate_priority_score(
//         &self,
//         gas_price: U256,
//         estimated_gas: u64,
//         dependencies: &HashSet<usize>,
//     ) -> f64 {
//         let gas_price_factor = gas_price.try_into().unwrap_or(0) as f64;
//         let dependency_penalty = 1.0 / (1.0 + dependencies.len() as f64);
//         let gas_efficiency = 1.0 / (1.0 + estimated_gas as f64);

//         gas_price_factor * dependency_penalty * gas_efficiency
//     }

//     fn update_dependencies(&mut self, tx_index: usize, dependencies: &HashSet<usize>) {
//         self.tx_dependencies.insert(tx_index, dependencies.clone());

//         // Update reverse dependencies
//         for &dep in dependencies {
//             if let Some(deps) = self.tx_dependencies.get_mut(&dep) {
//                 deps.insert(tx_index);
//             }
//         }
//     }

//     pub fn get_next_batch(&mut self, max_batch_size: usize) -> Vec<usize> {
//         let mut batch = Vec::new();
//         let mut remaining = max_batch_size;

//         while remaining > 0 {
//             if let Some(tx) = self.get_next_executable_transaction() {
//                 batch.push(tx.tx_index);
//                 self.scheduled_transactions.insert(tx.tx_index);
//                 remaining -= 1;
//             } else {
//                 break;
//             }
//         }

//         batch
//     }

//     fn get_next_executable_transaction(&mut self) -> Option<TransactionSchedulingInfo> {
//         let mut best_candidate = None;
//         let mut temp_storage = Vec::new();

//         while let Some(tx) = self.pending_transactions.pop() {
//             if self.is_executable(&tx) {
//                 best_candidate = Some(tx);
//                 break;
//             }
//             temp_storage.push(tx);
//         }

//         // Return unschedulable transactions to the heap
//         for tx in temp_storage {
//             self.pending_transactions.push(tx);
//         }

//         best_candidate
//     }

//     fn is_executable(&self, tx: &TransactionSchedulingInfo) -> bool {
//         tx.dependencies
//             .iter()
//             .all(|dep| self.scheduled_transactions.contains(dep))
//     }

//     pub fn update_priorities(&mut self) {
//         let mut updated_heap = BinaryHeap::new();

//         while let Some(mut tx) = self.pending_transactions.pop() {
//             // Update priority based on waiting time
//             let waiting_time = tx.waiting_since.elapsed().as_secs_f64();
//             let waiting_factor = 1.0 + (waiting_time / 60.0); // Increase priority by 100% per minute of waiting

//             tx.priority_score *= waiting_factor;
//             updated_heap.push(tx);
//         }

//         self.pending_transactions = updated_heap;
//     }
// }
