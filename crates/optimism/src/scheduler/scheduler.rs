use ethers::types::{Address, U256};
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashSet};

pub struct Scheduler {
    transactions: Vec<TransactionData>,
}

pub struct TransactionData {
    read_set: HashSet<(Address, U256)>,
    write_set: HashSet<(Address, U256)>,
    original_index: usize,
}

pub struct ConflictGraph {
    adjacency: Vec<Vec<usize>>,
}

impl Scheduler {
    pub fn new() -> Self {
        Scheduler {
            transactions: Vec::new(),
        }
    }

    // Adds a transaction with its read/write sets and original index
    pub fn add_transaction(
        &mut self,
        read_set: HashSet<(Address, U256)>,
        write_set: HashSet<(Address, U256)>,
        original_index: usize,
    ) {
        self.transactions.push(TransactionData {
            read_set,
            write_set,
            original_index,
        });
    }

    // Builds a conflict graph based on read/write overlaps
    pub fn build_conflict_graph(&self) -> ConflictGraph {
        let n = self.transactions.len();
        let mut adjacency = vec![vec![]; n];

        for i in 0..n {
            for j in (i + 1)..n {
                let ti = &self.transactions[i];
                let tj = &self.transactions[j];

                let wr_conflict = !ti.write_set.is_disjoint(&tj.read_set);
                let rw_conflict = !ti.read_set.is_disjoint(&tj.write_set);
                let ww_conflict = !ti.write_set.is_disjoint(&tj.write_set);

                // Handle WR and WW: Ti must come before Tj
                if wr_conflict || ww_conflict {
                    adjacency[i].push(j);
                }
                // Handle RW: Tj must come before Ti
                if rw_conflict {
                    adjacency[j].push(i);
                }
            }
        }

        ConflictGraph { adjacency }
    }

    // Determines optimal execution order using topological sort
    pub fn schedule(&self) -> Vec<usize> {
        let graph = self.build_conflict_graph();
        let n = self.transactions.len();
        let mut in_degree = vec![0; n];

        // Calculate in-degrees
        for edges in &graph.adjacency {
            for &node in edges {
                in_degree[node] += 1;
            }
        }

        // Use min-heap to prioritize smallest index when choosing nodes
        let mut heap = BinaryHeap::new();
        for i in 0..n {
            if in_degree[i] == 0 {
                heap.push(Reverse(i));
            }
        }

        let mut order = Vec::with_capacity(n);
        while let Some(Reverse(node)) = heap.pop() {
            order.push(node);

            // Decrement in-degree of neighbors
            for &neighbor in &graph.adjacency[node] {
                in_degree[neighbor] -= 1;
                if in_degree[neighbor] == 0 {
                    heap.push(Reverse(neighbor));
                }
            }
        }

        // Handle cycles by appending remaining nodes in original order
        if order.len() < n {
            let mut remaining: Vec<usize> = (0..n).filter(|i| !order.contains(i)).collect();
            remaining.sort_unstable(); // Fallback to original order
            order.extend(remaining);
        }

        order
    }
}
