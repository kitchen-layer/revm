use super::*;
use crate::tests::parallel_execution::*;
use rand::{thread_rng, Rng};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};

#[test]
fn test_concurrent_state_modifications() {
    let mut context = TestContext::new();
    let shared_address = Address::random();
    let num_slots = 5;
    let num_txs = 10;

    // Create transactions that modify the same contract but different slots
    for i in 0..num_txs {
        let mut tx = MockTransaction::new(i, U256::from(1 + i), 21000);
        let slot = U256::from(i % num_slots);

        tx.add_operation(Operation::Read {
            address: shared_address,
            slot,
        });
        tx.add_operation(Operation::Write {
            address: shared_address,
            slot,
            value: U256::from(100 + i),
            original_value: U256::zero(),
        });
        context.add_transaction(tx);
    }

    context.setup_coordinator();
    let mut evm = create_test_evm();

    let results = context
        .coordinator
        .as_mut()
        .unwrap()
        .execute_transactions(
            context
                .transactions
                .iter()
                .map(|tx| (tx.index, tx.gas_price, tx.estimated_gas))
                .collect(),
            &mut evm,
        )
        .unwrap();

    assert_eq!(results.len(), num_txs);

    // Verify that transactions modifying the same slot had conflicts
    let mut conflicts_per_slot = BTreeMap::new();
    for (i, result) in results.iter().enumerate() {
        let slot = i % num_slots;
        *conflicts_per_slot.entry(slot).or_insert(0) += result.conflicts_resolved;
    }

    assert!(conflicts_per_slot.values().any(|&conflicts| conflicts > 0));
}

#[test]
fn test_cascading_conflicts() {
    let mut context = TestContext::new();
    let address = Address::random();
    let num_txs = 5;

    // Create chain of dependent transactions
    for i in 0..num_txs {
        let mut tx = MockTransaction::new(i, U256::from(1), 21000);
        tx.add_operation(Operation::Read {
            address,
            slot: U256::from(i),
        });
        tx.add_operation(Operation::Write {
            address,
            slot: U256::from(i + 1),
            value: U256::from(100),
            original_value: U256::zero(),
        });
        context.add_transaction(tx);
    }

    context.setup_coordinator();
    let mut evm = create_test_evm();

    let results = context
        .coordinator
        .as_mut()
        .unwrap()
        .execute_transactions(
            context
                .transactions
                .iter()
                .map(|tx| (tx.index, tx.gas_price, tx.estimated_gas))
                .collect(),
            &mut evm,
        )
        .unwrap();

    // Verify cascading conflict resolution
    let mut total_reexecutions = 0;
    for result in &results {
        total_reexecutions += result.operations_reexecuted;
    }
    assert!(total_reexecutions > 0);
}

#[test]
fn test_priority_inversion() {
    let mut context = TestContext::new();
    let address = Address::random();

    // Create high-priority transaction
    let mut high_priority = MockTransaction::new(0, U256::from(1000), 21000);
    high_priority.add_operation(Operation::Read {
        address,
        slot: U256::from(1),
    });

    // Create several low-priority transactions that could block the high-priority one
    for i in 1..5 {
        let mut tx = MockTransaction::new(i, U256::from(1), 21000);
        tx.add_operation(Operation::Write {
            address,
            slot: U256::from(1),
            value: U256::from(i),
            original_value: U256::zero(),
        });
        context.add_transaction(tx);
    }

    context.add_transaction(high_priority);
    context.setup_coordinator();
    let mut evm = create_test_evm();

    let results = context
        .coordinator
        .as_mut()
        .unwrap()
        .execute_transactions(
            context
                .transactions
                .iter()
                .map(|tx| (tx.index, tx.gas_price, tx.estimated_gas))
                .collect(),
            &mut evm,
        )
        .unwrap();

    // Verify high-priority transaction was not severely delayed
    let high_priority_result = &results[results.len() - 1];
    assert!(high_priority_result.execution_time < Duration::from_secs(1));
}

#[test]
fn test_memory_access_patterns() {
    let mut context = TestContext::new();
    let num_txs = 20;
    let mut rng = thread_rng();

    // Create transactions with various memory access patterns
    for i in 0..num_txs {
        let mut tx = MockTransaction::new(i, U256::from(1), 21000);

        // Simulate different memory access patterns
        match i % 4 {
            0 => {
                // Sequential access
                for j in 0..5 {
                    tx.add_operation(Operation::Read {
                        address: Address::random(),
                        slot: U256::from(j),
                    });
                }
            }
            1 => {
                // Random access
                for _ in 0..5 {
                    tx.add_operation(Operation::Read {
                        address: Address::random(),
                        slot: U256::from(rng.gen::<u64>()),
                    });
                }
            }
            2 => {
                // Concentrated access
                let address = Address::random();
                let slot = U256::from(rng.gen::<u64>());
                for _ in 0..5 {
                    tx.add_operation(Operation::Read { address, slot });
                }
            }
            3 => {
                // Mixed access
                let address = Address::random();
                for j in 0..5 {
                    if j % 2 == 0 {
                        tx.add_operation(Operation::Read {
                            address,
                            slot: U256::from(j),
                        });
                    } else {
                        tx.add_operation(Operation::Write {
                            address,
                            slot: U256::from(j),
                            value: U256::from(100),
                            original_value: U256::zero(),
                        });
                    }
                }
            }
            _ => unreachable!(),
        }
        context.add_transaction(tx);
    }

    context.setup_coordinator();
    let mut evm = create_test_evm();

    let results = context
        .coordinator
        .as_mut()
        .unwrap()
        .execute_transactions(
            context
                .transactions
                .iter()
                .map(|tx| (tx.index, tx.gas_price, tx.estimated_gas))
                .collect(),
            &mut evm,
        )
        .unwrap();

    assert_eq!(results.len(), num_txs);
}

#[test]
fn test_reexecution_checkpoints() {
    let mut context = TestContext::new();
    let address = Address::random();

    // Create transaction with multiple operations and potential checkpoint locations
    let mut tx = MockTransaction::new(0, U256::from(1), 21000);

    // Add operations with increasing complexity
    for i in 0..10 {
        tx.add_operation(Operation::Read {
            address,
            slot: U256::from(i),
        });

        if i % 3 == 0 {
            // Add a write operation as a potential checkpoint
            tx.add_operation(Operation::Write {
                address,
                slot: U256::from(i),
                value: U256::from(100),
                original_value: U256::zero(),
            });
        }
    }

    context.add_transaction(tx);
    context.setup_coordinator();
    let mut evm = create_test_evm();

    let results = context
        .coordinator
        .as_mut()
        .unwrap()
        .execute_transactions(
            context
                .transactions
                .iter()
                .map(|tx| (tx.index, tx.gas_price, tx.estimated_gas))
                .collect(),
            &mut evm,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert!(results[0].operations_reexecuted > 0);
}

#[test]
fn test_concurrent_contract_creation() {
    let mut context = TestContext::new();
    let num_txs = 5;
    let contract_code = vec![0u8; 100]; // Dummy contract code

    // Create transactions that deploy contracts
    for i in 0..num_txs {
        let mut tx = MockTransaction::new(i, U256::from(1), 21000);

        // Simulate contract creation
        tx.add_operation(Operation::AccountAccess {
            address: Address::random(),
        });
        tx.add_operation(Operation::CodeAccess {
            address: Address::random(),
        });

        context.add_transaction(tx);
    }

    context.setup_coordinator();
    let mut evm = create_test_evm();

    let results = context
        .coordinator
        .as_mut()
        .unwrap()
        .execute_transactions(
            context
                .transactions
                .iter()
                .map(|tx| (tx.index, tx.gas_price, tx.estimated_gas))
                .collect(),
            &mut evm,
        )
        .unwrap();

    assert_eq!(results.len(), num_txs);
}
