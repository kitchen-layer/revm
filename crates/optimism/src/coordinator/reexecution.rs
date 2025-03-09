use super::conflict_detector::ConflictAnalysis;
use super::operation_logs::OperationLog;

pub struct ReexecutionContext {
    pub tx_idx: usize,
    pub conflicts: ConflictAnalysis,
    pub original_operations: Vec<Operation>,
    pub modified_addresses: HashSet<Address>,
}

impl<DB: database_interface::Database> PartialReexecutor<DB> {
    pub fn prepare_reexecution_context(
        &self,
        tx_idx: usize,
        conflicts: &ConflictAnalysis,
        operation_log: &OperationLog,
    ) -> ReexecutionContext {
        let modified_addresses = conflicts
            .iter()
            .map(|conflict| conflict.address)
            .collect::<HashSet<_>>();

        ReexecutionContext {
            tx_idx,
            conflicts: conflicts.clone(),
            original_operations: operation_log.operations().to_vec(),
            modified_addresses,
        }
    }

    pub fn reexecute<CTX, INSP, I, P>(
        &self,
        tx_idx: usize,
        evm: &mut Evm<CTX, INSP, I, P>,
        context: &ReexecutionContext,
    ) -> Result<ExecutionResult, DB::Error>
    where
        CTX: ContextTr + Host + Clone + Send + Sync + 'static,
        INSP: Clone + Send + Sync + 'static,
        I: Clone + Send + Sync + 'static,
        P: Clone + Send + Sync + 'static,
    {
        // Create a new versioned snapshot for re-execution
        let version = self.versioned_db.write().create_snapshot();

        // Apply conflict resolutions
        for conflict in &context.conflicts {
            self.resolve_conflict(conflict, version)?;
        }

        // Re-execute the transaction
        let execution_result = evm.transact();

        match execution_result {
            Ok(res) => {
                // Verify that re-execution resolved all conflicts
                if self.verify_conflict_resolution(&context.modified_addresses, version)? {
                    Ok(ExecutionResult {
                        result: res.result,
                        gas_used: res.gas_used,
                        execution_time: Duration::default(), // Will be set by caller
                        conflicts_resolved: context.conflicts.len(),
                        operations_reexecuted: context.original_operations.len(),
                    })
                } else {
                    // If verification fails, revert and return error
                    self.versioned_db.write().revert_version(version);
                    Err(DB::Error::Custom(
                        "Conflict resolution verification failed".into(),
                    ))
                }
            }
            Err(e) => {
                self.versioned_db.write().revert_version(version);
                Err(e)
            }
        }
    }

    fn resolve_conflict(&self, conflict: &Conflict, version: u64) -> Result<(), DB::Error> {
        // Apply the necessary state changes to resolve the conflict
        let mut db = self.versioned_db.write();

        match conflict.conflict_type {
            ConflictType::Read => {
                // Ensure we have the latest state for read conflicts
                db.ensure_latest_state(&conflict.address, version)?;
            }
            ConflictType::Write => {
                // For write conflicts, we need to merge the states
                db.merge_states(&conflict.address, conflict.from_version, version)?;
            }
            ConflictType::Account => {
                // Handle account-level conflicts
                db.sync_account_state(&conflict.address, version)?;
            }
        }

        Ok(())
    }

    fn verify_conflict_resolution(
        &self,
        modified_addresses: &HashSet<Address>,
        version: u64,
    ) -> Result<bool, DB::Error> {
        let db = self.versioned_db.read();

        // Verify that all modified addresses have consistent state
        for address in modified_addresses {
            if !db.verify_state_consistency(address, version)? {
                return Ok(false);
            }
        }

        Ok(true)
    }
}
