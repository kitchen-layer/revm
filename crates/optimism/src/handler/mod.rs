pub mod parallel;
pub mod precompiles;

use crate::transaction::OpTxTr;
use crate::L1BlockInfo;
use context::Context;
use primitives::Bytes;
use revm_interpreter::InterpreterResult;

pub trait OpHandler {
    fn execute_transaction(
        &mut self,
        transaction: &OpTxTr,
        l1_block_info: &L1BlockInfo,
        context: &mut Context,
    ) -> InterpreterResult;
}

// Re-export important types
pub use parallel::ParallelExecutionHandler;
pub use precompiles::OpPrecompileProvider;
