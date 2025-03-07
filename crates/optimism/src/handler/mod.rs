pub mod parallel;
pub mod precompiles;
pub mod handler; 

use crate::transaction::OpTxTr;
use crate::L1BlockInfo;
use revm::interpreter::InterpreterResult;
use revm::context::Context;

pub trait OpHandler {
    fn execute_transaction<T: OpTxTr>(
        &mut self,
        transaction: &T,
        l1_block_info: &L1BlockInfo,
        context: &mut Context,
    ) -> InterpreterResult;
}
