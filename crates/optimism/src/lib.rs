//! Optimism-specific constants, types, and helpers.
#![cfg_attr(not(test), warn(unused_crate_dependencies))]
#![cfg_attr(not(feature = "std"), no_std)]

#[cfg(not(feature = "std"))]
extern crate alloc as std;

mod api;
mod evm;
mod handler;
mod transaction;
mod parallel;
mod db;
mod coordinator;
mod scheduler;

// Re-export types from modules
pub use evm::OpEvm;
pub use handler::{
    parallel::ParallelExecutionHandler, precompiles::OpPrecompileProvider, OpHandler,
};
pub use db::*;
pub use coordinator::*;
pub use scheduler::*;
pub use transaction::OpTxTr;

pub mod bn128;
pub mod constants;
pub mod fast_lz;
pub mod l1block;
pub mod result;
pub mod spec;

pub use api::{
    builder::{OpBuilder, OpContext},
    default_ctx::DefaultOp,
};

pub use l1block::L1BlockInfo;
pub use result::OpHaltReason;
pub use spec::*;
pub use transaction::{error::OpTransactionError, estimate_tx_compressed_size, OpTransaction};
