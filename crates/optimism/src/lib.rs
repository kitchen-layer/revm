//! Optimism-specific constants, types, and helpers.
#![cfg_attr(not(test), warn(unused_crate_dependencies))]
#![cfg_attr(not(feature = "std"), no_std)]

#[cfg(not(feature = "std"))]
extern crate alloc as std;

pub mod api;
mod db;
pub mod evm;
mod handler;
pub mod parallel;
pub mod transaction;

// Re-export types from modules
pub use db::*;
pub use evm::OpEvm;
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
pub use parallel::precompiles::OpPrecompileProvider;
pub use result::OpHaltReason;
pub use spec::*;
pub use transaction::{error::OpTransactionError, estimate_tx_compressed_size, OpTransaction};

pub use handler::OpHandler;
