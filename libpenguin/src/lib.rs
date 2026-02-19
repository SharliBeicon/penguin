//! Penguin is a toy Payments Engine that processes transactions for multiple clients.
//! It ingest transactions from any iterator and returns a list of client states.
//!
//! ## Overview
//!
//! The core entry point is [`PenguinBuilder`]. Build a [`Penguin`] instance with an
//! iterator that yields `Result<Transaction, E>` items, configure it, and await `run()`.
//! If you want to stream worker outputs as they finish, use [`Penguin::get_stream()`].
//!
//! ## Usage example
//!
//! ```rust,ignore
//! use libpenguin::prelude::*;
//! use tokio_stream::StreamExt;
//! use std::str::FromStr;
//!
//! let inputs = [
//!     "deposit, 1, 1, 1.0",
//!     "deposit, 2, 2, 2.0",
//!     "deposit, 1, 3, 2.0",
//!     "withdrawal, 1, 4, 1.5",
//!     "withdrawal, 2, 5, 3.0",
//!     "deposit, 1, 5,",
//! ];
//!
//! let reader = inputs.into_iter().map(|line| {
//!     Ok::<Transaction, PenguinError>(line.parse::<Transaction>().expect("valid transaction"))
//! });
//!
//! let mut penguin = PenguinBuilder::from_reader(reader)
//!     .with_num_workers(std::thread::available_parallelism().unwrap())
//!     .with_logger("penguin.log")
//!     .build()?;
//!
//! // Collect all results at once.
//! let _output = penguin.run().await?;
//!
//! // Or stream worker outputs as they finish.
//! let mut stream = penguin.process().await?;
//! while let Some(states) = stream.next().await {
//!     for state in states {
//!         println!("{}", state.client);
//!     }
//! }
//! ```
//!
//! ## Logging
//!
//! If you want background logs while piping stdout, set a log file with
//! [`PenguinBuilder::with_logger`]. Logs use `tracing` and respect `RUST_LOG`.
//!
//! ## Error handling
//!
//! `PenguinError` captures I/O, parsing, and transaction errors. Invalid business
//! operations (like disputes of unknown transactions) are ignored and logged.
mod logger;
mod penguin;
mod types;

pub mod prelude {
    pub use super::{
        penguin::{Penguin, PenguinBuilder},
        types::{ClientState, PenguinError, Transaction, TransactionType},
    };
}
