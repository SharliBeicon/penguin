use serde::Deserialize;
use std::io;
use thiserror::Error;

#[derive(Debug, Deserialize)]
pub struct Transaction {
    #[serde(rename = "type")]
    tx_type: TransactionType,
    client: u16,
    tx: u32,
    amount: Option<f64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TransactionType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
}

#[derive(Error, Debug)]
pub enum PenguinError {
    #[error("I/O error: {0}")]
    IO(#[from] io::Error),
}

#[cfg(test)]
mod tests {}
