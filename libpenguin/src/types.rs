use serde::{Deserialize, Serialize};
use std::io;
use thiserror::Error;
use tokio::sync::mpsc::error::SendError;

pub(crate) type TxResult<E> = Result<Transaction, E>;

#[derive(Debug, Deserialize)]
pub struct Transaction {
    #[serde(rename = "type")]
    pub tx_type: TransactionType,
    pub client: u16,
    pub tx: u32,
    pub amount: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct ClientState {
    pub client: u16,
    pub available: f64,
    pub held: f64,
    pub total: f64,
    pub locked: bool,
}

impl ClientState {
    pub fn new(client: u16) -> Self {
        Self {
            client,
            available: 0.0,
            held: 0.0,
            total: 0.0,
            locked: false,
        }
    }
}

pub(crate) type ClientTx = (u16, u32); // (client_id, transaction_id)

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TransactionType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

#[derive(Error, Debug)]
pub enum PenguinError {
    #[error("I/O error: {0}")]
    IO(#[from] io::Error),
    #[error("Error while parsing on line {0}")]
    Parse(usize),
    #[error("Error sending transaction to the channel: {0}")]
    ChannelSend(#[from] SendError<Transaction>),
    #[error("Client {0} received a deposit/withdrawal transaction with no amount associated.")]
    DepositOrWithdrawalWithoutAmount(u16),
}
