use serde::{Deserialize, Serialize};
use std::{borrow::Cow, io, str::FromStr};
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

impl FromStr for Transaction {
    type Err = PenguinError;

    fn from_str(line: &str) -> Result<Self, Self::Err> {
        let mut parts = line.split(',').map(|part| part.trim());
        let tx_type = match parts
            .next()
            .ok_or_else(|| PenguinError::TransactionParse(Cow::Borrowed("type is required")))?
        {
            "deposit" => TransactionType::Deposit,
            "withdrawal" => TransactionType::Withdrawal,
            "dispute" => TransactionType::Dispute,
            "resolve" => TransactionType::Resolve,
            "chargeback" => TransactionType::Chargeback,
            other => {
                return Err(PenguinError::TransactionParse(Cow::Owned(format!(
                    "unexpected type: {other}"
                ))));
            }
        };
        let client = parts
            .next()
            .ok_or_else(|| PenguinError::TransactionParse(Cow::Borrowed("client is required")))?
            .parse()
            .map_err(|_| PenguinError::TransactionParse(Cow::Borrowed("client must be a u16")))?;
        let tx = parts
            .next()
            .ok_or_else(|| PenguinError::TransactionParse(Cow::Borrowed("tx is required")))?
            .parse()
            .map_err(|_| PenguinError::TransactionParse(Cow::Borrowed("tx must be a u32")))?;
        let amount = match parts.next() {
            Some(raw) if !raw.is_empty() => Some(raw.parse().map_err(|_| {
                PenguinError::TransactionParse(Cow::Borrowed("amount must be f64"))
            })?),
            _ => None,
        };

        Ok(Transaction {
            tx_type,
            client,
            tx,
            amount,
        })
    }
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
    #[error("Error parsing transaction: {0}")]
    TransactionParse(Cow<'static, str>),
}
