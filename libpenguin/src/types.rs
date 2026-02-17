use rust_decimal::Decimal;
use serde::{Deserialize, Serialize, ser::SerializeStruct};
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
    pub amount: Option<Decimal>,
}

impl FromStr for Transaction {
    type Err = PenguinError;

    fn from_str(line: &str) -> Result<Self, Self::Err> {
        let mut parts = line.split(',').map(|part| part.trim());
        let tx_type = match parts
            .next()
            .ok_or(PenguinError::TransactionParse(Cow::Borrowed(
                "type is required",
            )))? {
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
            .ok_or(PenguinError::TransactionParse(Cow::Borrowed(
                "client is required",
            )))?
            .parse()
            .map_err(|_| PenguinError::TransactionParse(Cow::Borrowed("client must be a u16")))?;
        let tx = parts
            .next()
            .ok_or(PenguinError::TransactionParse(Cow::Borrowed(
                "tx is required",
            )))?
            .parse()
            .map_err(|_| PenguinError::TransactionParse(Cow::Borrowed("tx must be a u32")))?;
        let amount = match parts.next() {
            Some(raw) if !raw.is_empty() => Some(
                Decimal::from_str(raw)
                    .map_err(|_| {
                        PenguinError::TransactionParse(Cow::Borrowed("amount must be decimal"))
                    })?
                    .round_dp(4),
            ),
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

#[derive(Debug)]
pub struct ClientState {
    pub client: u16,
    pub available: Decimal,
    pub held: Decimal,
    pub total: Decimal,
    pub locked: bool,
}

impl Serialize for ClientState {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let format_decimal = |value: Decimal| value.round_dp(4).normalize().to_string();

        let mut state = serializer.serialize_struct("ClientState", 5)?;
        state.serialize_field("client", &self.client)?;
        state.serialize_field("available", &format_decimal(self.available))?;
        state.serialize_field("held", &format_decimal(self.held))?;
        state.serialize_field("total", &format_decimal(self.total))?;
        state.serialize_field("locked", &self.locked)?;
        state.end()
    }
}

impl ClientState {
    pub fn new(client: u16) -> Self {
        Self {
            client,
            available: Decimal::ZERO,
            held: Decimal::ZERO,
            total: Decimal::ZERO,
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
