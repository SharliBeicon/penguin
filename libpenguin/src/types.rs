use rust_decimal::Decimal;
use serde::{Deserialize, Serialize, ser::SerializeStruct};
use std::{borrow::Cow, io, str::FromStr};
use thiserror::Error;
use tokio::sync::mpsc::error::SendError;

pub(crate) type TxResult<E> = Result<Transaction, E>;

/// A transaction coming from the input stream.
///
/// Any source is fine as long as it can produce values compatible with this struct.
#[derive(Debug, Deserialize)]
pub struct Transaction {
    #[serde(rename = "type")]
    /// Transaction type.
    pub tx_type: TransactionType,
    /// Client identifier.
    pub client: u16,
    /// Transaction identifier.
    pub tx: u32,
    /// Optional amount for deposit/withdrawal transactions.
    pub amount: Option<Decimal>,
}

/// Parse a transaction from a CSV-like line.
///
/// The expected format is: `type, client, tx, amount` where `amount` is optional.
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

/// Current state for a client.
#[derive(Debug)]
pub struct ClientState {
    /// Client identifier.
    pub client: u16,
    /// Funds available for withdrawal.
    pub available: Decimal,
    /// Funds held due to disputes.
    pub held: Decimal,
    /// Total funds (available + held).
    pub total: Decimal,
    /// Whether the account is locked by a chargeback.
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
    /// Create a new client state.
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

/// Convenience alias for (client_id, transaction_id)
pub(crate) type ClientTx = (u16, u32);

/// Supported transaction types.
#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum TransactionType {
    /// Increase available funds.
    Deposit,
    /// Decrease available funds if enough balance exists.
    Withdrawal,
    /// Move funds from available to held.
    Dispute,
    /// Release held funds back to available.
    Resolve,
    /// Finalize a dispute and lock the account.
    Chargeback,
}

/// Errors emitted by the engine and helpers.
#[derive(Error, Debug)]
pub enum PenguinError {
    /// I/O error while reading input or writing logs.
    #[error("I/O error: {0}")]
    IO(#[from] io::Error),
    /// Parsing failed at a given line number in the input.
    #[error("Error while parsing on line {0}")]
    Parse(usize),
    /// Failed to send a transaction to a worker channel.
    #[error("Error sending transaction to the channel: {0}")]
    ChannelSend(#[from] SendError<Transaction>),
    /// Deposit/withdrawal was missing an amount.
    #[error("Client {0} received a deposit/withdrawal transaction with no amount associated.")]
    DepositOrWithdrawalWithoutAmount(u16),
    /// Transaction text did not match the expected CSV-like format.
    #[error("Error parsing transaction: {0}")]
    TransactionParse(Cow<'static, str>),
}
