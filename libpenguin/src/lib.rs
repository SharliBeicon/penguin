use serde::{Deserialize, Serialize};
use std::{collections::HashMap, io, num::NonZero, path::Path, path::PathBuf};
use thiserror::Error;
use tokio::{
    sync::mpsc::{self, error::SendError},
    task::JoinSet,
};
use tracing::{error, warn};

#[derive(Debug, Deserialize)]
pub struct Transaction {
    #[serde(rename = "type")]
    tx_type: TransactionType,
    client: u16,
    tx: u32,
    amount: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct ClientState {
    client: u16,
    available: f64,
    held: f64,
    total: f64,
    locked: bool,
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

type ClientTx = (u16, u32); // (client_id, transaction_id)

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

type TxResult<E> = Result<Transaction, E>;

pub struct LogGuard {
    _guard: tracing_appender::non_blocking::WorkerGuard,
}

pub fn init_file_tracing(path: impl AsRef<Path>) -> io::Result<LogGuard> {
    let file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)?;
    let (non_blocking, _guard) = tracing_appender::non_blocking(file);
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    tracing_subscriber::fmt()
        .with_writer(non_blocking)
        .with_env_filter(env_filter)
        .with_ansi(false)
        .with_target(false)
        .try_init()
        .map_err(io::Error::other)?;

    Ok(LogGuard { _guard })
}

pub struct Penguin<T> {
    reader: T,
    num_workers: usize,
    _log_guard: Option<LogGuard>,
}

impl<T, E> Penguin<T>
where
    T: Iterator<Item = TxResult<E>>,
{
    pub async fn run(&mut self) -> Result<Vec<ClientState>, PenguinError> {
        let mut senders: HashMap<u16, mpsc::Sender<Transaction>> =
            HashMap::with_capacity(self.num_workers);
        let mut set = JoinSet::new();

        for group_id in 0..self.num_workers {
            let group_id = group_id as u16;
            let (tx, rx) = mpsc::channel(1024);

            senders.insert(group_id, tx);
            set.spawn(spawn_worker(rx));
        }

        let mut line_count = 1;
        for line in self.reader.by_ref() {
            let tx = line.map_err(|_| PenguinError::Parse(line_count))?;
            let group = (tx.client) % self.num_workers as u16;
            senders[&group].send(tx).await?;
            line_count += 1;
        }

        drop(senders);

        let mut group_clients = Vec::with_capacity(self.num_workers);
        while let Some(handle) = set.join_next().await {
            match handle {
                Ok(mut group_client) => group_clients.append(&mut group_client),
                Err(err) => error!(%err, "worker task failed"),
            }
        }

        Ok(group_clients)
    }
}

fn apply_tx(
    client_state: &mut ClientState,
    tx: &Transaction,
    client_tx_registry: &HashMap<ClientTx, f64>,
) -> Result<(), PenguinError> {
    use TransactionType as TType;

    if client_state.locked {
        return Ok(());
    }

    match tx.tx_type {
        TType::Deposit => {
            let amount = tx
                .amount
                .ok_or(PenguinError::DepositOrWithdrawalWithoutAmount(
                    client_state.client,
                ))?;
            client_state.available += amount;
            client_state.total += amount;
        }
        TType::Withdrawal => {
            let amount = tx
                .amount
                .ok_or(PenguinError::DepositOrWithdrawalWithoutAmount(
                    client_state.client,
                ))?;
            if client_state.available - amount < 0.0 {
                warn!(
                    client = client_state.client,
                    tx = tx.tx,
                    amount,
                    available = client_state.available,
                    "insufficient funds for withdrawal"
                );

                return Ok(());
            }
            client_state.available -= amount;
            client_state.total -= amount;
        }
        TType::Dispute => {
            let Some(tx_amount) = client_tx_registry.get(&(tx.client, tx.tx)) else {
                warn!(
                    client = tx.client,
                    tx = tx.tx,
                    "dispute for unknown transaction"
                );

                return Ok(());
            };

            client_state.held += tx_amount;
            client_state.available -= tx_amount;
        }
        TType::Resolve => {
            let Some(tx_amount) = client_tx_registry.get(&(tx.client, tx.tx)) else {
                warn!(
                    client = tx.client,
                    tx = tx.tx,
                    "resolve for unknown transaction"
                );

                return Ok(());
            };

            client_state.held -= tx_amount;
            client_state.available += tx_amount;
        }
        TType::Chargeback => {
            let Some(tx_amount) = client_tx_registry.get(&(tx.client, tx.tx)) else {
                warn!(
                    client = tx.client,
                    tx = tx.tx,
                    "chargeback for unknown transaction"
                );

                return Ok(());
            };

            client_state.held -= tx_amount;
            client_state.available -= tx_amount;
            client_state.total -= tx_amount;
            client_state.locked = true;
        }
    }

    Ok(())
}

async fn spawn_worker(mut rx: mpsc::Receiver<Transaction>) -> Vec<ClientState> {
    let mut client_states: HashMap<u16, ClientState> = HashMap::new();
    let mut client_tx_registry: HashMap<ClientTx, f64> = HashMap::new();

    while let Some(tx) = rx.recv().await {
        let client_state = client_states
            .entry(tx.client)
            .or_insert(ClientState::new(tx.client));

        if let Some(amount) = tx.amount {
            client_tx_registry
                .entry((tx.client, tx.tx))
                .or_insert(amount);
        }

        if let Err(err) = apply_tx(client_state, &tx, &client_tx_registry) {
            error!(
                %err,
                client = client_state.client,
                tx = tx.tx,
                "failed to apply transaction"
            );
        }
    }

    client_states.into_values().collect()
}

pub struct PenguinBuilder<T> {
    reader: T,
    num_workers: Option<usize>,
    log_file: Option<PathBuf>,
}

impl<T, E> PenguinBuilder<T>
where
    T: Iterator<Item = TxResult<E>>,
{
    pub fn from_reader(reader: T) -> Self {
        Self {
            reader,
            num_workers: None,
            log_file: Some(PathBuf::from("penguin.log")),
        }
    }

    pub fn with_num_workers(self, num_workers: NonZero<usize>) -> Self {
        Self {
            reader: self.reader,
            num_workers: Some(num_workers.get()),
            log_file: self.log_file,
        }
    }

    pub fn with_logger(self, path: impl Into<PathBuf>) -> Self {
        Self {
            reader: self.reader,
            num_workers: self.num_workers,
            log_file: Some(path.into()),
        }
    }

    pub fn build(self) -> Result<Penguin<T>, PenguinError> {
        let num_workers = self.num_workers.unwrap_or(1);

        let _log_guard = if let Some(path) = self.log_file {
            Some(init_file_tracing(path)?)
        } else {
            None
        };

        Ok(Penguin {
            reader: self.reader,
            num_workers,
            _log_guard,
        })
    }
}

#[cfg(test)]
mod tests {}
