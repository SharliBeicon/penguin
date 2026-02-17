use serde::{Deserialize, Serialize};
use std::{collections::HashMap, io, num::NonZero};
use thiserror::Error;
use tokio::{
    sync::mpsc::{self, error::SendError},
    task::{JoinHandle, JoinSet},
};

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

pub struct Penguin<T> {
    reader: T,
    num_workers: usize,
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
            set.spawn(spawn_worker(group_id, rx));
        }

        let mut line_count = 1;
        while let Some(line) = self.reader.next() {
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
                Err(_) => todo!("log an error"),
            }
        }

        Ok(group_clients)
    }
}

fn apply_tx(client_state: &mut ClientState, tx: Transaction) -> Result<(), PenguinError> {
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
                //TODO: Log a warning

                return Ok(());
            }
            client_state.available -= amount;
            client_state.total -= amount;
        }
        TType::Dispute => todo!(),
        TType::Resolve => todo!(),
        TType::Chargeback => todo!(),
    }

    Ok(())
}

async fn spawn_worker(group_id: u16, mut rx: mpsc::Receiver<Transaction>) -> Vec<ClientState> {
    let mut client_states: HashMap<u16, ClientState> = HashMap::new();

    while let Some(tx) = rx.recv().await {
        let client_state = client_states
            .entry(tx.client)
            .or_insert(ClientState::new(tx.client));

        if let Err(err) = apply_tx(client_state, tx) {
            // TODO: Log an error
        }
    }

    client_states.into_values().collect()
}

pub struct PenguinBuilder<T> {
    reader: T,
    num_workers: Option<usize>,
}

impl<T, E> PenguinBuilder<T>
where
    T: Iterator<Item = TxResult<E>>,
{
    pub fn from_reader(reader: T) -> Self {
        Self {
            reader,
            num_workers: None,
        }
    }

    pub fn with_num_workers(self, num_workers: NonZero<usize>) -> Self {
        Self {
            reader: self.reader,
            num_workers: Some(num_workers.get()),
        }
    }

    pub fn build(self) -> Penguin<T> {
        let num_workers = self.num_workers.unwrap_or(1);

        Penguin {
            reader: self.reader,
            num_workers,
        }
    }
}

#[cfg(test)]
mod tests {}
