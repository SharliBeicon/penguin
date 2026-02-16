use serde::{Deserialize, Serialize};
use std::{collections::HashMap, io, num::NonZero};
use thiserror::Error;
use tokio::{
    sync::mpsc::{self, error::SendError},
    task::{JoinError, JoinHandle},
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
    #[error("Error while parsing Transaction from reader")]
    Parse,
    #[error("Error sending transaction to the channel: {0}")]
    ChannelSend(#[from] SendError<Transaction>),
    #[error("Client {0} received a deposit/withdrawal transaction with no amount associated.")]
    DepositOrWithdrawalWithoutAmount(u16),
    #[error("Worker task failed: {0}")]
    WorkerJoin(#[from] JoinError),
}

type TxResult<E> = Result<Transaction, E>;

pub struct Penguin<T> {
    reader: T,
    num_workers: usize,
    senders: HashMap<u16, mpsc::Sender<Transaction>>,
    join_handles: Vec<JoinHandle<Vec<ClientState>>>,
}

impl<T, E> Penguin<T>
where
    T: Iterator<Item = TxResult<E>>,
{
    pub async fn run(&mut self) -> Result<Vec<ClientState>, PenguinError> {
        for group_id in 0..self.num_workers {
            let group_id = group_id as u16;
            let (tx, rx) = mpsc::channel(1024);

            self.senders.insert(group_id, tx);
            self.join_handles
                .push(tokio::spawn(spawn_worker(group_id, rx)));
        }

        while let Some(row) = self.reader.next() {
            let tx = row.map_err(|_| PenguinError::Parse)?;
            let group = (tx.client) % self.num_workers as u16;
            self.senders[&group].send(tx).await?;
        }

        self.senders.clear();

        let mut all_clients = Vec::new();
        for handle in self.join_handles.drain(..) {
            let mut group_clients = handle.await?;
            all_clients.append(&mut group_clients);
        }

        Ok(all_clients)
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
        let senders: HashMap<u16, mpsc::Sender<Transaction>> = HashMap::with_capacity(num_workers);
        let join_handles: Vec<JoinHandle<Vec<ClientState>>> = Vec::with_capacity(num_workers);

        Penguin {
            reader: self.reader,
            num_workers,
            senders,
            join_handles,
        }
    }
}

#[cfg(test)]
mod tests {}
