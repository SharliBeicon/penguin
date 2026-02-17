use crate::{logger::Logger, types::*};
use rust_decimal::Decimal;
use std::{collections::HashMap, num::NonZero, path::PathBuf};
use tokio::{sync::mpsc, task::JoinSet};
use tracing::{error, warn};

pub struct Penguin<T> {
    reader: T,
    num_workers: usize,
    _logger: Option<Logger>,
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

        let _logger = if let Some(path) = self.log_file {
            Some(Logger::try_init_from_path(path)?)
        } else {
            None
        };

        Ok(Penguin {
            reader: self.reader,
            num_workers,
            _logger,
        })
    }
}

async fn spawn_worker(mut rx: mpsc::Receiver<Transaction>) -> Vec<ClientState> {
    let mut client_states: HashMap<u16, ClientState> = HashMap::new();
    let mut client_tx_registry: HashMap<ClientTx, Decimal> = HashMap::new();

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

fn apply_tx(
    client_state: &mut ClientState,
    tx: &Transaction,
    client_tx_registry: &HashMap<ClientTx, Decimal>,
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
            if client_state.available < amount {
                warn!(
                    client = client_state.client,
                    tx = tx.tx,
                    amount = %amount,
                    available = %client_state.available,
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

            client_state.held += *tx_amount;
            client_state.available -= *tx_amount;
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

            client_state.held -= *tx_amount;
            client_state.available += *tx_amount;
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

            client_state.held -= *tx_amount;
            client_state.available -= *tx_amount;
            client_state.total -= *tx_amount;
            client_state.locked = true;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::str::FromStr;

    fn dec(value: &str) -> Decimal {
        Decimal::from_str(value).expect("valid decimal")
    }

    fn tx(tx_type: TransactionType, client: u16, tx: u32, amount: Option<Decimal>) -> Transaction {
        Transaction {
            tx_type,
            client,
            tx,
            amount,
        }
    }

    fn assert_state(
        state: &ClientState,
        client: u16,
        available: Decimal,
        held: Decimal,
        total: Decimal,
    ) {
        assert_eq!(state.client, client);
        assert_eq!(state.available, available);
        assert_eq!(state.held, held);
        assert_eq!(state.total, total);
    }

    #[tokio::test]
    async fn run_multiple_clients_with_mixed_transactions() {
        let inputs = [
            "deposit, 1, 1, 1.0",
            "deposit, 2, 2, 2.0",
            "deposit, 1, 3, 2.0",
            "withdrawal, 1, 4, 1.5",
            "withdrawal, 2, 5, 3.0",
            "deposit, 1, 5,",
        ];
        let reader = inputs.into_iter().map(|line| {
            Ok::<Transaction, PenguinError>(line.parse::<Transaction>().expect("valid transaction"))
        });
        let mut penguin = Penguin {
            reader,
            num_workers: 2,
            _logger: None,
        };

        let mut output = penguin.run().await.expect("run should succeed");
        output.sort_by_key(|state| state.client);

        assert_eq!(output.len(), 2);
        assert_state(&output[0], 1, dec("1.5"), dec("0"), dec("1.5"));
        assert_state(&output[1], 2, dec("2"), dec("0"), dec("2"));
    }

    #[tokio::test]
    async fn run_returns_parse_error_with_line_number() {
        let reader = vec![
            Ok(tx(TransactionType::Deposit, 1, 1, Some(dec("1.0")))),
            Err(()),
        ]
        .into_iter();
        let mut penguin = Penguin {
            reader,
            num_workers: 1,
            _logger: None,
        };

        let err = penguin.run().await.expect_err("expected parse error");
        assert!(matches!(err, PenguinError::Parse(2)));
    }

    #[test]
    fn deposit_and_withdrawal_update_balances() {
        let mut client_state = ClientState::new(1);
        let registry: HashMap<ClientTx, Decimal> = HashMap::new();

        apply_tx(
            &mut client_state,
            &tx(TransactionType::Deposit, 1, 1, Some(dec("1.0"))),
            &registry,
        )
        .expect("deposit should succeed");

        apply_tx(
            &mut client_state,
            &tx(TransactionType::Withdrawal, 1, 2, Some(dec("0.4"))),
            &registry,
        )
        .expect("withdrawal should succeed");

        assert_state(&client_state, 1, dec("0.6"), dec("0"), dec("0.6"));
    }

    #[test]
    fn withdrawal_with_insufficient_funds_is_ignored() {
        let mut client_state = ClientState::new(1);
        let registry: HashMap<ClientTx, Decimal> = HashMap::new();

        apply_tx(
            &mut client_state,
            &tx(TransactionType::Deposit, 1, 1, Some(dec("1.0"))),
            &registry,
        )
        .expect("deposit should succeed");

        apply_tx(
            &mut client_state,
            &tx(TransactionType::Withdrawal, 1, 2, Some(dec("2.0"))),
            &registry,
        )
        .expect("withdrawal is ignored when insufficient");

        assert_state(&client_state, 1, dec("1.0"), dec("0"), dec("1.0"));
    }

    #[test]
    fn dispute_and_resolve_move_funds_between_available_and_held() {
        let mut client_state = ClientState::new(1);
        let mut registry: HashMap<ClientTx, Decimal> = HashMap::new();

        apply_tx(
            &mut client_state,
            &tx(TransactionType::Deposit, 1, 1, Some(dec("1.0"))),
            &registry,
        )
        .expect("deposit should succeed");

        registry.insert((1, 1), dec("1.0"));

        apply_tx(
            &mut client_state,
            &tx(TransactionType::Dispute, 1, 1, None),
            &registry,
        )
        .expect("dispute should succeed");
        assert_state(&client_state, 1, dec("0"), dec("1.0"), dec("1.0"));

        apply_tx(
            &mut client_state,
            &tx(TransactionType::Resolve, 1, 1, None),
            &registry,
        )
        .expect("resolve should succeed");

        assert_state(&client_state, 1, dec("1.0"), dec("0"), dec("1.0"));
    }

    #[test]
    fn chargeback_locks_account_and_updates_totals() {
        let mut client_state = ClientState::new(1);
        let mut registry: HashMap<ClientTx, Decimal> = HashMap::new();

        apply_tx(
            &mut client_state,
            &tx(TransactionType::Deposit, 1, 1, Some(dec("1.0"))),
            &registry,
        )
        .expect("deposit should succeed");

        registry.insert((1, 1), dec("1.0"));

        apply_tx(
            &mut client_state,
            &tx(TransactionType::Dispute, 1, 1, None),
            &registry,
        )
        .expect("dispute should succeed");

        apply_tx(
            &mut client_state,
            &tx(TransactionType::Chargeback, 1, 1, None),
            &registry,
        )
        .expect("chargeback should succeed");

        assert!(client_state.locked);
        assert_state(&client_state, 1, dec("-1.0"), dec("0"), dec("0"));

        apply_tx(
            &mut client_state,
            &tx(TransactionType::Deposit, 1, 2, Some(dec("5.0"))),
            &registry,
        )
        .expect("locked accounts ignore deposits");

        assert_state(&client_state, 1, dec("-1.0"), dec("0"), dec("0"));
    }

    #[test]
    fn deposit_without_amount_is_an_error() {
        let mut client_state = ClientState::new(1);
        let registry: HashMap<ClientTx, Decimal> = HashMap::new();

        let err = apply_tx(
            &mut client_state,
            &tx(TransactionType::Deposit, 1, 1, None),
            &registry,
        )
        .expect_err("expected deposit without amount to error");

        assert!(matches!(
            err,
            PenguinError::DepositOrWithdrawalWithoutAmount(1)
        ));
    }
}
