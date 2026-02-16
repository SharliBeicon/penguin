use clap::Parser;
use csv::{ReaderBuilder, Trim};
use serde::Deserialize;
use std::io::{self, Write};
use thiserror::Error;

/// Penguin - A [p]ayments [engin]e
#[derive(Parser)]
struct Args {
    /// Input CSV file with a list of transactions
    input: String,
}

#[derive(Debug, Deserialize)]
struct Transaction {
    #[serde(rename = "type")]
    tx_type: TransactionType,
    client: u16,
    tx: u32,
    amount: Option<f64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
enum TransactionType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
}

#[derive(Error, Debug)]
enum PenguinError {
    #[error("I/O error: {0}")]
    IO(#[from] io::Error),
    #[error("CSV parse error: {0}")]
    Csv(#[from] csv::Error),
}

type Result<T> = std::result::Result<T, PenguinError>;

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let mut rdr = ReaderBuilder::new().trim(Trim::All).from_path(args.input)?;
    let iter = rdr.deserialize();

    let stdout = io::stdout();
    let mut out = stdout.lock();

    for line in iter {
        let tx: Transaction = line?;
        writeln!(out, "{:?}", tx)?;
    }

    Ok(())
}
