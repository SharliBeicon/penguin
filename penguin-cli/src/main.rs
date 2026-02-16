use clap::Parser;
use csv::{ReaderBuilder, Trim};
use libpenguin::{PenguinError, Transaction};
use std::io::{self, Write};
use thiserror::Error;

/// Penguin CLI - A command line tool to process a list of transactions with Penguin Engine
#[derive(Parser)]
struct Args {
    /// Input CSV file
    input: String,
}

#[derive(Error, Debug)]
enum CliError {
    #[error("Penguin error: {0}")]
    Penguin(#[from] PenguinError),
    #[error("CSV parse error: {0}")]
    Csv(#[from] csv::Error),
}

#[tokio::main]
async fn main() -> Result<(), CliError> {
    let args = Args::parse();
    let mut rdr = ReaderBuilder::new().trim(Trim::All).from_path(args.input)?;
    let iter = rdr.deserialize();

    let stdout = io::stdout();
    let mut out = stdout.lock();

    for line in iter {
        let tx: Transaction = line?;
        writeln!(out, "{:?}", tx).map_err(PenguinError::from)?;
    }

    Ok(())
}
