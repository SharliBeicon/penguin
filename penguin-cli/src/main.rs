use clap::Parser;
use csv::{ReaderBuilder, Trim, WriterBuilder};
use libpenguin::prelude::*;
use std::{io, num::NonZeroUsize};
use thiserror::Error;
use tokio_stream::StreamExt;

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
    #[error("I/O error: {0}")]
    IO(#[from] io::Error),
}

#[tokio::main]
async fn main() -> Result<(), CliError> {
    let args = Args::parse();
    let mut reader = ReaderBuilder::new().trim(Trim::All).from_path(args.input)?;
    let reader = reader.deserialize();

    let num_workers = std::thread::available_parallelism().unwrap_or(
        NonZeroUsize::new(4).unwrap(), // Not zero, so cannot fail
    );

    let mut penguin = PenguinBuilder::from_reader(reader)
        .with_num_workers(num_workers)
        .with_logger("penguin.log")
        .build()?;

    let mut writer = WriterBuilder::new()
        .has_headers(true)
        .from_writer(io::stdout());

    let mut stream = penguin.get_stream().await?;
    while let Some(states) = stream.next().await {
        for state in states {
            writer.serialize(state)?;
        }
    }
    writer.flush()?;

    Ok(())
}
