use criterion::{Criterion, black_box, criterion_group, criterion_main};
use libpenguin::prelude::*;
use std::{
    fs::File,
    io::{BufRead, BufReader, Lines},
    num::NonZeroUsize,
    path::PathBuf,
    time::Duration,
};
use tokio_stream::StreamExt;

fn create_reader(file: File) -> Lines<BufReader<File>> {
    let reader = BufReader::new(file);
    let mut reader = reader.lines();
    reader.next(); // bypass header
    reader
}

fn bench_config() -> Criterion {
    Criterion::default().measurement_time(Duration::from_secs(30))
}

fn bench_engine(c: &mut Criterion) {
    let input_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../input.csv");

    let num_workers = std::thread::available_parallelism().unwrap_or(NonZeroUsize::new(8).unwrap());

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime should build");

    c.bench_function("engine_run_collect", |b| {
        b.iter(|| {
            let file = File::open(&input_path).expect("input file must exist");
            let reader = create_reader(file)
                .map(|line: Result<String, std::io::Error>| line.map_err(PenguinError::from))
                .map(|line| line.and_then(|l| l.parse::<Transaction>()));
            let mut penguin = PenguinBuilder::from_reader(reader)
                .with_num_workers(num_workers)
                .build()
                .expect("build should succeed");

            runtime.block_on(async {
                let output = penguin.run().await.expect("run should succeed");
                black_box(output);
            });
        });
    });

    c.bench_function("engine_process_stream", |b| {
        b.iter(|| {
            let file = File::open(&input_path).expect("input file must exist");
            let reader = create_reader(file)
                .map(|line: Result<String, std::io::Error>| line.map_err(PenguinError::from))
                .map(|line| line.and_then(|l| l.parse::<Transaction>()));
            let mut penguin = PenguinBuilder::from_reader(reader)
                .with_num_workers(num_workers)
                .build()
                .expect("build should succeed");

            runtime.block_on(async {
                let mut stream = penguin.get_stream().await.expect("process should succeed");
                while let Some(states) = stream.next().await {
                    black_box(states);
                }
            });
        });
    });
}

criterion_group!(
    name = benches;
    config = bench_config();
    targets = bench_engine
);

criterion_main!(benches);
