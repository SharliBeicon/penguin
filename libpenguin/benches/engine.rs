use criterion::{Criterion, black_box, criterion_group, criterion_main};
use libpenguin::prelude::*;
use std::{
    fs::File,
    io::{BufRead, BufReader, Lines},
    num::NonZeroUsize,
};
use tokio_stream::StreamExt;

fn create_reader(file: File) -> Lines<BufReader<File>> {
    let reader = BufReader::new(file);
    let mut reader = reader.lines();
    reader.next(); // bypass header
    reader
}

fn bench_engine(c: &mut Criterion) {
    let file = File::open("../input.csv").expect("File must exists");

    let num_workers = NonZeroUsize::new(4).unwrap();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime should build");

    c.bench_function("engine_run_collect", |b| {
        b.iter(|| {
            let file = file.try_clone().expect("clonedfile");
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
            let file = file.try_clone().expect("clonedfile");
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

criterion_group!(benches, bench_engine);
criterion_main!(benches);
