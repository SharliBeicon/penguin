<div align="center">
    <img src="misc/logo.png" alt="Penguin" width="200" />
    <!-- All of this "toc" div just to avoid the annoying underline on headings -->
    <div id="toc">
        <ul style="list-style: none">
            <summary>
                <h1>Penguin</h1>
            </summary>
        </ul>
    </div>
    <h2>A [p]ayments [engin]e</h2>
    <a href="https://github.com/SharliBeicon/penguin/actions/workflows/rust.yml">
        <img src="https://github.com/SharliBeicon/penguin/actions/workflows/rust.yml/badge.svg?branch=main" alt="Rust CI" />
    </a>
</div>

Penguin is a toy Payments Engine with the ability to read a list of transactions (deposits, withdrawals, disputes, resolves and chargebacks) for different clients and returns the status of each client after that transactions sequence.

This project is divided in two parts.

- `libpenguin`: the engine itself, developed as a library aiming to be generic, extendable and composable.
- `penguin-cli`: a command line utility that receives an input `csv` file with a list of transactions, and writes the list of `ClientStates` in form of `csv` to the standard output.

### libpenguin

`Penguin Engine` has a pretty straightforward API. Just build a `Penguin` instance with the help of the `PenguinBuilder` struct, passing a reader (an iterator over a sequence of `Transactions`) and your desired configuration options. 

Once `Penguin` is built, just await for `run()` method to finish and it will output a list of `ClientStates` for you to handle as you desire.

For a comprehensive library documentation, please run:

```bash
cargo doc -p libpenguin --open
```

`libpenguin` package has to be explicitly specified since the default crate of the project is `penguin-cli`, to be able to run it with a plain `cargo run` command.

#### Usage example

```rust
let inputs = [
    "deposit, 1, 1, 1.0",
    "deposit, 2, 2, 2.0",
    "deposit, 1, 3, 2.0",
    "withdrawal, 1, 4, 1.5",
    "withdrawal, 2, 5, 3.0",
    "deposit, 1, 5,",
];

// Transaction implements FromStr trait so you can parse plain text.
// Also, it implements Deserialize's `serde` trait,
// so you can parse them from libraries that are "serde-compatible", such as `csv`
let reader = inputs.into_iter().map(|line| {
    Ok::<Transaction, PenguinError>(line.parse::<Transaction>().expect("valid transaction"))
});

let mut penguin = PenguinBuilder::from_reader(reader) // Iterator over Result<Transaction, E>
    .with_num_workers(num_workers) // Num of workers that will concurrently process the iterator.
    .with_logger("penguin.log") // If you want `Penguin` to log possible errors, use this. If you don't call this, nothing will be logged at all
    .build()?;

let output = penguin.run().await?;
```


### penguin-cli

`penguin-cli` is a tiny wrapper around `libpenguin`. It reads transactions from a CSV file and prints the resulting client states to stdout, so you can pipe it to a file.

#### Usage example

```bash
cargo run -- input.csv > output.csv
```

### AI usage disclaimer

No AI has been used to solve the core business logic of this project. Architectural, technical, and philosophical decisions are my own.

However, `opencode` has been used as an assistant to solve some "side quests":

- To clarify the syntax of certain APIs: "How can `tracing` write the formatted version of the error instead of the enum variant."
- To refactor a specific piece of code: "I don't like the thread spawning being in the `Penguin::run()` function. Move it to a separated `spawn_worker()` one."
- To develop the test suite: Given some inputs, some expected outputs, and the current project implementation, a decent first iteration of the test suite was made by AI. The test suite was later reviewed by me.
- To make me sure we comply with certain constraints: "Float numbers can lead to unexpected behavior while leading with decimal numbers. Is there a safer approach to handle them?" - That made me use `rust_decimal::Decimal` type.
- The cute penguin logo.
