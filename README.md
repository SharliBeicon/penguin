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

lorem ipsum

### penguin-cli

lorem ipsum

### AI usage disclaimer

No AI has been used to solve the core business logic of this project. Architectural, technical, and philosophical decisions are my own.

However, `opencode` has been used as an assistant to solve some "side quests":

- To clarify the syntax of certain APIs: "Can tracing write the formatted version of the error instead of the enum variant?"
- To refactor a specific piece of code: "I don't like the thread spawning behavior in the Penguin::run() function. Move it to a separate spawn_worker() one."
- To develop the test suite: Given some inputs, some expected outputs, and the current project implementation, a decent first iteration of the test suite was made by AI. The test suite was later reviewed by me.
