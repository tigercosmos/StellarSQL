# StellarSQL

[![Build Status](https://travis-ci.org/tigercosmos/StellarSQL.svg?branch=master)](https://travis-ci.org/tigercosmos/StellarSQL)

(WIP) A minimal SQL DBMS written in Rust

There is a series of articles introducing about this project: [Let's build a DBMS](https://tigercosmos.xyz/lets-build-dbms/)

![logo](https://raw.githubusercontent.com/tigercosmos/StellarSQL/master/logo/logo.png)

## Setup

Before you start, you need to have Rust(>=1.31) and Cargo.

```bash
curl https://sh.rustup.rs -sSf | sh
```

Then we could get the source code.

```bash
git clone https://github.com/tigercosmos/StellarSQL
cd StellarSQL
```

## Build

```bash
cargo build
```

## Test

## Run all tests

```bash
cargo test
```

## Debug a test

Add the line at the beginning of the test function.

```rust
// init the logger for the test
env_logger::init();
```

Then run the command to see the debug information:

```sh
RUST_LOG=debug cargo test -- --nocapture {test_name}
```

## Run

```bash
cargo run
```

## Pull Request

Install [rustfmt](https://github.com/rust-lang/rustfmt), and make sure you could pass:

```sh
cargo fmt --all -- --check
cargo build
cargo test
```

## Document

Build and open the document at localhost

```sh
cargo rustdoc --open -- --document-private-items
```

## License

MIT
