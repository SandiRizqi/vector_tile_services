@echo off
set RUST_LOG=info
set SQLX_WARN_SLOW_STATEMENTS=5
cargo run
