# aq

[![uses nix](https://img.shields.io/badge/uses-nix-%237EBAE4)](https://nixos.org/)
![rust](https://img.shields.io/badge/Rust-1.95%2B-orange.svg)

`aq` is a jq-style CLI for querying and rewriting structured data across more than just JSON.

It currently supports:

- jq-style query semantics and result streams
- JSON, JSONL, YAML, TOML, CSV, and TSV input and output
- output-only table rendering for inspection
- atomic in-place rewrites
- optional Starlark scripting and a persistent REPL with history, editing, and IDE-style completion for heavier transforms

## Install

- Release artifacts:
  tagged releases publish Linux musl `x86_64` and `aarch64`, macOS `aarch64`, and Windows `x86_64` binaries.
- Nix:
  TODO

## Quick Start

```bash
cargo build --release

aq '.users[] | select(.age >= 30) | .name' users.json
aq '.name' -
aq -f yaml -o json '.services[].port' compose.yaml
aq -o table '.items[] | {name, status, owner}' data.json
aq --stream 'select(.status >= 500)' logs.jsonl
aq --in-place '.version = "2.0"' config.toml
aq -n --starlark '1 + 2'
aq -n -P
```

## Docs

- Site source: [`docs/`](docs/)
- Performance: [`docs/performance.md`](docs/performance.md)
- jq compatibility: [`docs/jq-compatibility.md`](docs/jq-compatibility.md)
- Starlark: [`docs/starlark.md`](docs/starlark.md)
- Starlark examples: [`examples/starlark/README.md`](examples/starlark/README.md)

## Development

```bash
cargo fmt
cargo clippy --all --benches --tests --examples --all-features -- -D warnings

cd docs
bun install
bun run docs:dev
```
