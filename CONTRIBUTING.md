# Contributing to Butterfly-OSM

## Contributor License Agreement

**By submitting a pull request, issue patch, or any other contribution to
this repository, you agree that your contribution is licensed under the
GNU Affero General Public License, version 3 or (at your option) any
later version — "AGPL-3.0-or-later".** You affirm that you have the
right to license the contribution under these terms, and that your
contribution is your own work or is clearly attributed to its upstream
source. There is no separate CLA to sign; submission implies agreement.

The full license text lives in [LICENSE](LICENSE). Network-deployed
forks of this project must publish complete corresponding source per
AGPL §13.

## Code of Conduct

Be kind, be precise, be honest about performance claims. Report
unacceptable behaviour to <pierre@warnier.net>. See
[CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) when present.

## Build & Run (Docker-first)

Docker is the primary build and deployment method. See
[CLAUDE.md](CLAUDE.md) for the full command reference.

```bash
# Build the image
docker build -t butterfly-route .

# Run the server on Belgium data (port 3001)
docker run -d --name butterfly \
  -p 3001:8080 \
  -v "${PWD}/data/belgium:/data" \
  butterfly-route
```

## Local Development (cargo)

```bash
# Full workspace build
cargo build --workspace

# Run the full test suite (the only supported dataset is Belgium)
cargo test --workspace --lib

# Lint — workspace lints are warnings = deny, so clippy must be clean
cargo clippy --workspace --all-targets --all-features

# Format
cargo fmt --all -- --check
cargo fmt --all               # auto-fix
```

## Commit & PR conventions

- **Conventional Commits**: `feat(route): ...`, `fix(dl): ...`,
  `perf(common): ...`, `chore(license): ...`.
- **Atomic commits**: one logical change per commit.
- **Belgium-only** test data — never add Monaco, Luxembourg, or other
  regions to tests or benchmarks.
- **No placeholders, no `TODO: implement later`**. If a change cannot be
  completed correctly, open a discussion instead of merging partial
  work.

## License headers

The workspace `Cargo.toml` declares `license =
"AGPL-3.0-or-later"` and every crate inherits via
`license.workspace = true`, so individual `.rs` files do not require an
SPDX header. If you do add an SPDX line, it **must** be exactly:

```rust
// SPDX-License-Identifier: AGPL-3.0-or-later
```

Never commit a file whose SPDX identifier is anything other than
`AGPL-3.0-or-later`; the project is AGPL-only as of 2026-04-14.

## Questions

Open an issue or email <pierre@warnier.net>.
