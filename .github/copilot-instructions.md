# Instructions for automated agents (Copilot coding agent, Codex, any bot)

This crate follows a strict quality bar. A PR that ignores it will be closed.

## Gates that must pass (locally before pushing, and in CI)

```
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
cargo test --no-default-features
cargo doc --no-deps
cargo deny check
```

Coverage must stay at or above 95 % lines (`cargo llvm-cov --fail-under-lines 95`).

## Rules

- **No `unwrap`/`expect`/`panic!` in library code.** Doctest examples may unwrap; `src/` may not.
  `#![forbid(unsafe_code)]` and `#![deny(missing_docs)]` stay.
- **Doctest on every public item**, and a runnable example for every new public type or method.
- **Tests prove, they do not sample.** Every fix needs: the valid value, the value just inside the
  limit, and the value just outside it. Property tests (`proptest`) for pure functions, a fuzz target
  for parsers.
- **Permissive decoding**: unknown fields ignored, unknown enum variants carried, both snake_case and
  camelCase accepted. Strictness belongs in security checks, not in deserialisation.
- **Typed errors** (`thiserror`), `#[non_exhaustive]`, source chains, retryable vs terminal explicit.
  Never auto-retry a non-idempotent request after a 5xx or a timeout - that duplicates work; such an
  outcome belongs in an `Ambiguous` error.
- **Thread safety**: clients take `&self` and stay `Send + Sync`; no `static mut`, no `Rc`/`RefCell`
  in a public type, no lock held across `.await`.
- **Security**: constant-time comparison for secrets and signatures (`subtle`), fail closed on an
  empty secret, never log credentials, validate sizes before allocating.
- **Reuse before build**: if a maintained crate already does it (`aws_lambda_events`,
  `lambda_runtime`, `base64`, `time`, `subtle`, ...), use it instead of writing your own.

## What an agent must NOT do

- Do not bump the crate version, do not create a git tag, do not run `cargo publish`.
- Do not edit `.github/workflows/` (release and secret handling is the maintainer's).
- Do not weaken or `#[allow]` a failing gate, do not add an advisory ignore to `deny.toml`.
- Do not rewrite public API signatures for style - fix the reported issue and nothing else.
- One issue per PR. No drive-by refactors.

## Writing

Issues, PR descriptions, commit subjects and code comments are written in **English**.
