# Implementation Plan

## Objective

Improve correctness, error propagation, determinism, and testability without changing the public CLI unless the current behavior is unsafe or misleading.

## Planning Principles

- Preserve existing CLI syntax where possible.
- Prefer small, test-backed refactors over broad rewrites.
- Move user-facing logging and exit handling to the CLI boundary.
- Introduce abstractions only when they remove a concrete maintenance or testing problem.

## Delivery Map

| ID | Deliverable | Complexity | Depends On |
| --- | --- | --- | --- |
| D1 | Correctness and UX bug fixes | S | None |
| D2 | End-to-end error propagation | M | D1 |
| D3 | Deterministic tag filtering and limit semantics | M | D2 |
| D4 | Module boundary refactor | L | D2 |
| D5 | Test strategy hardening | M | D1, D2, D3 |
| D6 | Narrow AWS abstraction for command testing | L | D2, D4 |
| D7 | Documentation and release readiness | S | D1-D6 as applicable |

## Deliverables

### D1. Correctness and UX Bug Fixes

**Scope**

- Fix summary output so `Smallest file size` prints the size, not the key.
- Fix `Download::execute` so an existing target file skips that object instead of returning from the whole batch.
- Make bulk delete fail the command when the request fails instead of printing and returning success.
- Align size-filter semantics and help text. Decide whether `+N` and `-N` are strict or inclusive and make code, tests, and docs match.
- Review `SetPublic` URL generation for region and key encoding correctness.

**Acceptance Criteria**

- New unit tests cover summary formatting and batch download skip behavior.
- A failed delete request produces a non-zero process exit.
- Help text, tests, and implementation agree on size filter semantics.

**Estimate**

1-2 days

### D2. End-to-End Error Propagation

**Scope**

- Introduce a crate-level error/result type for the execution pipeline.
- Change `FindCommand::exec` to return `Result<Option<FindStat>>` instead of panicking.
- Change listing and filtering to propagate paginator and command failures instead of truncating output and printing to stderr.
- Centralize `eprintln!` and exit-code mapping in `src/bin/s3find.rs`.
- Define an explicit exit-code policy for:
  - argument/validation errors
  - listing/AWS errors
  - per-command execution errors

**Acceptance Criteria**

- No `unwrap()` remains on the production execution path.
- Listing failure cannot return exit code 0 with partial output.
- Regression tests cover failed pagination and failed command execution.

**Estimate**

2-3 days

### D3. Deterministic Tag Filtering and Limit Semantics

**Scope**

- Preserve traversal order when tag filtering is enabled.
- Make `--limit` apply to logical traversal order, not tag-fetch completion order.
- Decide explicit policy for tag fetch failures:
  - fail-fast for pipeline integrity, or
  - skip with warning and tracked stats
- Separate tag-fetch outcome accounting from filter-match accounting.

**Acceptance Criteria**

- Repeated runs with identical input yield identical ordering.
- `--limit` with mixed tag-fetch latency is deterministic in tests.
- Stats clearly distinguish success, throttled, failed, and excluded objects.

**Estimate**

2-3 days

### D4. Module Boundary Refactor

**Scope**

- Extract a thin application runner from `src/bin/s3find.rs`.
- Split `src/run_command.rs` into command-focused modules:
  - `print`
  - `delete`
  - `transfer`
  - `tag`
  - `acl`
  - `restore`
- Split `src/command.rs` into:
  - stream object model
  - listing/pagination
  - stats
- Remove or isolate disconnected experimental code until production-ready:
  - `retry.rs`
  - incomplete `src/performance/*` tree

**Acceptance Criteria**

- `main` is reduced to parse -> run -> render error/summary.
- Largest modules are materially smaller and easier to review.
- Unsupported experimental code is either integrated, feature-gated, or removed from the active crate surface.

**Estimate**

3-4 days

### D5. Test Strategy Hardening

**Scope**

- Make LocalStack integration tests opt-in or gracefully skipped when no container runtime is available.
- Add focused unit tests for:
  - paginator error propagation
  - subcommand failure propagation
  - deterministic tag filtering with `--limit`
  - delete/download failure handling
- Fix current clippy failures in tests.
- Separate fast and slow test paths in contributor guidance.

**Acceptance Criteria**

- `cargo test --lib` passes without Docker.
- `cargo clippy --all-targets --all-features -- -D warnings` passes.
- Container-based tests still run in CI or through a documented opt-in command.

**Estimate**

1-2 days

### D6. Narrow AWS Abstraction for Command Testing

**Scope**

- Introduce a small trait that wraps only the S3 operations used by commands.
- Keep the AWS SDK implementation in `src/adapters/aws.rs`.
- Add a fake implementation for unit tests covering delete, copy, move, download, restore, and tagging behavior.
- Do not attempt a full storage-provider rewrite in this pass.

**Acceptance Criteria**

- New command tests no longer require SDK replay setup for every path.
- The abstraction surface is intentionally small and command-driven.
- Production wiring still uses the AWS SDK client without behavior change.

**Estimate**

3-4 days

### D7. Documentation and Release Readiness

**Scope**

- Update `README.md` and CLI help text for any behavior changes.
- Document unit vs integration test commands and container prerequisites.
- Add a short migration note for exit-code and error-reporting changes.

**Acceptance Criteria**

- README examples and CLI help match actual behavior.
- Contributors can discover the correct test workflow without reading source.

**Estimate**

0.5-1 day

## Recommended Execution Order

1. D1: remove correctness bugs with minimal blast radius.
2. D2: make failures explicit and stop silent partial success.
3. D3: restore deterministic semantics for tag filtering and `--limit`.
4. D5: lock in the new behavior with stable tests and green clippy.
5. D4: split modules after behavior is stable.
6. D6: add the narrow AWS abstraction where it now supports simpler tests.
7. D7: refresh docs and release notes.

## Milestones

### Milestone 1: Safety Baseline

Includes D1 and D2.

**Exit Gate**

- No panic-based control flow in the production path.
- No silent success on listing, delete, or command failures.

### Milestone 2: Deterministic Pipeline

Includes D3 and D5.

**Exit Gate**

- Tag-filtered output is deterministic.
- Local development checks pass without requiring Docker.

### Milestone 3: Maintainable Structure

Includes D4 and D6.

**Exit Gate**

- Execution path is split into focused modules.
- Command behavior is testable behind a narrow adapter boundary.

### Milestone 4: Release Readiness

Includes D7.

**Exit Gate**

- Docs, examples, and release notes reflect the shipped behavior.

## Non-Goals for This Pass

- No full hexagonal architecture rewrite.
- No provider-agnostic storage platform.
- No major performance program until correctness and determinism are fixed.
