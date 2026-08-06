# How to release the ScopeDB Rust SDK

Rust SDK releases are published from a clean `main` checkout after the release
PR has merged. The crate version, changelog entry, Git tag, and crates.io
version must all match.

## Prepare

1. Set the SDK version without a leading `v`, then confirm that exact version is
   not already present on crates.io:

   ```sh
   export scopedb_rust_version=0.3.0
   if cargo info "scopedb-client@$scopedb_rust_version" >/dev/null 2>&1; then
     echo "scopedb-client $scopedb_rust_version already exists" >&2
     exit 1
   fi
   cargo owner --list scopedb-client
   ```

2. Update `Cargo.toml`, `CHANGELOG.md`, and user-facing documentation in a
   release PR. The crate is a library, so its generated `Cargo.lock` remains
   ignored. Do not publish directly from a feature branch.

3. After the release PR merges, update local `main`, fetch the remote, and
   verify that the checkout is clean and points at exactly `origin/main`:

   ```sh
   git switch main
   git pull --ff-only
   git fetch origin
   test "$(git branch --show-current)" = main
   test -z "$(git status --porcelain)"
   test "$(git rev-parse HEAD)" = "$(git rev-parse origin/main)"
   export scopedb_rust_version=0.3.0
   if cargo info "scopedb-client@$scopedb_rust_version" >/dev/null 2>&1; then
     echo "scopedb-client $scopedb_rust_version already exists" >&2
     exit 1
   fi
   ```

## Validate and publish

Run the same checks used by CI, build the documentation with warnings denied,
and verify the exact package that Cargo will upload:

```sh
cd rust
cargo +1.91.0 generate-lockfile
manifest_version="$(cargo pkgid | sed -E 's/.*@([^@]+)$/\1/')"
test "$manifest_version" = "$scopedb_rust_version"
cargo +nightly fmt --all --check
cargo +nightly clippy --locked --tests --all-targets --all-features -- -D warnings
cargo +1.91.0 test --locked --all-targets --all-features
RUSTDOCFLAGS="-D warnings" cargo +1.91.0 doc --locked --no-deps --all-features
cargo +1.91.0 publish --dry-run --locked
cargo +1.91.0 package --list --locked
```

Publish once from that same clean commit:

```sh
cargo +1.91.0 publish --locked
```

Wait until the published version is visible before tagging:

```sh
cargo info "scopedb-client@$scopedb_rust_version"
```

## Tag and verify

Create an annotated, SDK-scoped tag on the published commit and push it:

```sh
cd ..
git tag -a "rust/v$scopedb_rust_version" \
  -m "Release v$scopedb_rust_version for Rust SDK"
git push origin "rust/v$scopedb_rust_version"
```

Create a GitHub release whose notes are the matching `CHANGELOG.md` entry.
Then verify the crate page, the docs.rs build, and a fresh consumer project that
depends on the published version.
