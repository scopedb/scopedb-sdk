# Contribute to ScopeDB Go SDK

Welcome! Thanks for your help improving the project.

This guide documents essential information for setting up the environment, most frequently used commands, and understanding the project structure.

## Most frequently used commands

### Update test snapshots

The Go SDK uses [go-snaps](https://github.com/gkampitakis/go-snaps) for snapshot testing.

When the snapshot assertions are expected to be updated, run the tests with `UPDATE_SNAPS=true`:

```shell
UPDATE_SNAPS=true go test ./...
```
