# Releasing the ScopeDB Go SDK

The Go module lives in the repository's `go/` subdirectory, so release tags use
the `go/vX.Y.Z` form. The examples below use `go/v0.6.0`.

## Prepare and verify

From the repository root:

```sh
cd go
go fmt ./...
go mod tidy
git diff --exit-code -- go.mod go.sum
go test ./...
go test -race ./...
go vet ./...
go doc .
```

Review the public surface and release material:

```sh
go list -f '{{range .GoFiles}}{{$.Dir}}/{{.}}{{"\n"}}{{end}}' .
git diff --check
git diff -- README.md doc.go CHANGELOG.md RELEASE.md examples
rg -n 'https?://|SCOPEDB_' README.md doc.go CHANGELOG.md RELEASE.md examples
```

Confirm that `CHANGELOG.md` contains the intended release date, the examples
compile, no secret or private endpoint is present, and the final diff contains
only intentional changes. Commit and merge the release preparation before
tagging.

## Tag after acknowledgement

Immediately before publishing, obtain the required explicit acknowledgement.
Then, from the repository root, create the annotated submodule tag and push
only that tag:

```sh
version=v0.6.0
git tag -a "go/$version" -m "Release $version for Go SDK"
git push origin "go/$version"
```

This runbook documents the commands; preparing a release does not authorize
running the tag or push commands.

## Verify the published module

After the module proxy has observed the tag, verify it from a fresh temporary
module:

```sh
release_tmp=$(mktemp -d)
cd "$release_tmp"
go mod init example.com/scopedb-release-check
go get github.com/scopedb/scopedb-sdk/go@v0.6.0
go list -m github.com/scopedb/scopedb-sdk/go
go doc github.com/scopedb/scopedb-sdk/go
```

References:

* [Mapping versions to commits](https://go.dev/ref/mod#vcs-version)
* [Module version numbering](https://go.dev/doc/modules/version-numbers)
