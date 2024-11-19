# How to release ScopeDB SDKs

## Release the Go SDK

It should be as simple as tagging the release and pushing the Git tag to the repository:

```shell
git tag -a go/v0.1.0 -m "Release v0.1.0 for Go SDK"
git push origin go/v0.1.0
```

References:

* [Mapping versions to commits](https://go.dev/ref/mod#vcs-version) especially the paragraph "If a module is defined in a subdirectory within the repository, ..."
* [Module version numbering](https://go.dev/doc/modules/version-numbers) especially the paragraph "Major version" when deciding a v1 or v2 release.
