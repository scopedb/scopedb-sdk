# How to release ScopeDB Go SDK

Before tagging a release, update `CHANGELOG.md` and any release-specific
documentation such as `README.md`.

Then tag the release and push the Git tag to the repository:

```shell
export version=v0.1.0
git tag -a go/$version -m "Release $version for Go SDK"
git push origin go/$version
```

References:

* [Mapping versions to commits](https://go.dev/ref/mod#vcs-version) especially the paragraph "If a module is defined in a subdirectory within the repository, ..."
* [Module version numbering](https://go.dev/doc/modules/version-numbers) especially the paragraph "Major version" when deciding a v1 or v2 release.
