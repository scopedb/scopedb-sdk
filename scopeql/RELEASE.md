# How to release ScopeQL Binary

It should be as simple as tagging the release and pushing the Git tag to the repository:

```shell
export version=v0.1.0
git tag -a scopeql/$version -m "Release $version for ScopeQL Binary"
git push origin scopeql/$version
```
