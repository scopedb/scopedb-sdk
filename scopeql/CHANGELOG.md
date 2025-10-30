# CHANGELOG

All significant changes to this software be documented in this file.

## Unreleased

## v0.2.1 (2025-10-30)

### New Features

* Recognize `VACUUM` token to support `VACUUM` command.

### Improvements

* Repl now pretty-prints semi-structure data.

## v0.2.0 (2025-10-21)

### Breaking Changes

* No longer support `-e` option for specifying the entrypoint. Use config files instead.

### New Features

* Support load config from file:
  * Specify config file with `--config-file` option.
  * If not specified, trying to look up from:
    * `$HOME/.scopeql/config.toml`
    * `$HOME/.config/scopeql/config.toml`
    * `${CONFIG_DIR:-$XDG_CONFIG_HOME}/scopeql/config.toml`; see [this page](https://docs.rs/dirs/6.0.0/dirs/fn.config_dir.html) for more details about `config_dir`.
  * Otherwise, fallback to default config.

## v0.1.1 (2025-08-21)

### Developments

* Fix the release workflow to properly build AMD64 image.

## v0.1.0 (2025-08-21)

* Initial release.
