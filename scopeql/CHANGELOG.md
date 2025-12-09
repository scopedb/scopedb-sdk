# CHANGELOG

All significant changes to this software be documented in this file.

## Unreleased

### Breaking Changes

* `scopeql -c < script.sql` no longer supported, use `scopeql -f script.sql` instead.
* `scopeql -c -` no longer supported, use `-f <filename>` and `-c <statement>` mixed instead.
* `-q/--quiet` and `--config-file` options should follow the last subcommand.
  * `scopeql -q`: OK
  * `scopeql load -q ...`: OK
  * `scopeql -q load ...`: NOT OK

### New Features

* Support `scopeql -f <filename>` to run script from file.

## v0.2.2 (2025-12-08)

### New Features

* Support `scopeql -c < script.sql` to run script from file.
* Support `scopeql load -f <file> -t <transform>` command to load data from file.
* Support `ANALYZE` keyword so that `EXPLAIN ANALYZE <query>` works.

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
