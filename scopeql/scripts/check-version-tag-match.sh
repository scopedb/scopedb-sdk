#!/usr/bin/env bash

if [[ "${GITHUB_REF_TYPE}" != "tag" ]]; then
  echo "GITHUB_REF_TYPE=${GITHUB_REF_TYPE} Not a tag, skipping version check"
  exit 0
fi

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd "$SCRIPT_DIR/.."

VERSION=$( cargo metadata --format-version=1 --no-deps | jq -r '.packages[] | select(.name == "scopeql") | .version' )
echo "VERSION: $VERSION"
echo "GITHUB_REF_NAME: $GITHUB_REF_NAME"
if [[ "$GITHUB_REF_NAME" != "scopeql/v$VERSION" ]]; then
  echo "Version tag does not match the version in Cargo.toml"
  exit 1
fi
