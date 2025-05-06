#!/usr/bin/env bash

HAWKEYE_VERSION="6.0.3"

if [[ $(which hawkeye) ]]; then
  echo "Hawkeye is already installed."
  exit 0
fi

if [[ $(which cargo-binstall) ]]; then
  echo "Download hawkeye with cargo-binstall ..."
  cargo binstall "hawkeye@${HAWKEYE_VERSION}"
else
  curl --proto '=https' --tlsv1.2 -LsSf https://github.com/korandoru/hawkeye/releases/download/v${HAWKEYE_VERSION}/hawkeye-installer.sh | sh
fi
