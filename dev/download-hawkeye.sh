#!/usr/bin/env bash

if [[ $(which hawkeye) ]]; then
  echo "Hawkeye is already installed."
  exit 0
fi

if [[ $(which cargo-binstall) ]]; then
  echo "Download hawkeye with cargo-binstall ..."
  cargo binstall hawkeye
else
  cargo install --locked hawkeye
fi
