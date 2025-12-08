#!/usr/bin/env bash

set -euo pipefail

df -h

to_delete=(
  "/lib/firefox"
  "/lib/google-cloud-sdk"
  "/lib/jvm"
  "/lib/llvm-16"
  "/lib/llvm-17"
  "/opt/az"
  "/opt/google"
  "/opt/hostedtoolcache/CodeQL"
  "/opt/hostedtoolcache/PyPy"
  "/opt/hostedtoolcache/Ruby"
  "/opt/hostedtoolcache/go"
  "/opt/hostedtoolcache/node"
  "/opt/microsoft"
  "/opt/pipx"
  "/usr/share/swift"
)

for d in "${to_delete[@]}"; do
  echo "delete $d"

  if [ ! -d "$d" ]; then
    echo "directory does NOT exist"
    exit 1
  fi

  if [ -L "$d" ]; then
    echo "directory is a symlink, but we should delete the original source"
    exit 1
  fi

  sudo rm -rf "$d"
done

df -h
