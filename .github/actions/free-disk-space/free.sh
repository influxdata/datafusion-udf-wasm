#!/usr/bin/env bash

set -euo pipefail

df -h

to_delete=(
  "/opt/az"
  "/opt/google"
  "/opt/hostedtoolcache/CodeQL"
  "/opt/hostedtoolcache/PyPy"
  "/opt/hostedtoolcache/Ruby"
  "/opt/hostedtoolcache/go"
  "/opt/hostedtoolcache/node"
  "/opt/microsoft"
  "/opt/pipx"
  "/usr/lib/firefox"
  "/usr/lib/google-cloud-sdk"
  "/usr/lib/jvm"
  "/usr/lib/llvm-16"
  "/usr/lib/llvm-17"
  "/usr/local/.ghcup"
  "/usr/local/lib/android"
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

  r="$(realpath "$d")"
  if [ "$d" != "$r" ]; then
    echo "not a canonical path, use this instead: $r"
    exit 1
  fi

  sudo rm -rf "$d"
done

df -h
