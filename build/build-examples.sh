#!/bin/bash
# Build a statically linked Cockroach binary
#
# Author: Peter Mattis (peter@cockroachlabs.com)

set -eux

time make deps
for proj in bank block_writer fakerealtime filesystem photos; do
  time make STATIC=1 ${proj}
  strip -S ${proj}/${proj}
done
