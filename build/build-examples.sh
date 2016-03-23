#!/bin/bash
# Build a statically linked Cockroach binary
#
# Author: Peter Mattis (peter@cockroachlabs.com)

set -euo pipefail

time make deps
time make STATIC=1 block_writer
time make STATIC=1 photos

# Make sure the created binary is statically linked.  Seems
# awkward to do this programmatically, but this should work.
file block_writer/block_writer | grep -F 'statically linked' > /dev/null

strip -S block_writer/block_writer
