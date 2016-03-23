#!/bin/bash
# Build a statically linked Cockroach binary
#
# Author: Peter Mattis (peter@cockroachlabs.com)

set -eux

time make deps
time make STATIC=1 block_writer
time make STATIC=1 photos

strip -S block_writer/block_writer
strip -S photos/photos
