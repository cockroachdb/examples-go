#!/bin/bash

set -euo pipefail

builder=$(dirname $0)/builder.sh
echo "make test"
time ${builder} make test
