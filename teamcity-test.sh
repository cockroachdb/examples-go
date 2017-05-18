#!/usr/bin/env bash

set -euxo pipefail

docker run \
	--workdir=/go/src/github.com/cockroachdb/examples-go \
	--volume="$(dirname "${0}")":/go/src/github.com/cockroachdb/examples-go \
	--rm \
	cockroachdb/builder:20170422-212842 make deps build test | go-test-teamcity
