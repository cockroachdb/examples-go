#!/usr/bin/env bash

set -euxo pipefail

docker run \
	--workdir=/go/src/github.com/cockroachdb/examples-go \
	--volume="${GOPATH%%:*}/src":/go/src \
	--rm \
	cockroachdb/builder:20170422-212842 make deps build test | go-test-teamcity
