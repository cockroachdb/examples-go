#!/usr/bin/env bash

set -euxo pipefail

docker run \
	--workdir=/go/src/github.com/cockroachdb/examples-go \
	--volume="${GOPATH%%:*}/src":/go/src \
	--volume="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)":/go/src/github.com/cockroachdb/examples-go \
	--rm \
	cockroachdb/builder:20170422-212842 make deps test | go-test-teamcity
