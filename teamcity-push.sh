#!/usr/bin/env bash

set -euxo pipefail

VERSION=$(git describe || git rev-parse --short HEAD)

# Don't do this in Docker to avoid creating root-owned directories in GOPATH.
make deps

echo "Deploying ${VERSION}..."
aws configure set region us-east-1

BUCKET_NAME=cockroach
LATEST_SUFFIX=.LATEST
REPO_NAME=examples-go
SHA=$(git rev-parse HEAD)

# push_one_binary takes the path to the binary inside the repo.
# eg: push_one_binary sql/sql.test
# The file will be pushed to: s3://BUCKET_NAME/REPO_NAME/sql.test.SHA
# The binary's sha will be stored in s3://BUCKET_NAME/REPO_NAME/sql.test.LATEST
# The .LATEST file will also redirect to the latest binary when fetching through
# the S3 static-website.
function push_one_binary {
  rel_path=$1
  binary_name=$(basename "$1")

  time aws s3 cp "${rel_path}" s3://${BUCKET_NAME}/${REPO_NAME}/"${binary_name}"."${SHA}"

  # Upload LATEST file.
  tmpfile=$(mktemp /tmp/cockroach-push.XXXXXX)
  echo "${SHA}" > "${tmpfile}"
  time aws s3 cp --website-redirect /${REPO_NAME}/"${binary_name}"."${SHA}" "${tmpfile}" s3://${BUCKET_NAME}/${REPO_NAME}/"${binary_name}"${LATEST_SUFFIX}
  rm -f "${tmpfile}"
}

for proj in bank ledger block_writer fakerealtime filesystem photos; do
	docker run \
		--workdir=/go/src/github.com/cockroachdb/examples-go \
		--volume="${GOPATH%%:*}/src":/go/src \
		--volume="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)":/go/src/github.com/cockroachdb/examples-go \
		--rm \
		cockroachdb/builder:20170422-212842 make ${proj} STATIC=1
	push_one_binary ${proj}/${proj}
done
