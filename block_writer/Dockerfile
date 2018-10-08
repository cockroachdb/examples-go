FROM golang:1.11.1

COPY *.go src/github.com/cockroachdb/examples-go/block_writer/

RUN \
  go get github.com/cockroachdb/examples-go/block_writer && \
  go install github.com/cockroachdb/examples-go/block_writer && \
  rm -rf $GOPATH/src

ENTRYPOINT ["/go/bin/block_writer"]
