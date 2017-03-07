# Ledger example

## Summary

Simulate a ledger and a certain type of workload against it.
A general ledger is a complete record of financial transactions over the life
of a bank (or other company).
The example here aims to model a bank in a more realistic setting than our
previous bank example(s) do, and tickles contention issues (causing complete
deadlock in the more contended modes) which will be interesting to investigate.

## Running

See the bank example for more detailed information.
The example may be run both against Postgres and Cockroach.

### Cockroach

Run the example with `--help` to see all configuration options.

```bash
./cockroach start --background       # for CockroachDB
docker run -d -p 5432:5432 postgres  # for Postgres

# For Postgres, change 26257 to 5432 below.
go run $GOPATH/src/github.com/cockroachdb/examples-go/ledger/main.go --concurrency 5 --generator few-few postgres://root@localhost:26257?sslmode=disable
```
