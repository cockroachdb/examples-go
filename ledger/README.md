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

Run the example with `--help` to see all configuration options.

### Prerequisites
```bash
./cockroach start --background       # for CockroachDB
docker run -d -p 5432:5432 postgres  # for Postgres
```

### Examples

For Postgres, change 26257 to 5432 below.

```
go run $GOPATH/src/github.com/cockroachdb/examples-go/ledger/main.go --concurrency 5 --sources 10 --destinations 10 postgres://root@localhost:26257?sslmode=disable
```

This runs a moderately contended example, transferring money between ten random
accounts. You can vary the contention by source and destination:

* no contention: `--sources=0`, `--destinations=0`.
* asymmetric contention: money is transferred from (practically infinitely) many
  accounts to ten destinations accounts: `--sources=0`, `--destinations=10`
* hammering a single account, but only using 100 source accounts:
    `--sources=100`, `--destinations=1`.

The workload is essentially a read for both the source and target account, and
then a write to both the source and target account. Hence, you should expect
some symmetry, but since the source account is accessed first for both the read
and the write, it's not perfectly symmetric.
