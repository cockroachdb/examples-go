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

```bash
./cockroach start --background
go run main.go postgres://root@localhost:26257/ledger?sslmode=disable
```

### Postgres

```bash
docker run -d -p 5432:5432 postgres
# When not on OSX, use 'localhost' instead
go run main.go postgres://postgres@$(docker-machine ip default):5432?sslmode=disable
```
