# Bank example (part deux)

## Summary

This bank example program transfers money between accounts, creating
new ledger transactions in the form of a transaction record and two
transaction "legs" per database transaction. Each transfer additionally
queries and updates account balances.

There are two mechanisms for running: high contention and low contention.
Specify -contention={high|low} on the command line to specify which. The
default is low contention.

## Running

Run against an existing cockroach node or cluster.

#### Insecure node or cluster
```
# Launch your node or cluster in insecure mode (with --insecure passed to cockroach).
# Find a reachable address: [mycockroach:26257].
# Run the example with:
./bank2 postgres://root@mycockroach:26257?sslmode=disable
```

#### Secure node or cluster
```
# Launch your node or cluster in secure mode with certificates in [mycertsdir]
# Find a reachable address:[mycockroach:26257].
# Run the example with:
./bank2 "postgres://root@mycockroach:26257?sslcert=mycertsdir/root.client.crt&sslkey=mycertsdir/root.client.key"
```
