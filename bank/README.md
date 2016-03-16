# Bank example

## Summary

The bank example program continuously performs balance transfers between
accounts using concurrent transactions.

## Running

Run against an existing cockroach node or cluster.

#### Insecure node or cluster
```
# Launch your node or cluster in insecure mode (with --insecure passed to cockroach).
# Find a reachable address: [mycockroach:26257].
# Run the example with:
./sql_bank postgres://root@mycockroach:26257?sslmode=disable
```

#### Secure node or cluster
```
# Launch your node or cluster in secure mode with certificates in [mycertsdir]
# Find a reachable address:[mycockroach:26257].
# Run the example with:
./sql_bank "postgres://root@mycockroach:26257?sslcert=mycertsdir/root.client.crt&sslkey=mycertsdir/root.client.key"
```
