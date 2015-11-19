# Bank example

## Summary

The bank example program continuously performs balance transfers between
accounts using concurrent transactions.

## Running

Run against an existing cockroach node or cluster.

#### Development node
```
# Build cockroach binary from https://github.com/cockroachdb/cockroach
# Start it in dev mode (listens on localhost:26257)
./cockroach start --dev

# Build sql_bank example.
# Start it with:
./sql_bank http://localhost:26257
```

#### Insecure node or cluster
```
# Launch your node or cluster in insecure mode (with --insecure passed to cockroach).
# Find a reachable address: [mycockroach:26257].
# Run the example with:
./sql_bank http://mycockroach:26257
```

#### Secure node or cluster
```
# Launch your node or cluster in secure mode with certificates in [mycertsdir]
# Find a reachable address:[mycockroach:26257].
# Run the example with:
./sql_bank https://mycockroach:26257/?certs=mycertsdir
```
