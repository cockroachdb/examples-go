# Block writer example

## Summary

The block writer example program is a write-only workload intended to insert
a large amount of data into cockroach quickly. This example is intended to
trigger range splits and rebalances.

## Running

Run against an existing cockroach node or cluster.

#### Insecure node or cluster
```
# Launch your node or cluster in insecure mode (with --insecure passed to cockroach).
# Find a reachable address: [mycockroach:26257].
# Run the example with:
./block_writer postgres://root@mycockroach:26257?sslmode=disable
```

#### Secure node or cluster
```
# Launch your node or cluster in secure mode with certificates in [mycertsdir]
# Find a reachable address:[mycockroach:26257].
# Run the example with:
./block_writer "postgres://root@mycockroach:26257?sslcert=mycertsdir/root.client.crt&sslkey=mycertsdir/root.client.key"
```
