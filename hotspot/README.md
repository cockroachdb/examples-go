# Hotspot example

## Summary

The hotspot example program is a read/write workload intended to always hit
the exact same value. It performs reads and writes to simulate a super
contentious load.

## Running

Run against an existing cockroach node or cluster.

#### Insecure node or cluster
```
# Launch your node or cluster in insecure mode (with --insecure passed to cockroach).
# Find a reachable address: [mycockroach:26257].
# Run the example with:
./hotspot postgres://root@mycockroach:26257?sslmode=disable
```

#### Secure node or cluster
```
# Launch your node or cluster in secure mode with certificates in [mycertsdir]
# Find a reachable address:[mycockroach:26257].
# Run the example with:
./hotspot "postgres://root@mycockroach:26257?sslcert=mycertsdir/root.client.crt&sslkey=mycertsdir/root.client.key"
```
