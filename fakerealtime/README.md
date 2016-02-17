# Fake Real Time example

## Summary

This example uses a log-style table in an approximation of the
"fake real time" system used at Friendfeed. Two tables are used: a
`messages` table stores the complete data for all messages
organized by channel, and a global `updates` table stores metadata
about recently-updated channels.

## Running

Run against an existing cockroach node or cluster.

#### Development node
```
# Build cockroach binary from https://github.com/cockroachdb/cockroach
# Start it in insecure mode (listens on localhost:26257)
./cockroach start --insecure

# Build fakerealtime example.
# Start it with:
./fakerealtime http://localhost:26257
```

#### Insecure node or cluster
```
# Launch your node or cluster in insecure mode (with --insecure passed to cockroach).
# Find a reachable address: [mycockroach:26257].
# Run the example with:
./fakerealtime http://mycockroach:26257
```

#### Secure node or cluster
```
# Launch your node or cluster in secure mode with certificates in [mycertsdir]
# Find a reachable address:[mycockroach:26257].
# Run the example with:
./fakerealtime https://mycockroach:26257/?certs=mycertsdir
```
