# Fake Real Time example

## Summary

This example uses a log-style table in an approximation of the
"fake real time" system used at Friendfeed. Two tables are used: a
`messages` table stores the complete data for all messages
organized by channel, and a global `updates` table stores metadata
about recently-updated channels.

## Running

Run against an existing cockroach node or cluster.

#### Insecure node or cluster
```
# Launch your node or cluster in insecure mode (with --insecure passed to cockroach).
# Find a reachable address: [mycockroach:26257].
# Run the example with:
./fakerealtime postgres://root@mycockroach:26257?sslmode=disable
```

#### Secure node or cluster
```
# Launch your node or cluster in secure mode with certificates in [mycertsdir]
# Find a reachable address:[mycockroach:26257].
# Run the example with:
./fakerealtime "postgres://root@mycockroach:26257?sslcert=mycertsdir/root.client.crt&sslkey=mycertsdir/root.client.key"
```
