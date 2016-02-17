# Filesystem example

## Summary

This is a fuse filesystem using cockroach as a backing store.
The implemented features attempt to be posix compliant.
See `main.go` for more details, including implemented features and caveats.

## Running

Run against an existing cockroach node or cluster.

#### Development node
```
# Build cockroach binary from https://github.com/cockroachdb/cockroach
# Start it in insecure mode (listens on localhost:15432)
./cockroach start --insecure

# Build filesystem example.
# Start it with:
mkdir /tmp/foo
./filesystem postgresql://root@localhost:15432/?sslmode=disable /tmp/foo
# <CTRL-C> to umount and quit
# Use /tmp/foo as a filesystem.
```

#### Insecure node or cluster
```
# Launch your node or cluster in insecure mode (with --insecure passed to cockroach).
# Find a reachable address: [mycockroach:15432].
# Run the example with:
mkdir /tmp/foo
./filesystem postgresql://root@mycockroach:15432/?sslmode=disable /tmp/foo
# <CTRL-C> to umount and quit
# Use /tmp/foo as a filesystem.
```

#### Secure node or cluster
```
# Launch your node or cluster in secure mode with certificates in [mycertsdir]
# Find a reachable address:[mycockroach:15432].
# Run the example with:
mkdir /tmp/foo
./filesystem postgresqls://root@mycockroach:15432/?certs=verify-ca&sslcert=mycertsdir/root.client.crt&sslkey=mycertsdir/root.client.key&sslrootcert=mycertsdir/ca.crt /tmp/foo
# <CTRL-C> to umount and quit
# Use /tmp/foo as a filesystem.
```
