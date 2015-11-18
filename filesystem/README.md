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
# Start it in dev mode (listens on localhost:26257)
./cockroach start --dev

# Build filesystem example.
# Start it with:
mkdir /tmp/foo
./filesystem http://localhost:26257 /tmp/foo
# <CTRL-C> to umount and quit
# Use /tmp/foo as a filesystem.
```

#### Insecure node or cluster
```
# Launch your node or cluster in insecure mode (with --insecure passed to cockroach).
# Find a reachable address: [mycockroach:26257].
# Run the example with:
mkdir /tmp/foo
./filesystem http://mycockroach:26257 /tmp/foo
# <CTRL-C> to umount and quit
# Use /tmp/foo as a filesystem.
```

#### Secure node or cluster
```
# Launch your node or cluster in secure mode with certificates in [mycertsdir]
# Find a reachable address:[mycockroach:26257].
# Run the example with:
mkdir /tmp/foo
./filesystem https://mycockroach:26257/?certs=mycertsdir /tmp/foo
# <CTRL-C> to umount and quit
# Use /tmp/foo as a filesystem.
```
