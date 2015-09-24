// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Marc Berhault (marc@cockroachlabs.com)

package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"syscall"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"bazil.org/fuse/fuseutil"
	"golang.org/x/net/context"
)

var _ fs.Node = &Node{}               // Attr
var _ fs.NodeSetattrer = &Node{}      // Setattr
var _ fs.NodeStringLookuper = &Node{} // Lookup
var _ fs.HandleReadDirAller = &Node{} // HandleReadDirAller
var _ fs.NodeMkdirer = &Node{}        // Mkdir
var _ fs.NodeCreater = &Node{}        // Create
var _ fs.NodeRemover = &Node{}        // Remove
var _ fs.HandleWriter = &Node{}       // Write
var _ fs.HandleReader = &Node{}       // Read
var _ fs.NodeFsyncer = &Node{}        // Fsync
var _ fs.NodeRenamer = &Node{}        // Rename

// TODO(marc): let's be conservative here. We can try bigger later.
const maxSize = 1 << 20 // 1MB.

// Node implements the Node interface.
type Node struct {
	cfs CFS
	// These fields are immutable after node creation.
	Name string
	ID   uint64
	// TODO(marc): switch to enum for other types.
	IsDir bool

	// For files only.
	mu   sync.RWMutex
	Data []byte
}

// toJSON returns the json-encoded string for this node.
func (n *Node) toJSON() string {
	ret, err := json.Marshal(n)
	if err != nil {
		panic(err)
	}
	return string(ret)
}

// Attr fills attr with the standard metadata for the node.
func (n *Node) Attr(_ context.Context, a *fuse.Attr) error {
	a.Inode = n.ID
	if n.IsDir {
		a.Mode = os.ModeDir | 0755
	} else {
		n.mu.RLock()
		defer n.mu.RUnlock()
		a.Mode = 0644
		a.Size = uint64(len(n.Data))
	}
	return nil
}

// Setattr modifies node metadata. This includes changing the size.
func (n *Node) Setattr(_ context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Valid.Size() {
		if n.IsDir {
			return fuse.Errno(syscall.EISDIR)
		}
		if req.Size > maxSize {
			return fuse.Errno(syscall.EFBIG)
		}
		l := uint64(len(n.Data))
		if req.Size > l {
			n.Data = append(n.Data, make([]byte, req.Size-l)...)
		} else {
			n.Data = n.Data[:req.Size]
		}
	} else {
		// TODO(marc): handle other attributes.
		return nil
	}

	// TODO(marc): check that fuse forgets the node on errors, otherwise
	// we'll have updated data in memory but not committed it.
	return n.cfs.update(n)
}

// Lookup looks up a specific entry in the receiver,
// which must be a directory.  Lookup should return a Node
// corresponding to the entry.  If the name does not exist in
// the directory, Lookup should return ENOENT.
//
// Lookup need not to handle the names "." and "..".
func (n *Node) Lookup(_ context.Context, name string) (fs.Node, error) {
	if !n.IsDir {
		return nil, fuse.Errno(syscall.ENOTDIR)
	}
	node, err := n.cfs.lookup(n.ID, name)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fuse.ENOENT
		}
		return nil, err
	}
	node.cfs = n.cfs
	return node, nil
}

// ReadDirAll returns the list of child inodes.
func (n *Node) ReadDirAll(_ context.Context) ([]fuse.Dirent, error) {
	if !n.IsDir {
		return nil, fuse.Errno(syscall.ENOTDIR)
	}
	return n.cfs.list(n.ID)
}

// Mkdir creates a directory in 'n'.
// We let the sql query fail if the directory already exists.
// TODO(marc): better handling of errors.
func (n *Node) Mkdir(_ context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	if !n.IsDir {
		return nil, fuse.Errno(syscall.ENOTDIR)
	}
	if !req.Mode.IsDir() {
		return nil, fuse.Errno(syscall.ENOTDIR)
	}

	node := &Node{
		cfs:   n.cfs,
		ID:    n.cfs.newUniqueID(),
		Name:  req.Name,
		IsDir: true,
	}

	err := n.cfs.create(n.ID, node)
	if err != nil {
		return nil, err
	}
	return node, nil
}

// Create creates a new file in the receiver directory.
func (n *Node) Create(_ context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (
	fs.Node, fs.Handle, error) {
	if !n.IsDir {
		return nil, nil, fuse.Errno(syscall.ENOTDIR)
	}
	if req.Mode.IsDir() {
		return nil, nil, fuse.Errno(syscall.EISDIR)
	}

	node := &Node{
		cfs:   n.cfs,
		ID:    n.cfs.newUniqueID(),
		Name:  req.Name,
		IsDir: false,
	}

	err := n.cfs.create(n.ID, node)
	if err != nil {
		return nil, nil, err
	}
	return node, node, nil
}

// Remove may be unlink or rmdir.
func (n *Node) Remove(_ context.Context, req *fuse.RemoveRequest) error {
	if !n.IsDir {
		return fuse.Errno(syscall.ENOTDIR)
	}

	if req.Dir {
		// Rmdir.
		return n.cfs.remove(n.ID, req.Name, true /* checkChildren */)
	}
	// Unlink.
	return n.cfs.remove(n.ID, req.Name, false /* !checkChildren */)
}

// Write writes data to 'n'. It may overwrite existing data, or grow it.
func (n *Node) Write(_ context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	if n.IsDir {
		return fuse.Errno(syscall.EISDIR)
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	newLen := uint64(req.Offset) + uint64(len(req.Data))
	if newLen > maxSize {
		return fuse.Errno(syscall.EFBIG)
	}

	l := uint64(len(n.Data))
	if newLen > l {
		n.Data = append(n.Data, make([]byte, newLen-l)...)
	}

	written := copy(n.Data[req.Offset:], req.Data)
	// TODO(marc): check that fuse forgets the node on errors, otherwise
	// we'll have updated data in memory but not committed it.
	if err := n.cfs.update(n); err != nil {
		return err
	}
	resp.Size = written
	return nil
}

// Read reads data from 'n'.
func (n *Node) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	if n.IsDir {
		return fuse.Errno(syscall.EISDIR)
	}

	n.mu.RLock()
	defer n.mu.RUnlock()

	fuseutil.HandleRead(req, resp, n.Data)
	return nil
}

// Fsync is a noop for us, we always push writes to the DB. We do need to implement it though.
func (n *Node) Fsync(_ context.Context, _ *fuse.FsyncRequest) error {
	return nil
}

// Rename renames 'req.OldName' to 'req.NewName', optionally moving it to 'newDir'.
// If req.NewName exists, it is deleted. It is assumed that it cannot be a directory.
// NOTE: we do not keep track of opens, so we delete existing destinations right away.
// This means that anyone holding an open file descriptor on the destination will fail
// when trying to use it.
// To properly handle this, we need to count references (including inode -> inode refs,
// and open handles) and delete the inode only when it reaches zero.
func (n *Node) Rename(ctx context.Context, req *fuse.RenameRequest, newDir fs.Node) error {
	newNode, ok := newDir.(*Node)
	if !ok {
		return fmt.Errorf("newDir is not a Node: %v", newDir)
	}
	if !n.IsDir || !newNode.IsDir {
		return fuse.Errno(syscall.ENOTDIR)
	}
	return n.cfs.rename(n.ID, newNode.ID, req.OldName, req.NewName)
}
