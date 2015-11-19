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
	"log"
	"math"
	"os"
	"sync"
	"syscall"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
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

// Default permissions: we don't have any right now.
const defaultPerms = 0755

// Maximum file size.
const maxSize = math.MaxUint64

// Node implements the Node interface.
type Node struct {
	cfs CFS
	// These fields are immutable after node creation.
	ID uint64
	// Used for type only, permissions are ignored.
	Mode os.FileMode

	// Other fields to add:
	// nLinks: number of hard links
	// openFDs: number of open file descriptors
	// fastLink: path for symlink target
	// timestamps (probably just ctime and mtime)

	// Implicit fields:
	// numBlocks: number of 512b blocks
	// blocksize: preferred block size
	// mode bits: permissions

	// For regular files only.
	// Data blocks are addressed by inode number and offset.
	// Any op accessing Size and blocks must lock 'mu'.
	mu   sync.RWMutex
	Size uint64
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
	a.Mode = n.Mode
	// Does preferred block size make sense on things other
	// than regular files?
	a.BlockSize = BlockSize

	if n.Mode.IsRegular() {
		n.mu.RLock()
		defer n.mu.RUnlock()
		a.Size = n.Size

		// Blocks is the number of 512 byte blocks, regardless of
		// filesystem blocksize.
		a.Blocks = (n.Size + 511) / 512
	}
	return nil
}

// Setattr modifies node metadata. This includes changing the size.
func (n *Node) Setattr(_ context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	if !req.Valid.Size() {
		// We can exit early since only setting the size is implemented.
		return nil
	}

	if !n.Mode.IsRegular() {
		// Setting the size is only available on regular files.
		return fuse.Errno(syscall.EINVAL)
	}

	if req.Size > maxSize {
		// Too big.
		return fuse.Errno(syscall.EFBIG)
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Size == n.Size {
		// Nothing to do.
		return nil
	}

	// Wrap everything inside a transaction.
	tx, err := n.cfs.db.Begin()
	if err != nil {
		log.Print(err)
		return err
	}

	// Resize blocks as needed.
	if err := resizeBlocks(tx, n.ID, n.Size, req.Size); err != nil {
		log.Print(err)
		_ = tx.Rollback()
		return err
	}

	// Update this node descriptor. Store the current size in case
	// we need to rollback.
	originalSize := n.Size
	n.Size = req.Size
	if err := updateNode(tx, n); err != nil {
		log.Print(err)
		_ = tx.Rollback()
		// reset our size.
		n.Size = originalSize
		return err
	}

	// All set: commit.
	if err := tx.Commit(); err != nil {
		// Reset our size.
		log.Print(err)
		n.Size = originalSize
		return err
	}
	return nil
}

// Lookup looks up a specific entry in the receiver,
// which must be a directory.  Lookup should return a Node
// corresponding to the entry.  If the name does not exist in
// the directory, Lookup should return ENOENT.
//
// Lookup need not to handle the names "." and "..".
func (n *Node) Lookup(_ context.Context, name string) (fs.Node, error) {
	if !n.Mode.IsDir() {
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
	if !n.Mode.IsDir() {
		return nil, fuse.Errno(syscall.ENOTDIR)
	}
	return n.cfs.list(n.ID)
}

// Mkdir creates a directory in 'n'.
// We let the sql query fail if the directory already exists.
// TODO(marc): better handling of errors.
func (n *Node) Mkdir(_ context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	if !n.Mode.IsDir() {
		return nil, fuse.Errno(syscall.ENOTDIR)
	}
	if !req.Mode.IsDir() {
		return nil, fuse.Errno(syscall.ENOTDIR)
	}

	node := n.cfs.newDirNode()
	err := n.cfs.create(n.ID, req.Name, node)
	if err != nil {
		return nil, err
	}
	return node, nil
}

// Create creates a new file in the receiver directory.
func (n *Node) Create(_ context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (
	fs.Node, fs.Handle, error) {
	if !n.Mode.IsDir() {
		return nil, nil, fuse.Errno(syscall.ENOTDIR)
	}
	if req.Mode.IsDir() {
		return nil, nil, fuse.Errno(syscall.EISDIR)
	} else if !req.Mode.IsRegular() {
		return nil, nil, fuse.Errno(syscall.EINVAL)
	}

	node := n.cfs.newFileNode()
	err := n.cfs.create(n.ID, req.Name, node)
	if err != nil {
		return nil, nil, err
	}
	return node, node, nil
}

// Remove may be unlink or rmdir.
func (n *Node) Remove(_ context.Context, req *fuse.RemoveRequest) error {
	if !n.Mode.IsDir() {
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
	if !n.Mode.IsRegular() {
		return fuse.Errno(syscall.EINVAL)
	}
	if req.Offset < 0 {
		return fuse.Errno(syscall.EINVAL)
	}
	if len(req.Data) == 0 {
		return nil
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	newSize := uint64(req.Offset) + uint64(len(req.Data))
	if newSize > maxSize {
		return fuse.Errno(syscall.EFBIG)
	}

	// Wrap everything inside a transaction.
	tx, err := n.cfs.db.Begin()
	if err != nil {
		log.Print(err)
		return err
	}

	// Update blocks. They will be added as needed.
	if err := write(tx, n.ID, n.Size, uint64(req.Offset), req.Data); err != nil {
		log.Print(err)
		_ = tx.Rollback()
		return err
	}

	originalSize := n.Size
	if newSize > originalSize {
		// This was an append, commit the size change.
		n.Size = newSize
		if err := updateNode(tx, n); err != nil {
			log.Print(err)
			_ = tx.Rollback()
			// reset our size.
			n.Size = originalSize
			return err
		}
	}

	// All set: commit.
	if err := tx.Commit(); err != nil {
		// Reset our size.
		log.Print(err)
		n.Size = originalSize
		return err
	}

	// We always write everything.
	resp.Size = len(req.Data)
	return nil
}

// Read reads data from 'n'.
func (n *Node) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	if !n.Mode.IsRegular() {
		return fuse.Errno(syscall.EINVAL)
	}
	if req.Offset < 0 {
		// Before beginning of file.
		return fuse.Errno(syscall.EINVAL)
	}
	if req.Size == 0 {
		// No bytes requested.
		return nil
	}
	offset := uint64(req.Offset)

	n.mu.RLock()
	defer n.mu.RUnlock()
	if offset >= n.Size {
		// Beyond end of file.
		return nil
	}

	to := min(n.Size, offset+uint64(req.Size))
	if offset == to {
		return nil
	}

	data, err := read(n.cfs.db, n.ID, offset, to)
	if err != nil {
		return err
	}
	resp.Data = data
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
	if !n.Mode.IsDir() || !newNode.Mode.IsDir() {
		return fuse.Errno(syscall.ENOTDIR)
	}
	return n.cfs.rename(n.ID, newNode.ID, req.OldName, req.NewName)
}
