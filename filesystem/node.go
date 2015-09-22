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
	"encoding/json"
	"os"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"golang.org/x/net/context"
)

// Attr
var _ = fs.Node(&Node{})

// Lookup
var _ = fs.NodeStringLookuper(&Node{})

// Node implements the Node interface.
type Node struct {
	fs   CFS `json:"-"`
	name string
	id   uint64
	// TODO(marc): switch to enum for other types.
	isDir bool

	// For files only
	data []byte
}

// Attr fills attr with the standard metadata for the node.
func (n Node) Attr(ctx context.Context, a *fuse.Attr) error {
	if n.isDir {
		a.Mode = os.ModeDir | 0755
	} else {
		a.Mode = 0644
		a.Size = uint64(len(n.data))
	}
	return nil
}

// Lookup looks up a specific entry in the receiver,
// which must be a directory.  Lookup should return a Node
// corresponding to the entry.  If the name does not exist in
// the directory, Lookup should return ENOENT.
//
// Lookup need not to handle the names "." and "..".
func (n Node) Lookup(ctx context.Context, name string) (fs.Node, error) {
	if !n.isDir {
		return nil, fuse.ENOSYS
	}
	raw, err := n.fs.lookup(n.id, name)
	if err != nil {
		// TODO(marc): handle missing.
		return nil, err
	}
	node := &Node{}
	if err := json.Unmarshal([]byte(raw), node); err != nil {
		// TODO(marc): this defaults to EIO.
		return nil, err
	}
	node.fs = n.fs
	return node, nil
}

// ReadDirAll returns the list of child inodes.
func (Node) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	return nil, fuse.ENOSYS
}
