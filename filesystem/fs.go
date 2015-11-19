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
	"os"
	"syscall"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)

const rootNodeID = 1

const (
	fsSchema = `
CREATE DATABASE IF NOT EXISTS fs;

CREATE TABLE IF NOT EXISTS fs.namespace (
  parentID INT,
  name     STRING,
  id       INT,
  PRIMARY KEY (parentID, name)
);

CREATE TABLE IF NOT EXISTS fs.inode (
  id    INT PRIMARY KEY,
  inode STRING
);

CREATE TABLE IF NOT EXISTS fs.block (
  id    INT,
  block INT,
  data  BYTES,
  PRIMARY KEY (id, block)
);
`
)

var _ fs.FS = &CFS{}               // Root
var _ fs.FSInodeGenerator = &CFS{} // GenerateInode

// CFS implements a filesystem on top of cockroach.
type CFS struct {
	db *sql.DB
}

func initSchema(db *sql.DB) error {
	_, err := db.Exec(fsSchema)
	return err
}

// create inserts a new node.
// parentID: inode ID of the parent directory.
// name: name of the new node
// node: new node
func (cfs CFS) create(parentID uint64, name string, node *Node) error {
	inode := node.toJSON()
	tx, err := cfs.db.Begin()
	if err != nil {
		return err
	}
	const sql = `
INSERT INTO fs.inode VALUES ($1, $2);
INSERT INTO fs.namespace VALUES ($3, $4, $1);
`
	if _, err := tx.Exec(sql, node.ID, inode, parentID, name); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// remove removes a node give its name and its parent ID.
// If 'checkChildren' is true, fails if the node has children.
func (cfs CFS) remove(parentID uint64, name string, checkChildren bool) error {
	tx, err := cfs.db.Begin()
	if err != nil {
		return err
	}

	// Start by looking up the node ID.
	const lookupSQL = `
SELECT id FROM fs.namespace WHERE (parentID, name) = ($1, $2)`

	var id uint64
	if err := tx.QueryRow(lookupSQL, parentID, name).Scan(&id); err != nil {
		_ = tx.Rollback()
		return err
	}

	// Check if there are any children.
	if checkChildren {
		if err := checkIsEmpty(tx, id); err != nil {
			_ = tx.Rollback()
			return err
		}
	}

	// Delete all entries.
	const sql = `
DELETE FROM fs.namespace WHERE (parentID, name) = ($1, $2);
DELETE FROM fs.inode WHERE id = $3;
DELETE FROM fs.block WHERE id = $3;
`
	if _, err := tx.Exec(sql, parentID, name, id); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (cfs CFS) lookup(parentID uint64, name string) (*Node, error) {
	return getInode(cfs.db, parentID, name)
}

// list returns the children of the node with id 'parentID'.
// Dirent consists of:
// Inode uint64
// Type DirentType (optional)
// Name string
// TODO(pmattis): lookup all inodes and fill in the type, this will save a Getattr().
func (cfs CFS) list(parentID uint64) ([]fuse.Dirent, error) {
	rows, err := cfs.db.Query(`SELECT name, id FROM fs.namespace WHERE parentID = $1`, parentID)
	if err != nil {
		return nil, err
	}

	var results []fuse.Dirent
	for rows.Next() {
		dirent := fuse.Dirent{Type: fuse.DT_Unknown}
		if err := rows.Scan(&dirent.Name, &dirent.Inode); err != nil {
			return nil, err
		}
		results = append(results, dirent)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

// validateRename takes a source and destination node and verifies that
// a rename can be performed from source to destination.
// source must not be nil. destination can be.
func validateRename(tx *sql.Tx, source, destination *Node) error {
	if destination == nil {
		// No object at destination: good.
		return nil
	}

	if source.Mode.IsDir() {
		if destination.Mode.IsDir() {
			// Both are directories: destination must be empty
			return checkIsEmpty(tx, destination.ID)
		}
		// directory -> file: not allowed.
		return fuse.Errno(syscall.ENOTDIR)
	}

	// Source is a file.
	if destination.Mode.IsDir() {
		// file -> directory: not allowed.
		return fuse.Errno(syscall.EISDIR)
	}
	return nil
}

// rename moves 'oldParentID/oldName' to 'newParentID/newName'.
// If 'newParentID/newName' already exists, it is deleted.
// See NOTE on node.go:Rename.
func (cfs CFS) rename(oldParentID, newParentID uint64, oldName, newName string) error {
	if oldParentID == newParentID && oldName == newName {
		return nil
	}

	tx, err := cfs.db.Begin()
	if err != nil {
		return err
	}

	// Lookup source inode.
	srcObject, err := getInode(tx, oldParentID, oldName)
	if err != nil {
		_ = tx.Rollback()
		return err
	}

	// Lookup destination inode.
	destObject, err := getInode(tx, newParentID, newName)
	if err != nil && err != sql.ErrNoRows {
		_ = tx.Rollback()
		return err
	}

	// Check that the rename is allowed.
	if err := validateRename(tx, srcObject, destObject); err != nil {
		_ = tx.Rollback()
		return err
	}

	// At this point we know the following:
	// - srcObject is not nil
	// - destObject may be nil. If not, its inode can be deleted.
	if destObject == nil {
		// No new object: use INSERT.
		const insertSQL = `
DELETE FROM fs.namespace WHERE (parentID, name) = ($1, $2);
INSERT INTO fs.namespace VALUES ($3, $4, $5);
`
		if _, err := tx.Exec(insertSQL,
			oldParentID, oldName,
			newParentID, newName, srcObject.ID); err != nil {
			_ = tx.Rollback()
			return err
		}
	} else {
		// Destination exists.
		const updateSQL = `
DELETE FROM fs.namespace WHERE (parentID, name) = ($1, $2);
UPDATE fs.namespace SET id = $3 WHERE (parentID, name) = ($4, $5);
DELETE FROM fs.inode WHERE id = $6;
`
		if _, err := tx.Exec(updateSQL,
			oldParentID, oldName,
			srcObject.ID, newParentID, newName,
			destObject.ID); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

// Root returns the filesystem's root node.
// This node is special: it has a fixed ID and is not persisted.
func (cfs CFS) Root() (fs.Node, error) {
	return &Node{cfs: cfs, ID: rootNodeID, Mode: os.ModeDir | defaultPerms}, nil
}

// GenerateInode returns a new inode ID.
func (cfs CFS) GenerateInode(parentInode uint64, name string) uint64 {
	return cfs.newUniqueID()
}

func (cfs CFS) newUniqueID() (id uint64) {
	if err := cfs.db.QueryRow(`SELECT experimental_unique_int()`).Scan(&id); err != nil {
		panic(err)
	}
	return
}

// newFileNode returns a new node struct corresponding to a file.
func (cfs CFS) newFileNode() *Node {
	return &Node{
		cfs:  cfs,
		ID:   cfs.newUniqueID(),
		Mode: defaultPerms,
	}
}

// newDirNode returns a new node struct corresponding to a directory.
func (cfs CFS) newDirNode() *Node {
	return &Node{
		cfs:  cfs,
		ID:   cfs.newUniqueID(),
		Mode: os.ModeDir | defaultPerms,
	}
}
