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
	"syscall"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)

const rootNodeID = 1

const (
	fsSchema = `
CREATE DATABASE fs;

CREATE TABLE fs.namespace (
  parentID INT,
  name     STRING,
  id       INT,
  PRIMARY KEY (parentID, name)
);

CREATE TABLE fs.inode (
  id    INT PRIMARY KEY,
  inode STRING
);
`
)

var _ fs.FS = &CFS{}               // Root
var _ fs.FSInodeGenerator = &CFS{} // GenerateInode

// CFS implements a filesystem on top of cockroach.
type CFS struct {
	db *sql.DB
}

func (cfs CFS) initSchema() error {
	_, err := cfs.db.Exec(fsSchema)
	return err
}

// create inserts a new node. If node.ID is zero, an id is automatically generated.
func (cfs CFS) create(parentID uint64, node Node) error {
	if node.ID == 0 {
		if err := cfs.db.QueryRow(`SELECT experimental_unique_int()`).Scan(&node.ID); err != nil {
			return err
		}
	}
	inode := node.toJSON()
	tx, err := cfs.db.Begin()
	if err != nil {
		return err
	}
	const sql = `
INSERT INTO fs.inode VALUES ($1, $2);
INSERT INTO fs.namespace VALUES ($3, $4, $1);
`
	if _, err := tx.Exec(sql, node.ID, inode, parentID, node.Name); err != nil {
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
	const lookupSql = `
SELECT id FROM fs.namespace WHERE (parentID, name) = ($1, $2)`

	var id uint64
	if err := tx.QueryRow(lookupSql, parentID, name).Scan(&id); err != nil {
		_ = tx.Rollback()
		return err
	}

	// Check if there are any children.
	if checkChildren {
		count, err := countChildren(tx, id)
		if err != nil {
			_ = tx.Rollback()
			return err
		}
		if count != 0 {
			_ = tx.Rollback()
			return fuse.Errno(syscall.ENOTEMPTY)
		}
	}

	// Delete all entries.
	const sql = `
DELETE FROM fs.namespace WHERE (parentID, name) = ($1, $2);
DELETE FROM fs.inode WHERE id = $3;
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
	dirent := fuse.Dirent{Type: fuse.DT_Unknown}
	for rows.Next() {
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

// update writes the updated value of 'node'.
func (cfs CFS) update(node Node) error {
	inode := node.toJSON()
	const sql = `
UPDATE fs.inode SET inode = $1 WHERE id = $2;
`
	if _, err := cfs.db.Exec(sql, inode, node.ID); err != nil {
		return err
	}
	return nil
}

// rename moves 'oldParentID/oldName' to 'newParentID/newName'.
// If 'newParentID/newName' already exists, it is deleted.
// See NOTE on node.go:Rename.
// TODO(marc): this is getting a little gross. We should change this file to
// be a bunch of methods that take a transaction.
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

	if destObject != nil {
		// destination exists.
		if srcObject.IsDir {
			// srcObject is a directory.
			if destObject.IsDir {
				// Destination must be empty
				count, err := countChildren(tx, destObject.ID)
				if err != nil {
					_ = tx.Rollback()
					return err
				}
				if count != 0 {
					_ = tx.Rollback()
					return fuse.Errno(syscall.ENOTEMPTY)
				}
			} else {
				// Destination is a file: not allowed.
				_ = tx.Rollback()
				return fuse.Errno(syscall.ENOTDIR)
			}
		} else {
			// srcObject is a file.
			if destObject.IsDir {
				// source is a file, destination is a directory: not allowed.
				_ = tx.Rollback()
				return fuse.Errno(syscall.EISDIR)
			}
		}
	}

	// At this point we know the following:
	// - srcObject is not nil
	// - destObject may be nil. If not, its inode can be deleted.
	if destObject == nil {
		// No new object: use INSERT.
		const insertSql = `
DELETE FROM fs.namespace WHERE (parentID, name) = ($1, $2);
INSERT INTO fs.namespace VALUES ($3, $4, $5);
`
		if _, err := tx.Exec(insertSql,
			oldParentID, oldName,
			newParentID, newName, srcObject.ID); err != nil {
			_ = tx.Rollback()
			return err
		}
	} else {
		// Destination exists.
		const updateSql = `
DELETE FROM fs.namespace WHERE (parentID, name) = ($1, $2);
UPDATE fs.namespace SET id = $3 WHERE (parentID, name) = ($4, $5);
DELETE FROM fs.inode WHERE id = $6;
`
		if _, err := tx.Exec(updateSql,
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
func (cfs CFS) Root() (fs.Node, error) {
	return &Node{cfs: cfs, Name: "", ID: rootNodeID, IsDir: true}, nil
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
