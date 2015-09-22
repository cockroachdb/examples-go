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

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)

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

// Root
var _ = fs.FS(&CFS{})

// GenerateInode
var _ = fs.FSInodeGenerator(&CFS{})

// CFS implements a filesystem on top of cockroach.
type CFS struct {
	db *sql.DB
}

func (cfs CFS) initSchema() error {
	_, err := cfs.db.Exec(fsSchema)
	return err
}

func (cfs CFS) create(parentID uint64, name, inode string) error {
	var id int64
	if err := cfs.db.QueryRow(`SELECT experimental_unique_int()`).Scan(&id); err != nil {
		return err
	}
	tx, err := cfs.db.Begin()
	if err != nil {
		return err
	}
	const sql = `
INSERT INTO fs.inode VALUES ($1, $2);
INSERT INTO fs.namespace VALUES ($3, $4, $1);
`
	if _, err := tx.Exec(sql, id, inode, parentID, name); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (cfs CFS) lookup(parentID uint64, name string) (string, error) {
	const sql = `
SELECT inode FROM fs.inode WHERE id =
  (SELECT id FROM fs.namespace WHERE (parentID, name) = ($1, $2))
`
	var inode string
	if err := cfs.db.QueryRow(`sql`, parentID, name).Scan(&inode); err != nil {
		return "", err
	}
	return inode, nil
}

// list returns the children of the node with id 'parentID'.
// Dirent consists of:
// Inode uint64
// Type DirentType (optional)
// Name string
// TODO(pmattis): lookup all inodes and fill in the type,
// this will save a Getattr().
func (cfs CFS) list(parentID uint64) ([]fuse.Dirent, error) {
	rows, err := cfs.db.Query(`SELECT name, id FROM fs.namespace WHERE parentID = $1`, parentID)
	if err != nil {
		return nil, err
	}

	var results []fuse.Dirent
	for rows.Next() {
		var name string
		var id uint64
		if err := rows.Scan(&name, &id); err != nil {
			return nil, err
		}
		results = append(results, fuse.Dirent{Inode: id, Type: fuse.DT_Unknown, Name: name})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

// Root returns the filesystem's root node.
func (cfs CFS) Root() (fs.Node, error) {
	return &Node{cfs: cfs, name: "", id: 0, isDir: true}, nil
}

// GenerateInode returns a new inode ID.
func (cfs CFS) GenerateInode(parentInode uint64, name string) uint64 {
	var id uint64
	if err := cfs.db.QueryRow(`SELECT experimental_unique_int()`).Scan(&id); err != nil {
		panic(err)
	}
	return id
}
