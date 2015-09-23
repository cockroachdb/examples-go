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
	"syscall"

	"bazil.org/fuse"
)

// sqlExecutor is an interface needed for basic queries.
// It is implemented by both sql.DB and sql.Txn.
type sqlExecutor interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
}

// getInode looks up an inode given its name and its parent ID.
// If not found, error will be sql.ErrNoRows.
func getInode(e sqlExecutor, parentID uint64, name string) (*Node, error) {
	var raw string
	const sql = `SELECT inode FROM fs.inode WHERE id = 
(SELECT id FROM fs.namespace WHERE (parentID, name) = ($1, $2))`
	if err := e.QueryRow(sql, parentID, name).Scan(&raw); err != nil {
		return nil, err
	}

	node := &Node{}
	err := json.Unmarshal([]byte(raw), node)
	return node, err
}

// checkIsEmpty returns nil if 'id' has no children.
func checkIsEmpty(e sqlExecutor, id uint64) error {
	var count uint64
	const countSql = `
SELECT COUNT(parentID) FROM fs.namespace WHERE parentID = $1`
	if err := e.QueryRow(countSql, id).Scan(&count); err != nil {
		return err
	}
	if count != 0 {
		return fuse.Errno(syscall.ENOTEMPTY)
	}
	return nil
}
