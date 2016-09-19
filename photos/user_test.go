// Copyright 2016 The Cockroach Authors.
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
// Author: Ben Darnell

package main

import (
	"testing"

	"github.com/cockroachdb/cockroach-go/testserver"
)

func initTestDB(t *testing.T, numUsers int) (Context, func()) {
	db, stop := testserver.NewDBForTestWithDatabase(t, "photos")
	if err := initSchema(db); err != nil {
		stop()
		t.Fatal(err)
	}
	ctx := Context{
		DB:       db,
		NumUsers: numUsers,
	}
	return ctx, stop
}

// TestAllOps runs every operation once and ensures that they complete
// without error.
func TestAllOps(t *testing.T) {
	ctx, stop := initTestDB(t, 1)
	defer stop()

	for _, op := range ops {
		if err := runUserOp(ctx, 1, op.typ); err != nil {
			t.Error(err)
		}
	}
}
