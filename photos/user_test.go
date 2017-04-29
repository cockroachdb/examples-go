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
	"context"
	"testing"

	"github.com/cockroachdb/cockroach-go/testserver"
)

// TestAllOps runs every operation once and ensures that they complete
// without error.
func TestAllOps(t *testing.T) {
	db, stop := testserver.NewDBForTestWithDatabase(t, "photos")
	defer stop()

	ctx := context.Background()

	if err := initSchema(ctx, db); err != nil {
		t.Fatal(err)
	}
	cfg := Config{
		DB:       db,
		NumUsers: 1,
	}

	for _, op := range ops {
		t.Logf("running %s", op.name)
		if err := runUserOp(ctx, cfg, 1, op.typ); err != nil {
			t.Error(err)
		}
	}
}

func TestCommentWithoutPhotos(t *testing.T) {
	db, stop := testserver.NewDBForTestWithDatabase(t, "photos")
	defer stop()

	ctx := context.Background()

	if err := initSchema(ctx, db); err != nil {
		t.Fatal(err)
	}
	cfg := Config{
		DB:       db,
		NumUsers: 1,
	}

	if err := runUserOp(ctx, cfg, 1, createUserOp); err != nil {
		t.Error(err)
	}

	if err := runUserOp(ctx, cfg, 1, createCommentOp); err == nil {
		t.Error("unexpected success creating comment with no photos")
	} else if err != errNoPhoto {
		t.Errorf("expected errNoPhoto, got %s", err)
	}
}
