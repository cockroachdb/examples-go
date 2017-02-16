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
// Author: Marc Berhault (marc@cockroachlabs.com)

package main

import (
	"testing"

	"github.com/cockroachdb/cockroach-go/testserver"
)

func initTestDB(t *testing.T) func() {
	ts, err := testserver.NewTestServer()
	if err != nil {
		t.Fatal(err)
	}
	err = ts.Start()
	if err != nil {
		t.Fatal(err)
	}

	url, err := ts.PGURL()
	if err != nil {
		t.Fatal(err)
	}
	url.Path = "testshorty"

	*createDB = true
	if err := SetupDB(url.String()); err != nil {
		ts.Stop()
		t.Fatal(err)
	}

	return ts.Stop
}

func TestCounter(t *testing.T) {
	stop := initTestDB(t)
	defer stop()

	// Check the initial counter.
	c, err := getCounter(shortyDB)
	if err != nil {
		t.Fatal(err)
	}
	expectedCounter := checkParseCounter(initialCounter)
	if c != expectedCounter {
		t.Fatalf("expected initial counter of %d, got %d", expectedCounter, c)
	}

	expectedShortURL := counterFromInt(expectedCounter)
	// Add a single entry with automatic URL.
	newURL, err := addNewShorty(Shorty{
		ShortURL: "",
		LongURL:  "https://www.google.com",
		Custom:   false,
		Public:   true,
		AddedBy:  "shorty tester",
	})
	if err != nil {
		t.Fatal(err)
	}
	expectedCounter++

	if newURL != expectedShortURL {
		t.Fatalf("expected new shorty URL of %s, got %s", expectedShortURL, newURL)
	}

	if c, err = getCounter(shortyDB); err != nil {
		t.Fatal(err)
	}

	if c != expectedCounter {
		t.Fatalf("expected new counter of %d, got %d", expectedCounter, c)
	}

	// Insert a custom URL right at the next counter.
	expectedShortURL = counterFromInt(expectedCounter)
	newURL, err = addNewShorty(Shorty{
		ShortURL: expectedShortURL,
		LongURL:  "https://www.google.com",
		Custom:   true,
		Public:   true,
		AddedBy:  "shorty tester",
	})
	if err != nil {
		t.Fatal(err)
	}

	if newURL != expectedShortURL {
		t.Fatalf("expected new shorty URL of %s, got %s", expectedShortURL, newURL)
	}

	// Check counter, it shouldn't have changed.
	if c, err = getCounter(shortyDB); err != nil {
		t.Fatal(err)
	}

	if c != expectedCounter {
		t.Fatalf("expected new counter of %d, got %d", expectedCounter, c)
	}

	// Now insert another automatic URL, this should skip one counter value.
	expectedCounter++
	expectedShortURL = counterFromInt(expectedCounter)
	// Add a single entry with automatic URL.
	newURL, err = addNewShorty(Shorty{
		ShortURL: "",
		LongURL:  "https://www.google.com",
		Custom:   false,
		Public:   true,
		AddedBy:  "shorty tester",
	})
	if err != nil {
		t.Fatal(err)
	}
	expectedCounter++

	if newURL != expectedShortURL {
		t.Fatalf("expected new shorty URL of %s, got %s", expectedShortURL, newURL)
	}

	if c, err = getCounter(shortyDB); err != nil {
		t.Fatal(err)
	}

	if c != expectedCounter {
		t.Fatalf("expected new counter of %d, got %d", expectedCounter, c)
	}

}
