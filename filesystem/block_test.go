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
	"bytes"
	"database/sql"
	"fmt"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach-go/testserver"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func initTestDB(t *testing.T) (*sql.DB, func()) {
	db, stop := testserver.NewDBForTest(t)

	if err := initSchema(db); err != nil {
		stop()
		t.Fatal(err)
	}

	return db, stop
}

func getAllBlocks(db *sql.DB, inode uint64) ([]byte, error) {
	blocks, err := getBlocks(db, inode)
	if err != nil {
		return nil, err
	}
	num := len(blocks)
	var data []byte
	for i, b := range blocks {
		if i != b.block {
			// We can't have missing blocks.
			return nil, fmt.Errorf("gap in block list, found block %d at index %d", b.block, i)
		}
		bl := uint64(len(b.data))
		if bl == 0 {
			return nil, fmt.Errorf("empty block found at %d (out of %d blocks)", i, num)
		}
		if i != (num-1) && bl != BlockSize {
			return nil, fmt.Errorf("non-blocksize %d at %d (out of %d blocks)", bl, i, num)
		}
		data = append(data, b.data...)
	}
	return data, nil
}

func TestBlockInfo(t *testing.T) {
	testCases := []struct {
		start, length uint64
		expected      blockRange
	}{
		{0, 0, blockRange{0, 0, 0, 0, 0}},
		{0, BlockSize * 4, blockRange{0, 0, BlockSize, 4, 0}},
		{0, BlockSize*4 + 500, blockRange{0, 0, BlockSize, 4, 500}},
		{500, BlockSize * 4, blockRange{0, 500, BlockSize - 500, 4, 500}},
		{BlockSize, BlockSize * 4, blockRange{1, 0, BlockSize, 5, 0}},
		{BlockSize, 500, blockRange{1, 0, 500, 1, 500}},
		{500, 1000, blockRange{0, 500, 1000, 0, 1500}},
	}

	for tcNum, tc := range testCases {
		actual := newBlockRange(tc.start, tc.length)
		if !reflect.DeepEqual(actual, tc.expected) {
			t.Errorf("#%d: expected:\n%+v\ngot:\n%+v", tcNum, tc.expected, actual)
		}
	}
}

func tryGrow(db *sql.DB, data []byte, id, newSize uint64) ([]byte, error) {
	originalSize := uint64(len(data))
	data = append(data, make([]byte, newSize-originalSize)...)
	if err := grow(db, id, originalSize, newSize); err != nil {
		return nil, err
	}
	newData, err := getAllBlocks(db, id)
	if err != nil {
		return nil, err
	}
	if uint64(len(newData)) != newSize {
		return nil, fmt.Errorf("getAllBlocks lengths don't match: got %d, expected %d", len(newData), newSize)
	}
	if !bytes.Equal(data, newData) {
		return nil, fmt.Errorf("getAllBlocks data doesn't match")
	}

	if newSize == 0 {
		return newData, nil
	}

	// Check the read as well.
	newData, err = read(db, id, 0, newSize)
	if err != nil {
		return nil, err
	}

	if uint64(len(newData)) != newSize {
		return nil, fmt.Errorf("read lengths don't match: got %d, expected %d", len(newData), newSize)
	}
	if !bytes.Equal(data, newData) {
		return nil, fmt.Errorf("read data doesn't match")
	}

	return newData, nil
}

func tryShrink(db *sql.DB, data []byte, id, newSize uint64) ([]byte, error) {
	originalSize := uint64(len(data))
	data = data[:newSize]
	if err := shrink(db, id, originalSize, newSize); err != nil {
		return nil, err
	}
	newData, err := getAllBlocks(db, id)
	if err != nil {
		return nil, err
	}
	if uint64(len(newData)) != newSize {
		return nil, fmt.Errorf("getAllData lengths don't match: got %d, expected %d", len(newData), newSize)
	}
	if !bytes.Equal(data, newData) {
		return nil, fmt.Errorf("getAllData data doesn't match")
	}

	if newSize == 0 {
		return newData, nil
	}

	// Check the read as well.
	newData, err = read(db, id, 0, newSize)
	if err != nil {
		return nil, err
	}

	if uint64(len(newData)) != newSize {
		return nil, fmt.Errorf("read lengths don't match: got %d, expected %d", len(newData), newSize)
	}
	if !bytes.Equal(data, newData) {
		return nil, fmt.Errorf("read data doesn't match")
	}

	return newData, nil
}

func TestShrinkGrow(t *testing.T) {
	db, stop := initTestDB(t)
	defer stop()

	id := uint64(10)

	var err error
	data := []byte{}

	if data, err = tryGrow(db, data, id, BlockSize*4+500); err != nil {
		t.Fatal(err)
	}
	if data, err = tryGrow(db, data, id, BlockSize*4+600); err != nil {
		t.Fatal(err)
	}
	if data, err = tryGrow(db, data, id, BlockSize*5); err != nil {
		t.Fatal(err)
	}

	// Shrink it down to 0.
	if data, err = tryShrink(db, data, id, 0); err != nil {
		t.Fatal(err)
	}
	if data, err = tryGrow(db, data, id, BlockSize*3+500); err != nil {
		t.Fatal(err)
	}
	if data, err = tryShrink(db, data, id, BlockSize*3+300); err != nil {
		t.Fatal(err)
	}
	if data, err = tryShrink(db, data, id, BlockSize*3); err != nil {
		t.Fatal(err)
	}
	if data, err = tryShrink(db, data, id, 0); err != nil {
		t.Fatal(err)
	}
	if data, err = tryGrow(db, data, id, BlockSize); err != nil {
		t.Fatal(err)
	}
	if data, err = tryShrink(db, data, id, BlockSize-200); err != nil {
		t.Fatal(err)
	}
	if data, err = tryShrink(db, data, id, BlockSize-500); err != nil {
		t.Fatal(err)
	}
	if data, err = tryShrink(db, data, id, 0); err != nil {
		t.Fatal(err)
	}
}

func TestReadWriteBlocks(t *testing.T) {
	db, stop := initTestDB(t)
	defer stop()

	id := uint64(10)
	rng, _ := randutil.NewPseudoRand()
	length := BlockSize*3 + 500
	part1 := randutil.RandBytes(rng, length)

	if err := write(db, id, 0, 0, part1); err != nil {
		t.Fatal(err)
	}

	readData, err := read(db, id, 0, uint64(length))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(part1, readData) {
		t.Errorf("Bytes differ. lengths: %d, expected %d", len(readData), len(part1))
	}

	verboseData, err := getAllBlocks(db, id)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(verboseData, part1) {
		t.Errorf("Bytes differ. lengths: %d, expected %d", len(verboseData), len(part1))
	}

	// Write with hole in the middle.
	part2 := make([]byte, BlockSize*2+250, BlockSize*2+250)
	fullData := append(part1, part2...)
	part3 := randutil.RandBytes(rng, BlockSize+123)
	if err := write(db, id, uint64(len(part1)), uint64(len(fullData)), part3); err != nil {
		t.Fatal(err)
	}
	fullData = append(fullData, part3...)
	readData, err = read(db, id, 0, uint64(len(fullData)))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(fullData, readData) {
		t.Errorf("Bytes differ. lengths: %d, expected %d", len(readData), len(fullData))
	}

	verboseData, err = getAllBlocks(db, id)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(verboseData, fullData) {
		t.Errorf("Bytes differ. lengths: %d, expected %d", len(verboseData), len(fullData))
	}

	// Now write into the middle of the file.
	part2 = randutil.RandBytes(rng, len(part2))
	if err := write(db, id, uint64(len(fullData)), uint64(len(part1)), part2); err != nil {
		t.Fatal(err)
	}
	fullData = append(part1, part2...)
	fullData = append(fullData, part3...)
	readData, err = read(db, id, 0, uint64(len(fullData)))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(fullData, readData) {
		t.Errorf("Bytes differ. lengths: %d, expected %d", len(readData), len(fullData))
	}

	verboseData, err = getAllBlocks(db, id)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(verboseData, fullData) {
		t.Errorf("Bytes differ. lengths: %d, expected %d", len(verboseData), len(fullData))
	}

	// New file.
	id2 := uint64(20)
	if err := write(db, id2, 0, 0, []byte("1")); err != nil {
		t.Fatal(err)
	}
	readData, err = read(db, id2, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if string(readData) != "1" {
		t.Fatalf("mismatch: %s", readData)
	}

	if err := write(db, id2, 1, 0, []byte("22")); err != nil {
		t.Fatal(err)
	}
	readData, err = read(db, id2, 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	if string(readData) != "22" {
		t.Fatalf("mismatch: %s", readData)
	}

	id3 := uint64(30)
	part1 = randutil.RandBytes(rng, BlockSize)
	// Write 5 blocks.
	var offset uint64
	for i := 0; i < 5; i++ {
		if err := write(db, id3, offset, offset, part1); err != nil {
			t.Fatal(err)
		}
		offset += BlockSize
	}
}
