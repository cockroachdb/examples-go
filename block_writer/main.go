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
// Author: Matt Tracy

// The block writer example program is a write-only workload intended to insert
// a large amount of data into cockroach quickly. This example is intended to
// trigger range splits and rebalances.
package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/url"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/satori/go.uuid"
	// Import postgres driver.
	_ "github.com/cockroachdb/pq"
)

const (
	insertBlockStmt = `INSERT INTO blocks (block_id, writer_id, block_num, raw_bytes) VALUES ($1, $2, $3, $4)`
)

// concurrency = number of concurrent insertion processes.
var concurrency = flag.Int("concurrency", 2*runtime.NumCPU(), "Number of concurrent writers inserting blocks")

var tolerateErrors = flag.Bool("tolerate-errors", false, "Keep running on error")

// outputInterval = interval at which information is output to console.
var outputInterval = flag.Duration("output-interval", 1*time.Second, "Interval of output")

// Minimum and maximum size of inserted blocks.
var minBlockSizeBytes = flag.Int("min-block-bytes", 256, "Minimum amount of raw data written with each insertion")
var maxBlockSizeBytes = flag.Int("max-block-bytes", 1024, "Maximum amount of raw data written with each insertion")

var maxBlocks = flag.Uint64("max-blocks", 0, "Maximum number of blocks to write")
var duration = flag.Duration("duration", 0, "The duration to run. If 0, run forever.")

// numBlocks keeps a global count of successfully written blocks.
var numBlocks uint64

// A blockWriter writes blocks of random data into cockroach in an infinite
// loop.
type blockWriter struct {
	id         string
	blockCount uint64
	db         *sql.DB
	rand       *rand.Rand
}

func newBlockWriter(db *sql.DB) blockWriter {
	source := rand.NewSource(int64(time.Now().UnixNano()))
	return blockWriter{
		db:   db,
		id:   uuid.NewV4().String(),
		rand: rand.New(source),
	}
}

// run is an infinite loop in which the blockWriter continuously attempts to
// write blocks of random data into a table in cockroach DB.
func (bw blockWriter) run(errCh chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		blockID := bw.rand.Int63()
		blockData := bw.randomBlock()
		bw.blockCount++
		if _, err := bw.db.Exec(insertBlockStmt, blockID, bw.id, bw.blockCount, blockData); err != nil {
			errCh <- fmt.Errorf("error running blockwriter %s: %s", bw.id, err)
		} else {
			v := atomic.AddUint64(&numBlocks, 1)
			if *maxBlocks > 0 && v >= *maxBlocks {
				return
			}
		}
	}
}

// randomBlock generates a slice of randomized bytes. Random data is preferred
// to prevent compression in storage.
func (bw blockWriter) randomBlock() []byte {
	blockSize := bw.rand.Intn(*maxBlockSizeBytes-*minBlockSizeBytes) + *minBlockSizeBytes
	blockData := make([]byte, blockSize)
	for i := range blockData {
		blockData[i] = byte(bw.rand.Int() & 0xff)
	}
	return blockData
}

// setupDatabase performs initial setup for the example, creating a database and
// with a single table. If the desired table already exists on the cluster, the
// existing table will be dropped.
func setupDatabase(dbURL string) (*sql.DB, error) {
	parsedURL, err := url.Parse(dbURL)
	if err != nil {
		return nil, err
	}
	parsedURL.Path = "datablocks"

	// Open connection to server and create a database.
	db, err := sql.Open("postgres", parsedURL.String())
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec("CREATE DATABASE IF NOT EXISTS datablocks"); err != nil {
		return nil, err
	}

	// Allow a maximum of concurrency+1 connections to the database.
	db.SetMaxOpenConns(*concurrency + 1)
	db.SetMaxIdleConns(*concurrency + 1)

	// Create the initial table for storing blocks.
	if _, err := db.Exec(`
	CREATE TABLE IF NOT EXISTS blocks (
	  block_id BIGINT NOT NULL,
	  writer_id STRING NOT NULL,
	  block_num BIGINT NOT NULL,
	  raw_bytes BYTES NOT NULL,
	  PRIMARY KEY (block_id, writer_id, block_num)
	)`); err != nil {
		return nil, err
	}

	return db, nil
}

var usage = func() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s <db URL>\n\n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	flag.Usage = usage
	flag.Parse()

	dbURL := "postgresql://root@localhost:26257/photos?sslmode=disable"
	if flag.NArg() == 1 {
		dbURL = flag.Arg(0)
	}

	if *concurrency < 1 {
		log.Fatalf("Value of 'concurrency' flag (%d) must be greater than or equal to 1", *concurrency)
	}

	if max, min := *maxBlockSizeBytes, *minBlockSizeBytes; max < min {
		log.Fatalf("Value of 'max-block-bytes' (%d) must be greater than or equal to value of 'min-block-bytes' (%d)", max, min)
	}

	var db *sql.DB
	{
		var err error
		for err == nil || *tolerateErrors {
			db, err = setupDatabase(dbURL)
			if err == nil {
				break
			}
			if !*tolerateErrors {
				log.Fatal(err)
			}
		}
	}

	lastNow := time.Now()
	start := lastNow
	var lastNumDumps uint64
	writers := make([]blockWriter, *concurrency)

	errCh := make(chan error)
	var wg sync.WaitGroup
	for i := range writers {
		wg.Add(1)
		writers[i] = newBlockWriter(db)
		go writers[i].run(errCh, &wg)
	}

	var numErr int
	tick := time.Tick(*outputInterval)
	done := make(chan struct{}, 1)

	go func() {
		wg.Wait()
		done <- struct{}{}
	}()

	if *duration > 0 {
		go func() {
			time.Sleep(*duration)
			done <- struct{}{}
		}()
	}

	for {
		select {
		case err := <-errCh:
			numErr++
			if !*tolerateErrors {
				log.Fatal(err)
			} else {
				log.Print(err)
			}
			continue

		case <-tick:
			now := time.Now()
			elapsed := time.Since(lastNow)
			dumps := atomic.LoadUint64(&numBlocks)
			fmt.Printf("%6s: %6.1f/sec",
				time.Duration(time.Since(start).Seconds()+0.5)*time.Second,
				float64(dumps-lastNumDumps)/elapsed.Seconds())
			if numErr > 0 {
				fmt.Printf(" (%d total errors)", numErr)
			}
			fmt.Printf("\n")
			lastNumDumps = dumps
			lastNow = now

		case <-done:
			dumps := atomic.LoadUint64(&numBlocks)
			elapsed := time.Since(start)
			fmt.Printf("%6.1fs: %d total %6.1f/sec",
				elapsed.Seconds(), dumps, float64(dumps)/elapsed.Seconds())
			if numErr > 0 {
				fmt.Printf(" (%d total errors)\n", numErr)
			}
			fmt.Printf("\n")
			// Output results that mimic Go's built-in benchmark format.
			fmt.Printf("BenchmarkBlockWriter\t%8d\t%12.1f ns/op\n",
				numBlocks, float64(elapsed.Nanoseconds())/float64(numBlocks))
			os.Exit(0)
		}
	}
}
