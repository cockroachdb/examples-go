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
	"bytes"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/url"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/codahale/hdrhistogram"
	"github.com/satori/go.uuid"
	// Import postgres driver.
	_ "github.com/lib/pq"
)

const (
	insertBlockStmt = `INSERT INTO blocks (block_id, writer_id, block_num, raw_bytes) VALUES`
)

var createDB = flag.Bool("create-db", true, "Attempt to create the database (root user only)")

// concurrency = number of concurrent insertion processes.
var concurrency = flag.Int("concurrency", 2*runtime.NumCPU(), "Number of concurrent writers inserting blocks")

// batch = number of blocks to insert in a single SQL statement.
var batch = flag.Int("batch", 1, "Number of blocks to insert in a single SQL statement")

var splits = flag.Int("splits", 0, "Number of splits to perform before starting normal operations")

var tolerateErrors = flag.Bool("tolerate-errors", false, "Keep running on error")

// outputInterval = interval at which information is output to console.
var outputInterval = flag.Duration("output-interval", 1*time.Second, "Interval of output")

// Minimum and maximum size of inserted blocks.
var minBlockSizeBytes = flag.Int("min-block-bytes", 256, "Minimum amount of raw data written with each insertion")
var maxBlockSizeBytes = flag.Int("max-block-bytes", 1024, "Maximum amount of raw data written with each insertion")

var maxBlocks = flag.Uint64("max-blocks", 0, "Maximum number of blocks to write")
var duration = flag.Duration("duration", 0, "The duration to run. If 0, run forever.")
var benchmarkName = flag.String("benchmark-name", "BenchmarkBlockWriter", "Test name to report "+
	"for Go benchmark results.")

// numBlocks keeps a global count of successfully written blocks.
var numBlocks uint64

const (
	minLatency = 100 * time.Microsecond
	maxLatency = 10 * time.Second
)

func clampLatency(d, min, max time.Duration) time.Duration {
	if d < min {
		return min
	}
	if d > max {
		return max
	}
	return d
}

type blockWriter struct {
	db      *sql.DB
	rand    *rand.Rand
	latency struct {
		sync.Mutex
		*hdrhistogram.WindowedHistogram
	}
}

func newBlockWriter(db *sql.DB) *blockWriter {
	bw := &blockWriter{
		db:   db,
		rand: rand.New(rand.NewSource(int64(time.Now().UnixNano()))),
	}
	bw.latency.WindowedHistogram = hdrhistogram.NewWindowed(1,
		minLatency.Nanoseconds(), maxLatency.Nanoseconds(), 1)
	return bw
}

// run is an infinite loop in which the blockWriter continuously attempts to
// write blocks of random data into a table in cockroach DB.
func (bw *blockWriter) run(errCh chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()

	id := uuid.NewV4().String()
	var blockCount uint64

	for {
		var buf bytes.Buffer
		var args []interface{}
		fmt.Fprintf(&buf, "%s", insertBlockStmt)

		for i := 0; i < *batch; i++ {
			blockID := bw.rand.Int63()
			blockCount++
			args = append(args, bw.randomBlock())
			if i > 0 {
				fmt.Fprintf(&buf, ",")
			}
			fmt.Fprintf(&buf, ` (%d, '%s', %d, $%d)`, blockID, id, blockCount, i+1)
		}

		start := time.Now()
		if _, err := bw.db.Exec(buf.String(), args...); err != nil {
			errCh <- err
		} else {
			elapsed := clampLatency(time.Since(start), minLatency, maxLatency)
			bw.latency.Lock()
			if err := bw.latency.Current.RecordValue(elapsed.Nanoseconds()); err != nil {
				log.Fatal(err)
			}
			bw.latency.Unlock()
			v := atomic.AddUint64(&numBlocks, uint64(*batch))
			if *maxBlocks > 0 && v >= *maxBlocks {
				return
			}
		}
	}
}

func (bw *blockWriter) randomBlock() []byte {
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

	if *createDB {
		if _, err := db.Exec("CREATE DATABASE IF NOT EXISTS datablocks"); err != nil {
			return nil, err
		}
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

	if *splits > 0 {
		r := rand.New(rand.NewSource(int64(time.Now().UnixNano())))
		for i := 0; i < *splits; i++ {
			if _, err := db.Exec(`ALTER TABLE blocks SPLIT AT ($1, '', 0)`, r.Int63()); err != nil {
				return nil, err
			}
		}
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
	var lastBlocks uint64
	writers := make([]*blockWriter, *concurrency)

	errCh := make(chan error)
	var wg sync.WaitGroup
	for i := range writers {
		wg.Add(1)
		writers[i] = newBlockWriter(db)
		go writers[i].run(errCh, &wg)
	}

	var numErr int
	tick := time.Tick(*outputInterval)
	done := make(chan os.Signal, 3)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		wg.Wait()
		done <- syscall.Signal(0)
	}()

	if *duration > 0 {
		go func() {
			time.Sleep(*duration)
			done <- syscall.Signal(0)
		}()
	}

	defer func() {
		// Output results that mimic Go's built-in benchmark format.
		elapsed := time.Since(start)
		fmt.Printf("%s\t%8d\t%12.1f ns/op\n",
			*benchmarkName, numBlocks, float64(elapsed.Nanoseconds())/float64(numBlocks))
	}()

	for i := 0; ; {
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
			var h *hdrhistogram.Histogram
			for _, w := range writers {
				w.latency.Lock()
				m := w.latency.Merge()
				w.latency.Rotate()
				w.latency.Unlock()
				if h == nil {
					h = m
				} else {
					h.Merge(m)
				}
			}

			p50 := h.ValueAtQuantile(50)
			p95 := h.ValueAtQuantile(95)
			p99 := h.ValueAtQuantile(99)
			pMax := h.ValueAtQuantile(100)

			now := time.Now()
			elapsed := time.Since(lastNow)
			blocks := atomic.LoadUint64(&numBlocks)
			if i%20 == 0 {
				fmt.Println("_elapsed___errors__ops/sec(inst)___ops/sec(cum)__p50(ms)__p95(ms)__p99(ms)_pMax(ms)")
			}
			i++
			fmt.Printf("%8s %8d %14.1f %14.1f %8.1f %8.1f %8.1f %8.1f\n",
				time.Duration(time.Since(start).Seconds()+0.5)*time.Second,
				numErr,
				float64(blocks-lastBlocks)/elapsed.Seconds(),
				float64(blocks)/time.Since(start).Seconds(),
				time.Duration(p50).Seconds()*1000,
				time.Duration(p95).Seconds()*1000,
				time.Duration(p99).Seconds()*1000,
				time.Duration(pMax).Seconds()*1000)
			lastBlocks = blocks
			lastNow = now

		case <-done:
			blocks := atomic.LoadUint64(&numBlocks)
			elapsed := time.Since(start).Seconds()
			fmt.Println("\n_elapsed___errors_________blocks___ops/sec(cum)")
			fmt.Printf("%7.1fs %8d %14d %14.1f\n\n",
				time.Since(start).Seconds(), numErr,
				blocks, float64(blocks)/elapsed)
			return
		}
	}
}
