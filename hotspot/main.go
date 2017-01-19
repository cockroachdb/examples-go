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

// The hot spot example program is a read/write workload intended to always hit
// the exact same row. It performs reads and writes to simulate a super
// contentious load.

package main

import (
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

	// Import postgres driver.
	_ "github.com/lib/pq"
)

const (
	createDatabaseStatement = "CREATE DATABASE IF NOT EXISTS hot"
	createTableStatement    = `
	CREATE TABLE IF NOT EXISTS hot.spot (
	  id BIGINT NOT NULL,
	  value BIGINT,
	  PRIMARY KEY (id)
	)`
)

var concurrency = flag.Int("concurrency", 2*runtime.NumCPU(), "Number of concurrent reading/writing processes")
var writePercent = flag.Int("write-percent", 50, "Percentage, from 0 to 100 of the operations that will perform writes instead of reads")
var tolerateErrors = flag.Bool("tolerate-errors", false, "Keep running on error")
var outputInterval = flag.Duration("output-interval", 1*time.Second, "Interval of output")
var duration = flag.Duration("duration", 0, "The duration to run. If 0, run forever.")
var benchmarkName = flag.String("benchmark-name", "BenchmarkHotSpot", "Test name to report for Go benchmark results.")

var readCount, writeCount uint64

// A hotSpotWriter writes and reads values from one row in an infinite loop.
type hotSpotWriter struct {
	db   *sql.DB
	rand *rand.Rand
}

func newHotSpotWriter(db *sql.DB) hotSpotWriter {
	source := rand.NewSource(int64(time.Now().UnixNano()))
	return hotSpotWriter{
		db:   db,
		rand: rand.New(source),
	}
}

// run is an infinite loop in which the hotSpotWriter continuously attempts to
// read and write values from a single row.
func (w hotSpotWriter) run(errCh chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()

	wPercent := *writePercent
	for {
		if w.rand.Intn(100) < wPercent {
			if _, err := w.db.Exec("UPSERT INTO hot.spot(id, value) VALUES (1, $1)", rand.Int63()); err != nil {
				errCh <- err
			} else {
				atomic.AddUint64(&writeCount, 1)
			}
		} else {
			var value int
			if err := w.db.QueryRow("SELECT value FROM hot.spot WHERE id = 1").Scan(&value); err != nil {
				errCh <- err
			} else {
				atomic.AddUint64(&readCount, 1)
			}
		}
	}
}

// setupDatabase performs initial setup for the example, creating a database and
// with a single table.
func setupDatabase(dbURL string) (*sql.DB, error) {
	parsedURL, err := url.Parse(dbURL)
	if err != nil {
		return nil, err
	}
	parsedURL.Path = "hot"

	// Open connection to server and create a database.
	db, err := sql.Open("postgres", parsedURL.String())
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec(createDatabaseStatement); err != nil {
		return nil, err
	}
	if _, err := db.Exec(createTableStatement); err != nil {
		return nil, err
	}
	// Insert a single value into the database to avoid errors if we try to read
	// before writing.
	if _, err := db.Exec("UPSERT INTO hot.spot(id, value) VALUES (1, $1)", rand.Int63()); err != nil {
		return nil, err
	}

	// Allow a maximum of concurrency+1 connections to the database.
	db.SetMaxOpenConns(*concurrency + 1)
	db.SetMaxIdleConns(*concurrency + 1)

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

	dbURL := "postgresql://root@localhost:26257/hot?sslmode=disable"
	if flag.NArg() == 1 {
		dbURL = flag.Arg(0)
	}

	if *concurrency < 1 {
		log.Fatalf("Value of 'concurrency' flag (%d) must be greater than or equal to 1", *concurrency)
	}

	if *writePercent < 0 || *writePercent > 100 {
		log.Fatalf("Value of 'writePercent' flag (%d) must be between 0 and 100", *writePercent)
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
	var lastReads, lastWrites, lastTotal uint64
	writers := make([]hotSpotWriter, *concurrency)

	errCh := make(chan error)
	var wg sync.WaitGroup
	for i := range writers {
		wg.Add(1)
		writers[i] = newHotSpotWriter(db)
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
		reads := atomic.LoadUint64(&readCount)
		writes := atomic.LoadUint64(&writeCount)
		total := reads + writes
		fmt.Printf("%s\t%8d\t%12.1f ns/op\n",
			*benchmarkName, total, float64(elapsed.Nanoseconds())/float64(total))
	}()

	ticks := -1
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
			ticks++
			now := time.Now()
			elapsed := time.Since(lastNow)
			reads := atomic.LoadUint64(&readCount)
			writes := atomic.LoadUint64(&writeCount)
			total := reads + writes

			if ticks%20 == 0 {
				fmt.Println("_elapsed___errors____r/sec____w/sec___rw/sec")
			}

			fmt.Printf("%8s %8d %8d %8d %8d\n",
				time.Duration(time.Since(start).Seconds()+0.5)*time.Second,
				numErr,
				int(float64(reads-lastReads)/elapsed.Seconds()),
				int(float64(writes-lastWrites)/elapsed.Seconds()),
				int(float64(total-lastTotal)/elapsed.Seconds()))
			lastReads = reads
			lastWrites = writes
			lastTotal = total
			lastNow = now

		case <-done:
			fmt.Println("---------------------------------------------")
			reads := atomic.LoadUint64(&readCount)
			writes := atomic.LoadUint64(&writeCount)
			total := reads + writes
			elapsed := time.Duration(time.Since(start).Seconds()+0.5) * time.Second
			fmt.Printf("time:%s, reads:%d(%.1f/sec), writes:%d(%.1f/sec), total:%d(%.1f/sec), errors:%d\n",
				elapsed,
				reads, float64(reads)/elapsed.Seconds(),
				writes, float64(writes)/elapsed.Seconds(),
				total, float64(total)/elapsed.Seconds(),
				numErr,
			)
			return
		}
	}
}
