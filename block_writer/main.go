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
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/util/uuid"
	_ "github.com/lib/pq"
)

// concurrency = number of concurrent insertion processes.
var concurrency = flag.Int("concurrency", 3, "Number of concurrent writers inserting blocks.")
var tolerateErrors = flag.Bool("tolerate-errors", false, "Keep running on error")
var sqlDialect = flag.String("sql-dialect", "cockroach", "Dialect of SQL to use in example. One of 'cockroach' or 'postgres'")

// outputInterval = interval at which information is output to console.
var outputInterval = flag.Duration("output-interval", 1*time.Second, "Interval of output.")

// Minimum and maximum size of inserted blocks.
var minBlockSizeBytes = flag.Int("min-block-bytes", 256, "Minimum amount of raw data written with each insertion.")
var maxBlockSizeBytes = flag.Int("max-block-bytes", 1024, "Maximum amount of raw data written with each insertion.")

// SqlLogic implements basic blockwriter logic for a specific SQL dialect.
type SqlLogic interface {
	InsertBlock(int64, uint64, string, []byte) error
	Setup(string) error
}

// CockroachLogic implements blockwriter logic for Cockroach SQL.
type CockroachLogic struct {
	db *sql.DB
}

func (cl CockroachLogic) InsertBlock(blockID int64, blockNum uint64, writerID string, data []byte) error {
	const insertBlockStmt = `INSERT INTO datablocks.blocks (block_id, block_num, writer_id, raw_bytes) VALUES ($1, $2, $3, $4)`
	_, err := cl.db.Exec(insertBlockStmt, blockID, blockNum, writerID, data)
	return err
}

func (cl *CockroachLogic) Setup(connString string) error {
	parsedURL, err := url.Parse(connString)
	if err != nil {
		return err
	}

	// Remove any database specified in the connection URL.
	q := parsedURL.Query()
	q.Del("dbname")
	parsedURL.RawQuery = q.Encode()
	parsedURL.RawPath = ""

	// In order to create initial database, open connection to server with no database.
	cl.db, err = sql.Open("postgres", parsedURL.String())
	if err != nil {
		return err
	}
	if _, err = cl.db.Exec("CREATE DATABASE IF NOT EXISTS datablocks"); err != nil {
		return err
	}
	if err := cl.db.Close(); err != nil {
		return err
	}

	// Open connection to the datablocks database.
	q = parsedURL.Query()
	q.Add("dbname", "datablocks")
	parsedURL.RawQuery = q.Encode()
	cl.db, err = sql.Open("postgres", parsedURL.String())
	if err != nil {
		return err
	}

	// Create the 'blocks' table, dropping it if it already existed.
	if _, err := cl.db.Exec(`DROP TABLE IF EXISTS blocks`); err != nil {
		return err
	}
	if _, err := cl.db.Exec(`
		CREATE TABLE IF NOT EXISTS blocks (
		  block_id BIGINT NOT NULL,
		  writer_id TEXT NOT NULL,
		  block_num BIGINT NOT NULL,
		  raw_bytes BYTES NOT NULL,
		  PRIMARY KEY (block_id, writer_id, block_num)
		)`); err != nil {
		return err
	}

	cl.db.SetMaxOpenConns(*concurrency + 1)
	return nil
}

// CockroachLogic implements blockwriter logic for Postgres SQL.
type PostgresLogic struct {
	db *sql.DB
}

func (pl PostgresLogic) InsertBlock(blockID int64, blockNum uint64, writerID string, data []byte) error {
	const insertBlockStmt = `INSERT INTO blocks (block_id, block_num, writer_id, raw_bytes) VALUES ($1, $2, $3, $4)`
	_, err := pl.db.Exec(insertBlockStmt, blockID, blockNum, writerID, data)
	return err
}

func (pl *PostgresLogic) Setup(connString string) error {
	parsedURL, err := url.Parse(connString)
	if err != nil {
		return err
	}

	// Remove any dbname specified in the provided connection url.
	q := parsedURL.Query()
	q.Set("dbname", "postgres")
	parsedURL.RawQuery = q.Encode()
	parsedURL.RawPath = ""

	// Open connection to the 'postgres' database.
	pl.db, err = sql.Open("postgres", parsedURL.String())
	if err != nil {
		return err
	}
	// There is no conditional database creation in postgres; we must first
	// check it if it exists, and then create it if necessary.
	var count int
	if err := pl.db.QueryRow("SELECT COUNT(*) FROM pg_database WHERE datname = 'datablocks'").Scan(&count); err != nil {
		return err
	}
	if count == 0 {
		if _, err = pl.db.Exec("CREATE DATABASE datablocks"); err != nil {
			return err
		}
	}
	if err := pl.db.Close(); err != nil {
		return err
	}

	// Open connection directly to the datablocks database.
	q = parsedURL.Query()
	q.Add("dbname", "datablocks")
	parsedURL.RawQuery = q.Encode()
	pl.db, err = sql.Open("postgres", parsedURL.String())
	if err != nil {
		return err
	}

	// Create the 'blocks' table, dropping it if it already exists.
	if _, err := pl.db.Exec(`DROP TABLE IF EXISTS blocks`); err != nil {
		return err
	}
	if _, err := pl.db.Exec(`
		CREATE TABLE IF NOT EXISTS blocks (
		  block_id BIGINT NOT NULL,
		  writer_id TEXT NOT NULL,
		  block_num BIGINT NOT NULL,
		  raw_bytes BYTEA NOT NULL,
		  PRIMARY KEY (block_id, writer_id, block_num)
		)`); err != nil {
		return err
	}

	pl.db.SetMaxOpenConns(*concurrency + 1)
	return nil
}

// numBlocks keeps a global count of successfully written blocks.
var numBlocks uint64

// A blockWriter writes blocks of random data into cockroach in an infinite
// loop.
type blockWriter struct {
	id         string
	blockCount uint64
	logic      SqlLogic
	rand       *rand.Rand
}

func newBlockWriter(logic SqlLogic) *blockWriter {
	source := rand.NewSource(int64(time.Now().UnixNano()))
	return &blockWriter{
		logic: logic,
		id:    uuid.NewUUID4().String(),
		rand:  rand.New(source),
	}
}

// run is an infinite loop in which the blockWriter continuously attempts to
// write blocks of random data into a table in cockroach DB.
func (bw *blockWriter) run(errCh chan<- error) {
	for {
		blockID := bw.rand.Int63()
		blockData := bw.randomBlock()
		bw.blockCount++
		if err := bw.logic.InsertBlock(blockID, bw.blockCount, bw.id, blockData); err != nil {
			errCh <- fmt.Errorf("error running blockwriter %s: %s", bw.id, err)
		} else {
			atomic.AddUint64(&numBlocks, 1)
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

func usage() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s <db URL>\n\n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	flag.Usage = usage
	flag.Parse()

	if flag.NArg() != 1 {
		usage()
		os.Exit(2)
	}

	dbURL := flag.Arg(0)

	if *concurrency < 1 {
		fmt.Fprintf(os.Stderr, "Value of 'concurrency' flag (%d) must be greater than or equal to 1", *concurrency)
		usage()
		os.Exit(1)
	}

	if max, min := *maxBlockSizeBytes, *minBlockSizeBytes; max < min {
		fmt.Fprintf(os.Stderr, "Value of 'max-block-bytes' (%d) must be greater than or equal to value of 'min-block-bytes' (%d)", max, min)
		usage()
		os.Exit(1)
	}

	var logic SqlLogic
	switch *sqlDialect {
	case "cockroach":
		logic = &CockroachLogic{}
	case "postgres":
		logic = &PostgresLogic{}
	default:
		fmt.Fprintf(os.Stderr, "Invalid value for 'sql-dialect' flag, must be 'cockroach' or 'postgres'")
		usage()
		os.Exit(1)
	}

	if err := logic.Setup(dbURL); err != nil {
		log.Fatal(err)
	}

	lastNow := time.Now()
	var lastNumDumps uint64
	writers := make([]*blockWriter, *concurrency)

	errCh := make(chan error)
	for i := range writers {
		writers[i] = newBlockWriter(logic)
		go writers[i].run(errCh)
	}

	var numErr int
	for range time.Tick(*outputInterval) {
		now := time.Now()
		elapsed := time.Since(lastNow)
		dumps := atomic.LoadUint64(&numBlocks)
		log.Printf("%d dumps were executed at %.1f/second (%d total errors)", (dumps - lastNumDumps), float64(dumps-lastNumDumps)/elapsed.Seconds(), numErr)
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
			default:
			}
			break
		}
		lastNumDumps = dumps
		lastNow = now
	}
}
