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
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

// This example simulates a (particular) banking ledger. Depending on the
// chosen generator and concurrency, the workload carried out is contended
// or entirely non-overlapping.
package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net/url"
	"os"
	"strconv"
	"time"

	// Import postgres driver.
	"github.com/cockroachdb/cockroach-go/crdb"
	"github.com/lib/pq"
	"github.com/paulbellamy/ratecounter"
)

const stmtCreate = `
CREATE TABLE IF NOT EXISTS accounts (
  causality_id BIGINT NOT NULL,
  posting_group_id BIGINT NOT NULL,

  amount BIGINT,
  balance BIGINT,
  currency VARCHAR,

  created TIMESTAMP,
  value_date TIMESTAMP,

  account_id VARCHAR,
  transaction_id VARCHAR,

  scheme VARCHAR,

  PRIMARY KEY (account_id, posting_group_id),
  UNIQUE (account_id, causality_id) STORING(balance)
);
-- Could create this inline on Cockroach, but not on Postgres.
CREATE INDEX ON accounts(transaction_id);
CREATE INDEX ON accounts (posting_group_id);
`

var nSource = flag.Uint64("sources", 10, "Number of source accounts to choose from at random for transfers. Specify zero for maximum possible.")
var nDest = flag.Uint64("destinations", 10, "Number of destination accounts to choose from at random for transfers. Specify zero for maximum possible.")

var concurrency = flag.Int("concurrency", 5, "Number of concurrent actors moving money.")
var noRunningBalance = flag.Bool("no-running-balance", false, "Do not keep a running balance per account. Avoids contention.")
var verbose = flag.Bool("verbose", false, "Print information about each transfer.")

var counter *ratecounter.RateCounter

func init() {
	counter = ratecounter.NewRateCounter(1 * time.Second)
	rand.Seed(time.Now().UnixNano())
}

var usage = func() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s <db URL>\n\n", os.Args[0])
	flag.PrintDefaults()
}

type postingRequest struct {
	Group              int64
	AccountA, AccountB string
	Amount             int64 // deposited on AccountA, removed from AccountB
	Currency           string

	Transaction, Scheme string // opaque
}

var goldenReq = postingRequest{
	Group:    1,
	AccountA: "myacc",
	AccountB: "youracc",
	Amount:   5,
	Currency: "USD",
}

func generator(nSrc, nDst uint64) func() postingRequest {
	if nSrc == 0 || nSrc >= math.MaxInt64 {
		nSrc = math.MaxInt64
	}
	if nDst == 0 || nDst >= math.MaxInt64 {
		nDst = math.MaxInt64
	}
	return func() postingRequest {
		req := goldenReq
		req.AccountA = fmt.Sprintf("acc%d", rand.Int63n(int64(nSrc)))
		req.AccountB = fmt.Sprintf("acc%d", rand.Int63n(int64(nDst)))
		req.Group = rand.Int63()
		return req
	}
}

func getLast(tx *sql.Tx, accountID string) (lastCID int64, lastBalance int64, err error) {
	err = tx.QueryRow(`SELECT causality_id, balance FROM accounts `+
		`WHERE account_id = $1 ORDER BY causality_id DESC LIMIT 1`, accountID).
		Scan(&lastCID, &lastBalance)

	if err == sql.ErrNoRows {
		err = nil
		// Paranoia about unspecified semantics.
		lastBalance = 0
		lastCID = 0
	}
	return
}

func doPosting(tx *sql.Tx, req postingRequest) error {
	var cidA, balA, cidB, balB int64
	if !*noRunningBalance {
		var err error
		cidA, balA, err = getLast(tx, req.AccountA)
		if err != nil {
			return err
		}
		cidB, balB, err = getLast(tx, req.AccountB)
		if err != nil {
			return err
		}
	} else {
		// For Cockroach, unique_rowid() would be the better choice.
		cidA, cidB = rand.Int63(), rand.Int63()
		// Want the running balance to always be zero in this case without
		// special-casing below.
		balA = -req.Amount
		balB = req.Amount
	}
	_, err := tx.Exec(`
INSERT INTO accounts (
  posting_group_id,
  amount,
  account_id,
  causality_id, -- strictly increasing in absolute time. Only used for running balance.
  balance
)
VALUES (
  $1,	-- posting_group_id
  $2, 	-- amount
  $3, 	-- account_id (A)
  $4, 	-- causality_id
  $5+CAST($2 AS BIGINT) -- (new) balance (Postgres needs the cast)
), (
  $1,   -- posting_group_id
 -$2,   -- amount
  $6,   -- account_id (B)
  $7,   -- causality_id
  $8-$2 -- (new) balance
)`, req.Group, req.Amount,
		req.AccountA, cidA+1, balA,
		req.AccountB, cidB+1, balB)
	return err
}

type worker struct {
	l   func(string, ...interface{}) // logger
	gen func() postingRequest        // request generator
}

func (w *worker) run(db *sql.DB) {
	for {
		req := w.gen()
		if req.AccountA == req.AccountB {
			// The code we use throws a unique constraint violation since we
			// try to insert two conflicting primary keys. This isn't the
			// interesting case.
			continue
		}
		if *verbose {
			w.l("running %v", req)
		}
		if err := crdb.ExecuteTx(db, func(tx *sql.Tx) error {
			return doPosting(tx, req)
		}); err != nil {
			pqErr, ok := err.(*pq.Error)
			if ok {
				if pqErr.Code.Class() == pq.ErrorClass("23") {
					// Integrity violations. Don't expect many.
					w.l("%s", pqErr)
					continue
				}
				if pqErr.Code.Class() == pq.ErrorClass("40") {
					// Transaction rollback errors (e.g. Postgres
					// serializability restarts)
					if *verbose {
						w.l("%s", pqErr)
					}
					continue
				}
			}
			log.Fatal(err)
		} else {
			if *verbose {
				w.l("success")
			}
			counter.Incr(1)
		}
	}
}

func main() {
	flag.Usage = usage
	flag.Parse()

	if flag.NArg() != 1 {
		usage()
		os.Exit(2)
	}

	gen := generator(*nSource, *nDest)
	dbURL := flag.Arg(0)

	parsedURL, err := url.Parse(dbURL)
	if err != nil {
		log.Fatal(err)
	}
	if parsedURL.Path != "" && parsedURL.Path != "ledger" {
		log.Fatalf("unsupported database name %q in URL", parsedURL.Path)
	}
	parsedURL.Path = "ledger"

	db, err := sql.Open("postgres", parsedURL.String())
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	// Ignoring the error is the easiest way to be reasonably sure the db+table
	// exist without bloating the example by introducing separate code for
	// CockroachDB and Postgres (for which `IF NOT EXISTS` is not available
	// in this context).
	_, _ = db.Exec(`CREATE DATABASE ledger`)
	if _, err := db.Exec(stmtCreate); err != nil {
		log.Fatal(err)
	}

	for i := 0; i < *concurrency; i++ {
		num := i
		go (&worker{
			l: func(s string, args ...interface{}) {
				if *verbose {
					log.Printf(strconv.Itoa(num)+": "+s, args...)
				}
			},
			gen: gen}).run(db)
	}

	go func() {
		t := time.NewTicker(time.Second)
		for {
			select {
			case <-t.C:
				log.Printf("%d postings/sec", counter.Rate())
			}
		}
	}()

	select {} // block until killed
}
