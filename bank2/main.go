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
// Author: Spencer Kimball (spencer@cockroachlabs.com)

package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/url"
	"os"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach-go/crdb"
)

const systemAccountID = 0
const initialBalance = 1000

var maxTransfer = flag.Int("max-transfer", 100, "Maximum amount to transfer in one transaction.")
var numTransfers = flag.Int("num-transfers", 0, "Number of transfers (0 to continue indefinitely).")
var numAccounts = flag.Int("num-accounts", 100, "Number of accounts.")
var concurrency = flag.Int("concurrency", 16, "Number of concurrent actors moving money.")
var contention = flag.String("contention", "low", "Contention model {low | high}.")
var balanceCheckInterval = flag.Duration("balance-check-interval", time.Second, "Interval of balance check.")
var parallelStmts = flag.Bool("parallel-stmts", false, "Run independent statements in parallel.")

var txnCount int32
var successCount int32
var initialSystemBalance int

type measurement struct {
	read, write, total int64
	retries            int32
}

func transfersComplete() bool {
	return *numTransfers > 0 && atomic.LoadInt32(&successCount) >= int32(*numTransfers)
}

func moveMoney(db *sql.DB, aggr *measurement) {
	useSystemAccount := *contention == "high"

	for !transfersComplete() {
		var startWrite time.Time
		var readDuration time.Duration
		var fromBalance, toBalance int
		from, to := rand.Intn(*numAccounts)+1, rand.Intn(*numAccounts)+1
		if from == to {
			continue
		}
		if useSystemAccount {
			// Use the first account number we generated as a coin flip to
			// determine whether we're transferring money into or out of
			// the system account.
			if from > *numAccounts/2 {
				from = systemAccountID
			} else {
				to = systemAccountID
			}
		}
		amount := rand.Intn(*maxTransfer)
		start := time.Now()
		attempts := 0

		if err := crdb.ExecuteTx(context.TODO(), db, nil, func(tx *sql.Tx) error {
			attempts++
			if attempts > 1 {
				atomic.AddInt32(&aggr.retries, 1)
			}
			startRead := time.Now()
			rows, err := tx.Query(`SELECT id, balance FROM account WHERE id IN ($1, $2)`, from, to)
			if err != nil {
				return err
			}
			readDuration = time.Since(startRead)
			for rows.Next() {
				var id, balance int
				if err = rows.Scan(&id, &balance); err != nil {
					log.Fatal(err)
				}
				switch id {
				case from:
					fromBalance = balance
				case to:
					toBalance = balance
				default:
					panic(fmt.Sprintf("got unexpected account %d", id))
				}
			}
			startWrite = time.Now()
			if fromBalance < amount {
				return nil
			}
			insertTxn := `INSERT INTO transaction (id, txn_ref) VALUES ($1, $2)`
			insertTxnLeg := `INSERT INTO transaction_leg (account_id, amount, running_balance, txn_id) VALUES ($1, $2, $3, $4)`
			updateAcct := `UPDATE account SET balance = $1 WHERE id = $2`
			if *parallelStmts {
				const parallelize = ` RETURNING NOTHING`
				insertTxn += parallelize
				insertTxnLeg += parallelize
				updateAcct += parallelize
			}
			txnID := atomic.AddInt32(&txnCount, 1)
			if _, err = tx.Exec(insertTxn, txnID, fmt.Sprintf("txn %d", txnID)); err != nil {
				return err
			}
			if _, err = tx.Exec(insertTxnLeg, from, -amount, fromBalance-amount, txnID); err != nil {
				return err
			}
			if _, err = tx.Exec(insertTxnLeg, to, amount, toBalance+amount, txnID); err != nil {
				return err
			}
			if _, err = tx.Exec(updateAcct, toBalance+amount, to); err != nil {
				return err
			}
			if _, err = tx.Exec(updateAcct, fromBalance-amount, from); err != nil {
				return err
			}
			return nil
		}); err != nil {
			log.Printf("failed transaction: %v", err)
			continue
		}
		if fromBalance >= amount {
			atomic.AddInt32(&successCount, 1)
			atomic.AddInt64(&aggr.read, readDuration.Nanoseconds())
			atomic.AddInt64(&aggr.write, time.Since(startWrite).Nanoseconds())
			atomic.AddInt64(&aggr.total, time.Since(start).Nanoseconds())
		}
	}
}

func verifyTotalBalance(db *sql.DB) {
	var sum int
	if err := db.QueryRow("SELECT SUM(balance) FROM account").Scan(&sum); err != nil {
		log.Fatal(err)
	}
	if sum != *numAccounts*initialBalance+initialSystemBalance {
		log.Printf("The total balance is incorrect: %d.", sum)
		os.Exit(1)
	}
}

var usage = func() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s <db URL>\n\n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	flag.Usage = usage
	flag.Parse()

	dbURL := "postgresql://root@localhost:26257/bank2?sslmode=disable"
	if flag.NArg() == 1 {
		dbURL = flag.Arg(0)
	}

	parsedURL, err := url.Parse(dbURL)
	if err != nil {
		log.Fatal(err)
	}
	parsedURL.Path = "bank2"

	db, err := sql.Open("postgres", parsedURL.String())
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	if _, err := db.Exec("CREATE DATABASE IF NOT EXISTS bank2"); err != nil {
		log.Fatal(err)
	}

	// concurrency + 1, for this thread and the "concurrency" number of
	// goroutines that move money
	db.SetMaxOpenConns(*concurrency + 1)
	db.SetMaxIdleConns(*concurrency + 1)

	if _, err = db.Exec(`
CREATE TABLE IF NOT EXISTS account (
  id INT,
  balance INT NOT NULL,
  name STRING,

  PRIMARY KEY (id),
  UNIQUE INDEX byName (name)
);

CREATE TABLE IF NOT EXISTS transaction (
  id INT,
  booking_date TIMESTAMP DEFAULT NOW(),
  txn_date TIMESTAMP DEFAULT NOW(),
  txn_ref STRING,

  PRIMARY KEY (id),
  UNIQUE INDEX byTxnRef (txn_ref)
);

CREATE TABLE IF NOT EXISTS transaction_leg (
  id BYTES DEFAULT uuid_v4(),
  account_id INT,
  amount INT NOT NULL,
  running_balance INT NOT NULL,
  txn_id INT,

  PRIMARY KEY (id)
);

TRUNCATE TABLE account;
TRUNCATE TABLE transaction;
TRUNCATE TABLE transaction_leg;
`); err != nil {
		log.Fatal(err)
	}

	insertSQL := "INSERT INTO account (id, balance, name) VALUES ($1, $2, $3)"

	// Insert initialSystemBalance into the system account.
	initialSystemBalance = *numAccounts * initialBalance
	if _, err = db.Exec(insertSQL, systemAccountID, initialSystemBalance, "system account"); err != nil {
		log.Fatal(err)
	}
	// Insert initialBalance into all user accounts.
	for i := 1; i <= *numAccounts; i++ {
		if _, err = db.Exec(insertSQL, i, initialBalance, fmt.Sprintf("account %d", i)); err != nil {
			log.Fatal(err)
		}
	}

	verifyTotalBalance(db)

	var aggr measurement
	var lastSuccesses int32
	for i := 0; i < *concurrency; i++ {
		go moveMoney(db, &aggr)
	}

	start := time.Now()
	lastTime := start
	for range time.NewTicker(*balanceCheckInterval).C {
		now := time.Now()
		elapsed := now.Sub(lastTime)
		lastTime = now
		successes := atomic.LoadInt32(&successCount)
		newSuccesses := (successes - lastSuccesses)
		log.Printf("%d transfers were executed at %.1f/s", newSuccesses, float64(newSuccesses)/elapsed.Seconds())
		lastSuccesses = successes

		d := time.Duration(successes)
		read := time.Duration(atomic.LoadInt64(&aggr.read))
		write := time.Duration(atomic.LoadInt64(&aggr.write))
		total := time.Duration(atomic.LoadInt64(&aggr.total))
		retries := time.Duration(atomic.LoadInt32(&aggr.retries))
		log.Printf("averages: read: %v, write: %v, txn: %v, retries: %d",
			read/d, write/d, total/d, retries/d)
		verifyTotalBalance(db)
		if transfersComplete() {
			break
		}
	}
	log.Printf("completed %d transfers in %s with %d retries", atomic.LoadInt32(&successCount),
		time.Since(start), atomic.LoadInt32(&aggr.retries))
}
