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
// permissions and limitations under the License.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/spf13/cobra"
)

// pflagValue wraps flag.Value and implements the extra methods of the
// pflag.Value interface.
type pflagValue struct {
	flag.Value
}

func (v pflagValue) Type() string {
	t := reflect.TypeOf(v.Value).Elem()
	return t.Kind().String()
}

func (v pflagValue) IsBoolFlag() bool {
	t := reflect.TypeOf(v.Value).Elem()
	return t.Kind() == reflect.Bool
}

func normalizeStdFlagName(s string) string {
	return strings.Replace(s, "_", "-", -1)
}

var usage = map[string]string{
	"db":             "URL to the CockroachDB cluster",
	"users":          "number of concurrent simulated users",
	"benchmark-name": "name of benchmark to report for Go benchmark results",
}

// A Context holds configuration data.
type Context struct {
	// DBUrl is the URL to the database server.
	DBUrl string
	// NumUsers is the number of concurrent users generating load.
	NumUsers int
	//
	DB *sql.DB
	// Name of benchmark to use in benchmark results outputted upon process
	// termination. Used for analyzing performance over time.
	BenchmarkName string
}

var ctx = Context{
	DBUrl:         "postgresql://root@localhost:26257/photos?sslmode=disable",
	NumUsers:      1,
	BenchmarkName: "BenchmarkPhotos",
}

var loadCmd = &cobra.Command{
	Use:   "photos",
	Short: "generate artifical load using a simple three-table schema with indexes",
	Long: `
Create artificial load using a simple database schema containing
users, photos and comments. Users have photos, photos have comments.
Users can author comments on any photos. User actions are simulated
using an exponential distribution on user IDs, so lower IDs see
more activity than high ones.
`,
	Example: `  photos --db=postgresql://root@localhost:26257/photos?sslmode=disable`,
	RunE:    runLoad,
}

func runLoad(c *cobra.Command, args []string) error {
	log.Printf("generating load for %d concurrent users...", ctx.NumUsers)
	db, err := openDB(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if err := initSchema(db); err != nil {
		log.Fatal(err)
	}
	ctx.DB = db

	stopper := stop.NewStopper()
	stopper.RunWorker(func() {
		startStats(stopper)
	})
	for i := 0; i < ctx.NumUsers; i++ {
		stopper.RunWorker(func() {
			startUser(ctx, stopper)
		})
	}

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, os.Kill)
	signal.Notify(signalCh, syscall.SIGTERM)

	// Block until one of the signals above is received or the stopper
	// is stopped externally.
	select {
	case <-stopper.ShouldStop():
	case <-signalCh:
		stopper.Stop()
	}

	select {
	case <-signalCh:
		return fmt.Errorf("second signal received, initiating hard shutdown")
	case <-time.After(time.Minute):
		return fmt.Errorf("time limit reached, initiating hard shutdown")
	case <-stopper.IsStopped():
		log.Println("load generation complete")

		// Output results that mimic Go's built-in benchmark format.
		stats.Lock()
		elapsed := time.Now().Sub(stats.start)
		fmt.Println("Go benchmark results:")
		fmt.Printf("%s\t%8d\t%12.1f ns/op\n",
			ctx.BenchmarkName, stats.totalOps, float64(elapsed.Nanoseconds())/float64(stats.totalOps))
		stats.Unlock()
	}
	return nil
}

var dropCmd = &cobra.Command{
	Use:   "drop",
	Short: "drop the photos database",
	Long: `
Drop the photos database to start fresh.
`,
	Example: `  photos drop --db=<URL>`,
	RunE:    runDrop,
}

var splitCmd = &cobra.Command{
	Use:   "split",
	Short: "split the photos database",
	Long: `
Split all tables in the photos database to start fresh.
`,
	Example: `  photos split --db=<URL> <num splits>`,
	RunE:    runSplit,
}

func runDrop(c *cobra.Command, args []string) error {
	log.Printf("dropping photos database")
	db, err := openDB(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if err := dropDatabase(db); err != nil {
		log.Fatal(err)
	}
	return nil
}

func splitByUUID(db *sql.DB, numSplits int, tableName string, statementString string) {
	log.Printf("splitting table %q", tableName)
	for count := 0; count < numSplits; {
		if _, err := db.Exec(statementString, uuid.MakeV4().GetBytes()); err != nil {
			log.Printf("problem splitting: %v", err)
		} else {
			count++
		}
	}
}

func runSplit(c *cobra.Command, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("argument required: <num splits>")
	}
	n, err := strconv.ParseUint(args[0], 10, 32)
	if err != nil {
		return fmt.Errorf("unable to parse argument <num splits>: %v", err)
	}
	numSplits := int(n)
	log.Printf("splitting photos database into %d chunks", numSplits)

	db, err := openDB(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	if err := initSchema(db); err != nil {
		log.Fatal(err)
	}
	ctx.DB = db

	log.Printf(`splitting table "users"`)
	for count := 0; count < numSplits; {
		// Use the userID generation logic.
		userID := 1 + int(rand.ExpFloat64()/rate)
		if _, err := db.Exec(`ALTER TABLE users SPLIT AT VALUES ($1)`, userID); err != nil {
			log.Printf("problem splitting: %v", err)
		} else {
			count++
		}
	}

	splitByUUID(db, numSplits, "photos", `ALTER TABLE photos SPLIT AT VALUES ($1)`)
	splitByUUID(db, numSplits, "comments", `ALTER TABLE comments SPLIT AT VALUES ($1, '2016-01-01', '')`)
	return nil
}

func init() {
	rand.Seed(time.Now().UnixNano())
	loadCmd.AddCommand(
		dropCmd,
		splitCmd,
	)
	// Map any flags registered in the standard "flag" package into the
	// top-level command.
	pf := loadCmd.PersistentFlags()
	flag.VisitAll(func(f *flag.Flag) {
		pf.Var(pflagValue{f.Value}, normalizeStdFlagName(f.Name), f.Usage)
	})
	// Add persistent flags to the top-level command.
	loadCmd.PersistentFlags().IntVarP(&ctx.NumUsers, "users", "", ctx.NumUsers, usage["users"])
	loadCmd.PersistentFlags().StringVarP(&ctx.DBUrl, "db", "", ctx.DBUrl, usage["db"])
	loadCmd.PersistentFlags().StringVarP(&ctx.BenchmarkName, "benchmark-name", "", ctx.BenchmarkName,
		usage["benchmark-name"])
}

// Run ...
func Run(args []string) error {
	loadCmd.SetArgs(args)
	return loadCmd.Execute()
}

func main() {
	if err := Run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "failed running command %q: %v\n", os.Args[1:], err)
		os.Exit(1)
	}
}
