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

	"github.com/spf13/cobra"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/util/uuid"
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
	"db":                     "URL to the CockroachDB cluster",
	"users":                  "number of concurrent simulated users",
	"benchmark-name":         "name of benchmark to report for Go benchmark results",
	"analytics":              "true/false to indicate if analytics queries should be occasionally run (default false)",
	"analytics-wait-seconds": "the wait time between successive analytics queries in seconds. Note that this is measured from the end of the last query to the start of the next query. (default 30s)",
}

// A Config holds configuration data.
type Config struct {
	// DBUrl is the URL to the database server.
	DBUrl string
	// NumUsers is the number of concurrent users generating load.
	NumUsers int
	//
	DB *sql.DB
	// Name of benchmark to use in benchmark results outputted upon process
	// termination. Used for analyzing performance over time.
	BenchmarkName string
	// AnalyticsQueries controls if analytics queries are periodically run.
	// Used for testing DistSQL code paths.
	AnalyticsQueries bool
	// AnalyticsQueriesWaitSeconds is the wait time between successive
	// analytics queries in seconds. Note that this is measured from the end
	// of the last query to the start of the next query.
	AnalyticsQueriesWaitSeconds int
}

var cfg = Config{
	DBUrl:                       "postgresql://root@localhost:26257/photos?sslmode=disable",
	NumUsers:                    1,
	BenchmarkName:               "BenchmarkPhotos",
	AnalyticsQueries:            false,
	AnalyticsQueriesWaitSeconds: 30,
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log.Printf("generating load for %d concurrent users...", cfg.NumUsers)
	db, err := openDB(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if err := initSchema(ctx, db); err != nil {
		log.Fatal(err)
	}
	cfg.DB = db

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, os.Kill)
	signal.Notify(signalCh, syscall.SIGTERM)

	go func() {
		<-signalCh
		cancel()
	}()

	errChan := make(chan error, 1+cfg.NumUsers)
	go func() {
		errChan <- startStats(ctx)
	}()
	for i := 0; i < cfg.NumUsers; i++ {
		go func() {
			errChan <- startUser(ctx, cfg)
		}()
	}
	if cfg.AnalyticsQueries {
		go func() {
			errChan <- startAnalytics(ctx, cfg)
		}()
	}

	for i := 0; i < 1+cfg.NumUsers; i++ {
		if err := <-errChan; err != ctx.Err() {
			return err
		}
	}

	log.Println("load generation complete")

	// Output results that mimic Go's built-in benchmark format.
	stats.Lock()
	elapsed := time.Now().Sub(stats.start)
	fmt.Println("Go benchmark results:")
	fmt.Printf("%s\t%8d\t%12.1f ns/op\n",
		cfg.BenchmarkName, stats.totalOps, float64(elapsed.Nanoseconds())/float64(stats.totalOps))
	stats.Unlock()

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
	ctx := context.Background()

	log.Printf("dropping photos database")
	db, err := openDB(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if err := dropDatabase(ctx, db); err != nil {
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
	ctx := context.Background()

	if len(args) != 1 {
		return fmt.Errorf("argument required: <num splits>")
	}
	n, err := strconv.ParseUint(args[0], 10, 32)
	if err != nil {
		return fmt.Errorf("unable to parse argument <num splits>: %v", err)
	}
	numSplits := int(n)
	log.Printf("splitting photos database into %d chunks", numSplits)

	db, err := openDB(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	if err := initSchema(ctx, db); err != nil {
		log.Fatal(err)
	}
	cfg.DB = db

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
	loadCmd.PersistentFlags().IntVarP(&cfg.NumUsers, "users", "", cfg.NumUsers, usage["users"])
	loadCmd.PersistentFlags().StringVarP(&cfg.DBUrl, "db", "", cfg.DBUrl, usage["db"])
	loadCmd.PersistentFlags().StringVarP(&cfg.BenchmarkName, "benchmark-name", "", cfg.BenchmarkName,
		usage["benchmark-name"])
	loadCmd.PersistentFlags().BoolVarP(&cfg.AnalyticsQueries, "analytics", "", cfg.AnalyticsQueries, usage["analytics"])
	loadCmd.PersistentFlags().IntVarP(&cfg.AnalyticsQueriesWaitSeconds, "analytics-wait-seconds", "", cfg.AnalyticsQueriesWaitSeconds, usage["analytics-wait-seconds"])
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
