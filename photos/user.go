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
	"encoding/binary"
	"hash/fnv"
	"log"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach-go/crdb"
	"github.com/codahale/hdrhistogram"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

const (
	meanUserID    = 1 << 15
	rate          = 1.0 / (meanUserID * 2)
	statsInterval = 10 * time.Second
)

const (
	createUserOp = iota
	createPhotoOp
	createCommentOp
	listPhotosOp
	listCommentsOp
	updatePhotoOp
	updateCommentOp
	deleteCommentOp
	deletePhotoOp
	listMostCommentedPhotosOp
	listCommentsAlphabeticallyOp
)

const (
	topCommentersAnalyticsOp = iota
	topPostersAnalyticsOp
	topPhotosAnalyticsOp
	top10PhotoPostersNamesAnalytcsOp
	numAnalyticsOps
)

type opDesc struct {
	typ      int
	name     string
	relFreq  float64
	normFreq float64
}

// Note that tests care about the order here: running each command
// once in this order is expected to succeed (so users must be created
// before photos which must be created before comments, with deletion in
// the reverse order).
var ops = []*opDesc{
	{createUserOp, "create user", 1, 0},
	{createPhotoOp, "create photo", 10, 0},
	{createCommentOp, "create comment", 50, 0},
	{listPhotosOp, "list photos", 20, 0},
	{listCommentsOp, "list comments", 20, 0},
	{updatePhotoOp, "update photo", 2.5, 0},
	{updateCommentOp, "update comment", 5, 0},
	{listMostCommentedPhotosOp, "list most commented on photos", 25, 0},
	{listCommentsAlphabeticallyOp, "list comments alphabetically", 15, 0},
	{deleteCommentOp, "delete comment", 2.5, 0},
	{deletePhotoOp, "delete photo", 1.25, 0},
}

var stats struct {
	sync.Mutex
	start             time.Time
	computing         bool
	totalOps          int
	noUserOps         int
	noPhotoOps        int
	noAnalyticsOps    int
	failedOps         int
	hist              *hdrhistogram.Histogram
	opCounts          map[int]int
	analyticsOpCounts map[int]int
}

func init() {
	stats.hist = hdrhistogram.New(0, 0x7fffffff, 1)
	stats.start = time.Now()
	stats.opCounts = map[int]int{}
	stats.analyticsOpCounts = map[int]int{}

	// Compute the total of all op relative frequencies.
	var relFreqTotal float64
	for _, op := range ops {
		relFreqTotal += op.relFreq
	}
	// Normalize frequencies.
	var normFreqTotal float64
	for _, op := range ops {
		normFreq := op.relFreq / relFreqTotal
		op.normFreq = normFreqTotal + normFreq
		normFreqTotal += normFreq
	}
}

// randomOp chooses a random operation from the ops slice.
func randomOp() *opDesc {
	r := rand.Float64()
	for _, op := range ops {
		if r < op.normFreq {
			return op
		}
	}
	return ops[len(ops)-1]
}

func randomAnalyticsOp() int {
	return rand.Intn(numAnalyticsOps)
}

func startStats(ctx context.Context) error {
	var lastOps int
	ticker := time.NewTicker(statsInterval)
	for {
		select {
		case <-ticker.C:
			stats.Lock()
			opsPerSec := float64(stats.totalOps-lastOps) / float64(statsInterval/1E9)
			log.Printf("%d ops, %d no-user, %d no-photo, %d analytics, %d errs (%.2f/s)",
				stats.totalOps, stats.noUserOps, stats.noPhotoOps,
				stats.noAnalyticsOps, stats.failedOps, opsPerSec,
			)
			lastOps = stats.totalOps
			stats.Unlock()
		case <-ctx.Done():
			stats.Lock()
			if !stats.computing {
				stats.computing = true
				//showHistogram()
			}
			stats.Unlock()
			return ctx.Err()
		}
	}
}

// startAnalytics simulates periodic analytics queries until the context
// indicates it's time to exit.
func startAnalytics(ctx context.Context, cfg Config) error {
	for {
		opType := randomAnalyticsOp()
		if err := ctx.Err(); err != nil {
			return err
		}
		err := runAnalyticsOp(ctx, cfg, opType)
		stats.Lock()
		stats.totalOps++
		stats.noAnalyticsOps++
		stats.analyticsOpCounts[opType]++
		if err != nil {
			stats.failedOps++
			log.Printf("failed to run analytics op: %s: %s", opType, err)
		}
		stats.Unlock()

		time.Sleep(time.Second * time.Duration(cfg.AnalyticsQueriesWaitSeconds))
	}
}

// startUser simulates a stream of user events until the context indicates
// it's time to exit.
func startUser(ctx context.Context, cfg Config) error {
	h := fnv.New32()
	var buf [8]byte

	randomUser := func() int {
		// Use an exponential distribution to skew the user ID generation, but
		// hash the randomly generated value so that the "hot" users are spread
		// throughout the user ID key space (and thus not all on 1 range).
		binary.BigEndian.PutUint64(buf[:8], math.Float64bits(rand.ExpFloat64()/rate))
		h.Reset()
		h.Write(buf[:8])
		return int(h.Sum32())
	}

	for {
		userID := randomUser()
		op := randomOp()

		if err := ctx.Err(); err != nil {
			return err
		}
		err := runUserOp(ctx, cfg, userID, op.typ)
		stats.Lock()
		_ = stats.hist.RecordValue(int64(userID))
		stats.totalOps++
		stats.opCounts[op.typ]++
		switch {
		case err == errNoUser:
			stats.noUserOps++
		case err == errNoPhoto:
			stats.noPhotoOps++
		case err != nil:
			stats.failedOps++
			log.Printf("failed to run %s op for %d: %s", op.name, userID, err)
		}
		stats.Unlock()
	}
}

// runUserOp starts a transaction and creates the user if it doesn't
// yet exist.
func runUserOp(ctx context.Context, cfg Config, userID, opType int) error {
	return crdb.ExecuteTx(cfg.DB, func(tx *sql.Tx) error {
		switch opType {
		case createUserOp:
			return createUser(ctx, tx, userID)
		case createPhotoOp:
			return createPhoto(ctx, tx, userID)
		case createCommentOp:
			return createComment(ctx, tx, userID)
		case listPhotosOp:
			return listPhotos(ctx, tx, userID, nil)
		case listCommentsOp:
			_, err := listComments(ctx, tx, userID, nil)
			return err
		case updatePhotoOp:
			return updatePhoto(ctx, tx, userID)
		case updateCommentOp:
			return updateComment(ctx, tx, userID)
		case deletePhotoOp:
			return deletePhoto(ctx, tx, userID)
		case deleteCommentOp:
			return deleteComment(ctx, tx, userID)
		case listMostCommentedPhotosOp:
			return listMostCommentedPhotos(ctx, tx, userID)
		case listCommentsAlphabeticallyOp:
			return listCommentsAlphabetically(ctx, tx, userID)
		default:
			return errors.Errorf("unsupported op type: %d", opType)
		}
	})
}

func runAnalyticsOp(ctx context.Context, cfg Config, analyticsOpType int) error {
	return crdb.ExecuteTx(cfg.DB, func(tx *sql.Tx) error {
		return analyticsQuery(ctx, tx, analyticsOpType)
	})
}

func showHistogram() {
	log.Printf("**** histogram of user op counts (minUserID=%d, maxUserID=%d, userCount=%d)",
		stats.hist.Min(), stats.hist.Max(), stats.hist.TotalCount())
	for _, b := range stats.hist.Distribution() {
		log.Printf("** users %d-%d (%d)", b.From, b.To, b.Count)
	}
}
