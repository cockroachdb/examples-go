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
	"errors"
	"log"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/util"
	_ "github.com/lib/pq"
)

const (
	// TODO(spencer): update the CREATE DATABASE statement in the schema
	//   to pull out the database specified in the DB URL and use it instead
	//   of "photos" below.
	photosSchema = `
CREATE DATABASE IF NOT EXISTS photos;

CREATE TABLE IF NOT EXISTS users (
  id           INT,
  photoCount   INT,
  commentCount INT,
  name         STRING,
  address      STRING,

  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS photos (
  id           BYTES DEFAULT EXPERIMENTAL_UUID_V4(),
  userID       INT,
  commentCount INT,
  caption      STRING,
  latitude     FLOAT,
  longitude    FLOAT,
  timestamp    TIMESTAMP,

  PRIMARY KEY (id)
);

CREATE UNIQUE INDEX IF NOT EXISTS photosByUserID ON photos (userID, timestamp);

CREATE TABLE IF NOT EXISTS comments (
  photoID   BYTES,
  commentID BYTES DEFAULT EXPERIMENTAL_UUID_V4(),
  userID    INT,
  message   STRING,
  timestamp TIMESTAMP,

  PRIMARY KEY (photoID, commentID)
);

CREATE UNIQUE INDEX IF NOT EXISTS commentsByPhotoID ON comments (photoID, timestamp);
`
)

// openDB opens the database connection according to the context.
func openDB(ctx Context) (*sql.DB, error) {
	return sql.Open("postgres", ctx.DBUrl)
}

// initSchema creates the database schema if it doesn't exist.
func initSchema(db *sql.DB) error {
	_, err := db.Exec(photosSchema)
	return err
}

// dropDatabase drops the database.
func dropDatabase(db *sql.DB) error {
	_, err := db.Exec("DROP DATABASE IF EXISTS photos;")
	return err
}

// execute runs a closure within a transaction. On failure,
// the transaction is aborted and rolled back; on success,
// the transaction is committed. If the error string matches
// CockroachDB's restart directive, the same transaction is
// re-used to retry executing the closure.
//
// NOTE: the supplied exec closure should not have external
//       side effects beyond changes to the database.
func executeTxn(db *sql.DB, exec func(*sql.Tx) error) error {
	tx, err := ctx.DB.Begin()
	if err != nil {
		return err
	}
	for {
		if err := exec(tx); err != nil {
			// TODO(spencer): check error return here and if retry txn is
			// warranted, continue.
			/*
				if err.Error().Matches("retry cockroachdb txn") {
					if _, err = tx.Exec(`RETRY TRANSACTION;`); err == nil {
						continue
					}
				}
			*/
			_ = tx.Rollback()
			return err
		}
		return tx.Commit()
	}
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func randString(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

var noUserErr = errors.New("no user found")

// userExists looks up a user by ID.
func userExists(tx *sql.Tx, userID int) (bool, error) {
	var id int
	const selectSQL = `
SELECT id FROM users WHERE id = $1;
`
	err := tx.QueryRow(selectSQL, userID).Scan(&id)
	switch err {
	case sql.ErrNoRows:
		return false, nil
	case nil:
		return true, nil
	default:
		return false, err
	}
}

// findClosestUserByID selects the first user which exists with
// id >= userID. Returns the found user ID or an error.
func findClosestUserByID(tx *sql.Tx, userID int) (int, error) {
	var id int
	const selectSQL = `
SELECT id FROM users WHERE id >= $1 ORDER BY id LIMIT 1;
`
	err := tx.QueryRow(selectSQL, userID).Scan(&id)
	if err == sql.ErrNoRows {
		return 0, noUserErr
	}
	return id, err
}

// createUser creates a new user with random name and address strings.
func createUser(tx *sql.Tx, userID int) error {
	exists, err := userExists(tx, userID)
	if err != nil || exists {
		return err
	}
	const insertSQL = `
INSERT INTO users VALUES ($1, 0, 0, $2, $3);
`
	const minNameLen = 1
	const maxNameLen = 30
	const minAddrLen = 20
	const maxAddrLen = 100
	name := randString(minNameLen + rand.Intn(maxNameLen-minNameLen))
	addr := randString(minAddrLen + rand.Intn(maxAddrLen-minAddrLen))
	_, err = tx.Exec(insertSQL, userID, name, addr)
	return err
}

// createPhoto looks up or creates a new user to match userID (it's
// the only method in this interface which doesn't match an existing
// user except for createUser). It then creates a new photo for the
// new or pre-existing user.
func createPhoto(tx *sql.Tx, userID int) error {
	if err := createUser(tx, userID); err != nil {
		return err
	}

	const insertSQL = `
INSERT INTO photos VALUES (DEFAULT, $1, 0, $2, $3, $4, NOW());
`
	const minCaptionLen = 10
	const maxCaptionLen = 200
	caption := randString(minCaptionLen + rand.Intn(maxCaptionLen-minCaptionLen))
	latitude := rand.Float32() * 90
	longitude := rand.Float32() * 180
	if _, err := tx.Exec(insertSQL, userID, caption, latitude, longitude); err != nil {
		return err
	}

	const updateSQL = `
UPDATE users SET photoCount = photoCount + 1 WHERE id = $1;
`
	if _, err := tx.Exec(updateSQL, userID); err != nil {
		return err
	}
	return nil
}

// createComment chooses a random photo from a user with the closest
// matching user ID and generates a random author ID to author the
// comment. Counts are updated on the photo and author user.
func createComment(tx *sql.Tx, userID int) error {
	photoID, err := chooseRandomPhoto(tx, userID)
	if err != nil {
		return err
	}
	authorID := rand.Intn(userID) + 1

	const insertSQL = `
INSERT INTO comments VALUES ($1, DEFAULT, $2, $3, NOW());
`
	const minMessageLen = 32
	const maxMessageLen = 1024
	message := randString(minMessageLen + rand.Intn(maxMessageLen-minMessageLen))
	if _, err := tx.Exec(insertSQL, photoID, authorID, message); err != nil {
		log.Printf("insert into photos failed: %s", err)
		return err
	}

	const updatePhotoSQL = `
UPDATE photos SET commentCount = commentCount + 1 WHERE id = $1;
`
	if _, err := tx.Exec(updatePhotoSQL, photoID); err != nil {
		return err
	}

	const updateUserSQL = `
UPDATE users SET commentCount = commentCount + 1 WHERE id = $1;
`
	if _, err := tx.Exec(updateUserSQL, authorID); err != nil {
		return err
	}
	return nil
}

// listPhotos queries up to 100 photos, sorted by timestamp in
// descending order, for the first user with ID >= userID. If photoIDs
// is not nil, stores the queried photo IDs in photoIDs.
func listPhotos(tx *sql.Tx, userID int, photoIDs *[][]byte) error {
	var err error
	userID, err = findClosestUserByID(tx, userID)
	if err != nil {
		return err
	}
	const selectSQL = `
SELECT id, caption, commentCount, latitude, longitude, timestamp FROM photos WHERE userID = $1 ORDER BY timestamp DESC LIMIT 100`
	rows, err := tx.Query(selectSQL, userID)
	switch {
	case err == sql.ErrNoRows:
		return nil
	case err != nil:
		return err
	}
	defer rows.Close()
	// Count and process the result set so we make sure work is done to
	// stream the results.
	var count int
	for rows.Next() {
		if err := rows.Err(); err != nil {
			return err
		}
		var id []byte
		var caption string
		var cCount int
		var lat, lon float64
		var ts time.Time
		if err := rows.Scan(&id, &caption, &cCount, &lat, &lon, &ts); err != nil {
			return util.Errorf("failed to scan result set for user %d: %s", userID, err)
		}
		count++
		if photoIDs != nil {
			*photoIDs = append(*photoIDs, id)
		}
	}
	//log.Printf("selected %d photos for user %d", count, userID)
	return nil
}

// chooseRandomPhoto selects a random photo for the specified
// user or an existing user with the closest user ID. Returns
// the photo ID or an error.
func chooseRandomPhoto(tx *sql.Tx, userID int) ([]byte, error) {
	photoIDs := [][]byte{}
	if err := listPhotos(tx, userID, &photoIDs); err != nil {
		return nil, err
	}
	if len(photoIDs) == 0 {
		return nil, nil
	}
	photoID := photoIDs[rand.Intn(len(photoIDs))]
	return photoID, nil
}

// listComments chooses a random photo and lists up to 100 of its
// comments. Returns the photoID or an error. If the commentIDs slice
// is not nil, it's set to the queried comments' IDs.
func listComments(tx *sql.Tx, userID int, commentIDs *[][]byte) ([]byte, error) {
	photoID, err := chooseRandomPhoto(tx, userID)
	if err != nil {
		return nil, err
	}
	const selectSQL = `SELECT commentID, userID, message, timestamp FROM comments ` +
		`WHERE photoID = $1 AND commentID IN ` +
		`(SELECT commentID FROM comments WHERE photoID = $1 ORDER BY timestamp DESC LIMIT 100)` +
		`ORDER BY timestamp DESC`
	rows, err := tx.Query(selectSQL, photoID)
	switch {
	case err == sql.ErrNoRows:
		return photoID, nil
	case err != nil:
		return nil, err
	}
	defer rows.Close()
	// Count and process the result set so we make sure work is done to
	// stream the results.
	var count int
	for rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, err
		}
		var commentID []byte
		var message string
		var userID int
		var ts time.Time
		if err := rows.Scan(&commentID, &userID, &message, &ts); err != nil {
			return nil, util.Errorf("failed to scan result set for photo %q: %s", photoID, err)
		}
		count++
		if commentIDs != nil {
			*commentIDs = append(*commentIDs, commentID)
		}
	}
	//log.Printf("selected %d comments for photo %q", count, photoID)
	return photoID, nil
}

// chooseRandomComment selects a random comment for the specified
// user or an existing user with the closest user ID. Returns
// the photo and comment IDs or an error.
func chooseRandomComment(tx *sql.Tx, userID int) ([]byte, []byte, error) {
	commentIDs := [][]byte{}
	photoID, err := listComments(tx, userID, &commentIDs)
	if err != nil {
		return nil, nil, err
	}
	if len(commentIDs) == 0 {
		return photoID, nil, nil
	}
	commentID := commentIDs[rand.Intn(len(commentIDs))]
	return photoID, commentID, nil
}

func updatePhoto(tx *sql.Tx, userID int) error {
	photoID, err := chooseRandomPhoto(tx, userID)
	if err != nil {
		return err
	}

	const updatePhotoSQL = `
UPDATE photos SET caption = $1 WHERE id = $2;
`
	const minCaptionLen = 10
	const maxCaptionLen = 200
	caption := randString(minCaptionLen + rand.Intn(maxCaptionLen-minCaptionLen))
	if _, err := tx.Exec(updatePhotoSQL, caption, photoID); err != nil {
		return err
	}
	return nil
}

func updateComment(tx *sql.Tx, userID int) error {
	photoID, commentID, err := chooseRandomComment(tx, userID)
	if err != nil {
		return err
	}

	const updateCommentSQL = `
UPDATE comments SET message = $1 WHERE photoID = $2 AND commentID = $3;
`
	const minMessageLen = 10
	const maxMessageLen = 200
	message := randString(minMessageLen + rand.Intn(maxMessageLen-minMessageLen))
	if _, err := tx.Exec(updateCommentSQL, message, photoID, commentID); err != nil {
		return err
	}
	return nil
}

func deletePhoto(tx *sql.Tx, userID int) error {
	photoID, err := chooseRandomPhoto(tx, userID)
	if err != nil {
		return err
	}
	const deletePhotoSQL = `
DELETE FROM photos WHERE id = $1;
`
	if _, err := tx.Exec(deletePhotoSQL, photoID); err != nil {
		return err
	}

	const updateSQL = `
UPDATE users SET photoCount = photoCount - 1 WHERE id = $1;
`
	if _, err := tx.Exec(updateSQL, userID); err != nil {
		return err
	}
	return nil
}

func deleteComment(tx *sql.Tx, userID int) error {
	photoID, commentID, err := chooseRandomComment(tx, userID)
	if err != nil {
		return err
	}
	const deleteCommentSQL = `
DELETE FROM comments WHERE photoID = $1 AND commentID = $2;
`
	if _, err := tx.Exec(deleteCommentSQL, photoID, commentID); err != nil {
		return err
	}

	const updatePhotoSQL = `
UPDATE photos SET commentCount = commentCount - 1 WHERE id = $1;
`
	if _, err := tx.Exec(updatePhotoSQL, photoID); err != nil {
		return err
	}

	const updateUserSQL = `
UPDATE users SET commentCount = commentCount - 1 WHERE id = $1;
`
	if _, err := tx.Exec(updateUserSQL, userID); err != nil {
		return err
	}
	return nil
}
