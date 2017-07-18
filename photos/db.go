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
	"log"
	"math/rand"
	"time"

	// Import postgres driver.
	_ "github.com/lib/pq"

	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

var errNoUser = errors.New("no user found")
var errNoPhoto = errors.New("no photos found")

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
  id           BYTES DEFAULT uuid_v4(),
  userID       INT,
  commentCount INT,
  caption      STRING,
  latitude     FLOAT,
  longitude    FLOAT,
  timestamp    TIMESTAMP,

  PRIMARY KEY (id),
  UNIQUE INDEX byUserID (userID, timestamp)
);

CREATE TABLE IF NOT EXISTS comments (
  -- length check guards against insertion of empty photo ID.
  -- TODO(bdarnell): consider replacing length check with foreign key.
  -- Start with the length check because it's local; we'll want to keep
  -- an eye on performance when introducing the FK.
  photoID   BYTES CHECK (length(photoID) = 16),
  commentID BYTES DEFAULT uuid_v4(),
  userID    INT,
  message   STRING,
  timestamp TIMESTAMP,

  PRIMARY KEY (photoID, timestamp, commentID)
);`
)

// openDB opens the database connection according to the context.
func openDB(cfg Config) (*sql.DB, error) {
	return sql.Open("postgres", cfg.DBUrl)
}

// initSchema creates the database schema if it doesn't exist.
func initSchema(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, photosSchema)
	return err
}

// dropDatabase drops the database.
func dropDatabase(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, "DROP DATABASE IF EXISTS photos;")
	return err
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func randString(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

// userExists looks up a user by ID.
func userExists(ctx context.Context, tx *sql.Tx, userID int) (bool, error) {
	var id int
	const selectSQL = `
SELECT id FROM users WHERE id = $1;
`
	err := tx.QueryRowContext(ctx, selectSQL, userID).Scan(&id)
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
func findClosestUserByID(ctx context.Context, tx *sql.Tx, userID int) (int, error) {
	var id int
	const selectSQL = `
SELECT id FROM users WHERE id >= $1 ORDER BY id LIMIT 1;
`
	err := tx.QueryRowContext(ctx, selectSQL, userID).Scan(&id)
	if err == sql.ErrNoRows {
		return 0, errNoUser
	}
	return id, err
}

// createUser creates a new user with random name and address strings.
func createUser(ctx context.Context, tx *sql.Tx, userID int) error {
	exists, err := userExists(ctx, tx, userID)
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
	_, err = tx.ExecContext(ctx, insertSQL, userID, name, addr)
	return err
}

// createPhoto looks up or creates a new user to match userID (it's
// the only method in this interface which doesn't match an existing
// user except for createUser). It then creates a new photo for the
// new or pre-existing user.
func createPhoto(ctx context.Context, tx *sql.Tx, userID int) error {
	if err := createUser(ctx, tx, userID); err != nil {
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
	if _, err := tx.ExecContext(ctx, insertSQL, userID, caption, latitude, longitude); err != nil {
		return err
	}

	const updateSQL = `
UPDATE users SET photoCount = photoCount + 1 WHERE id = $1;
`
	if _, err := tx.ExecContext(ctx, updateSQL, userID); err != nil {
		return err
	}
	return nil
}

// createComment chooses a random photo from a user with the closest
// matching user ID and generates a random author ID to author the
// comment. Counts are updated on the photo and author user.
func createComment(ctx context.Context, tx *sql.Tx, userID int) error {
	photoID, err := chooseRandomPhoto(ctx, tx, userID)
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
	if _, err := tx.ExecContext(ctx, insertSQL, photoID, authorID, message); err != nil {
		log.Printf("insert into photos failed: %s", err)
		return err
	}

	const updatePhotoSQL = `
UPDATE photos SET commentCount = commentCount + 1 WHERE id = $1;
`
	if _, err := tx.ExecContext(ctx, updatePhotoSQL, photoID); err != nil {
		return err
	}

	const updateUserSQL = `
UPDATE users SET commentCount = commentCount + 1 WHERE id = $1;
`
	if _, err := tx.ExecContext(ctx, updateUserSQL, authorID); err != nil {
		return err
	}
	return nil
}

// listPhotos queries up to 100 photos, sorted by timestamp in
// descending order, for the first user with ID >= userID. If photoIDs
// is not nil, stores the queried photo IDs in photoIDs.
func listPhotos(ctx context.Context, tx *sql.Tx, userID int, photoIDs *[][]byte) error {
	var err error
	userID, err = findClosestUserByID(ctx, tx, userID)
	if err != nil {
		return err
	}
	const selectSQL = `
SELECT id, caption, commentCount, latitude, longitude, timestamp FROM photos WHERE userID = $1 ORDER BY timestamp DESC LIMIT 100`
	rows, err := tx.QueryContext(ctx, selectSQL, userID)
	switch {
	case err == sql.ErrNoRows:
		return nil
	case err != nil:
		return err
	}
	defer func() { _ = rows.Close() }()
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
			return errors.Errorf("failed to scan result set for user %d: %s", userID, err)
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
func chooseRandomPhoto(ctx context.Context, tx *sql.Tx, userID int) ([]byte, error) {
	photoIDs := [][]byte{}
	if err := listPhotos(ctx, tx, userID, &photoIDs); err != nil {
		return nil, err
	}
	if len(photoIDs) == 0 {
		return nil, errNoPhoto
	}
	photoID := photoIDs[rand.Intn(len(photoIDs))]
	return photoID, nil
}

// listComments chooses a random photo and lists up to 100 of its
// comments. Returns the photoID or an error. If the commentIDs slice
// is not nil, it's set to the queried comments' IDs.
func listComments(ctx context.Context, tx *sql.Tx, userID int, commentIDs *[][]byte) ([]byte, error) {
	photoID, err := chooseRandomPhoto(ctx, tx, userID)
	if err != nil {
		return nil, err
	}
	const selectSQL = `SELECT commentID, userID, message, timestamp FROM comments ` +
		`WHERE photoID = $1 ORDER BY timestamp DESC LIMIT 100`
	rows, err := tx.QueryContext(ctx, selectSQL, photoID)
	switch {
	case err == sql.ErrNoRows:
		return photoID, nil
	case err != nil:
		return nil, err
	}
	defer func() { _ = rows.Close() }()
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
			return nil, errors.Errorf("failed to scan result set for photo %q: %s", photoID, err)
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
func chooseRandomComment(ctx context.Context, tx *sql.Tx, userID int) ([]byte, []byte, error) {
	commentIDs := [][]byte{}
	photoID, err := listComments(ctx, tx, userID, &commentIDs)
	if err != nil {
		return nil, nil, err
	}
	if len(commentIDs) == 0 {
		return photoID, nil, nil
	}
	commentID := commentIDs[rand.Intn(len(commentIDs))]
	return photoID, commentID, nil
}

func updatePhoto(ctx context.Context, tx *sql.Tx, userID int) error {
	photoID, err := chooseRandomPhoto(ctx, tx, userID)
	if err != nil {
		return err
	}

	const updatePhotoSQL = `
UPDATE photos SET caption = $1 WHERE id = $2;
`
	const minCaptionLen = 10
	const maxCaptionLen = 200
	caption := randString(minCaptionLen + rand.Intn(maxCaptionLen-minCaptionLen))
	if _, err := tx.ExecContext(ctx, updatePhotoSQL, caption, photoID); err != nil {
		return err
	}
	return nil
}

func updateComment(ctx context.Context, tx *sql.Tx, userID int) error {
	photoID, commentID, err := chooseRandomComment(ctx, tx, userID)
	if err != nil {
		return err
	}

	const updateCommentSQL = `
UPDATE comments SET message = $1 WHERE photoID = $2 AND commentID = $3;
`
	const minMessageLen = 10
	const maxMessageLen = 200
	message := randString(minMessageLen + rand.Intn(maxMessageLen-minMessageLen))
	if _, err := tx.ExecContext(ctx, updateCommentSQL, message, photoID, commentID); err != nil {
		return err
	}
	return nil
}

func deletePhoto(ctx context.Context, tx *sql.Tx, userID int) error {
	photoID, err := chooseRandomPhoto(ctx, tx, userID)
	if err != nil {
		return err
	}
	const deletePhotoSQL = `
DELETE FROM photos WHERE id = $1;
`
	if _, err := tx.ExecContext(ctx, deletePhotoSQL, photoID); err != nil {
		return err
	}

	const updateSQL = `
UPDATE users SET photoCount = photoCount - 1 WHERE id = $1;
`
	if _, err := tx.ExecContext(ctx, updateSQL, userID); err != nil {
		return err
	}
	return nil
}

func deleteComment(ctx context.Context, tx *sql.Tx, userID int) error {
	photoID, commentID, err := chooseRandomComment(ctx, tx, userID)
	if err != nil {
		return err
	}
	const deleteCommentSQL = `
DELETE FROM comments WHERE photoID = $1 AND commentID = $2;
`
	if _, err := tx.ExecContext(ctx, deleteCommentSQL, photoID, commentID); err != nil {
		return err
	}

	const updatePhotoSQL = `
UPDATE photos SET commentCount = commentCount - 1 WHERE id = $1;
`
	if _, err := tx.ExecContext(ctx, updatePhotoSQL, photoID); err != nil {
		return err
	}

	const updateUserSQL = `
UPDATE users SET commentCount = commentCount - 1 WHERE id = $1;
`
	if _, err := tx.ExecContext(ctx, updateUserSQL, userID); err != nil {
		return err
	}
	return nil
}

// listMostCommentedPhotos queries the top 100 most commented on photos for the
// first user with ID >= userID.
func listMostCommentedPhotos(ctx context.Context, tx *sql.Tx, userID int) error {
	var err error
	userID, err = findClosestUserByID(ctx, tx, userID)
	if err != nil {
		return err
	}
	const selectSQL = `
SELECT id, caption, commentCount, latitude, longitude, timestamp
FROM photos
WHERE userID = $1
ORDER BY commentcount DESC LIMIT 100
	`
	rows, err := tx.QueryContext(ctx, selectSQL, userID)
	switch {
	case err == sql.ErrNoRows:
		return nil
	case err != nil:
		return err
	}
	defer func() { _ = rows.Close() }()

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
			return errors.Errorf("failed to scan result set for user %d: %s", userID, err)
		}
	}

	return nil
}

// listCommentsAlphabeticallyOp retrieves the first 100 comments for a photo
// in alphabetical order for the first user with ID >= userID. This query is
// semantically useless but tests sorting by a non-indexed column, which
// triggers distributed SQL.
func listCommentsAlphabetically(ctx context.Context, tx *sql.Tx, userID int) error {
	photoID, err := chooseRandomPhoto(ctx, tx, userID)
	if err != nil {
		return err
	}
	const selectSQL = `
	SELECT commentID, userID, message, timestamp
	FROM comments
	WHERE photoID = $1
	ORDER BY message LIMIT 100
	`
	rows, err := tx.QueryContext(ctx, selectSQL, photoID)
	switch {
	case err == sql.ErrNoRows:
		return nil
	case err != nil:
		return err
	}
	defer func() { _ = rows.Close() }()

	// Process all rows.
	for rows.Next() {
		if err := rows.Err(); err != nil {
			return err
		}
		var commentID []byte
		var message string
		var userID int
		var ts time.Time
		if err := rows.Scan(&commentID, &userID, &message, &ts); err != nil {
			return errors.Errorf("failed to scan result set for photo %q: %s", photoID, err)
		}
	}

	return nil
}

const (
	// top10Commenters scans all the comments, grouping by userid, to find the
	// top commenters. Note that this query is artificial, as this value can be
	// queried directly from the user table.
	selectTop10Commenters = `SELECT count(*) AS post_count FROM comments GROUP BY userid ORDER BY post_count DESC LIMIT 10;`
	// top10Posters scans all the photos, grouping by userid, to find the
	// top photo posters. Note that this query is artificial, as this value can
	// be queried directly from the user table.
	selectTop10Posters = `SELECT count(*) AS photos_count FROM photos GROUP BY userid ORDER BY photos_count DESC LIMIT 10;`
	// top10Photos scans all the photos ordered by commentcount, to find the most
	// commented photos. It does this directly on the photos table.
	selectTop10Photos = `SELECT commentcount FROM photos ORDER BY commentcount DESC LIMIT 10`
	// top10PhotoPostersNames finds the top photos, but joins this on the users table
	// to return the names of the users.
	selectTop10PhotoPostersNames = `SELECT users.name FROM photos JOIN users ON userid = userid ORDER BY photos.commentcount DESC LIMIT 10;`
)

// analyticsQuery runs the selected
func analyticsQuery(ctx context.Context, tx *sql.Tx, analyticsOpType int) error {
	var selectSQL string
	var outputTypeString bool
	switch analyticsOpType {
	case topCommentersAnalyticsOp:
		selectSQL = selectTop10Commenters
	case topPostersAnalyticsOp:
		selectSQL = selectTop10Posters
	case topPhotosAnalyticsOp:
		selectSQL = selectTop10Photos
	case top10PhotoPostersNamesAnalytcsOp:
		selectSQL = selectTop10PhotoPostersNames
		outputTypeString = true
	}

	rows, err := tx.QueryContext(ctx, selectSQL)
	switch {
	case err == sql.ErrNoRows:
		return nil
	case err != nil:
		return err
	}

	for rows.Next() {
		if err := rows.Err(); err != nil {
			return err
		}
		if outputTypeString {
			var user string
			if err := rows.Scan(&user); err != nil {
				return errors.Errorf("failed to scan string result set for query '%s': %s",
					selectSQL, err)
			}
		} else {
			var count int
			if err := rows.Scan(&count); err != nil {
				return errors.Errorf("failed to scan int result set for query '%s': %s",
					selectSQL, err)
			}
		}
	}

	return nil
}
