package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/url"

	// Import postgres driver.
	"github.com/cockroachdb/cockroach-go/crdb"
	_ "github.com/lib/pq"
)

var shortyDB *sql.DB

const (
	urlTableSchema = `
	  CREATE TABLE IF NOT EXISTS urls (
			id STRING PRIMARY KEY,
			url STRING DEFAULT '' NOT NULL,
			reserved BOOL DEFAULT FALSE NOT NULL,
			custom BOOL DEFAULT FALSE NOT NULL,
			public BOOL DEFAULT FALSE NOT NULL,
			date_added TIMESTAMP DEFAULT CLOCK_TIMESTAMP() NOT NULL,
			added_by STRING DEFAULT '' NOT NULL
		)`

	counterTableSchema = `
	  CREATE TABLE IF NOT EXISTS counters (
			id STRING PRIMARY KEY,
			counter INT NOT NULL
		)`

	counterName = "CURRENT"
)

// SetupDB initializes the DB object.
func SetupDB(dbURL string) error {
	parsedURL, err := url.Parse(dbURL)
	if err != nil {
		return err
	}

	if len(parsedURL.Path) == 1 {
		return fmt.Errorf("connection url must specify database name")
	}

	dbName := parsedURL.Path[1:]
	log.Printf("Using database: %s", dbName)

	// Open connection to server and create a database.
	db, err := sql.Open("postgres", parsedURL.String())
	if err != nil {
		return err
	}

	if *createDB {
		if _, err := db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", dbName)); err != nil {
			return err
		}
	}

	if _, err := db.Exec(fmt.Sprintf("SET DATABASE=%s", dbName)); err != nil {
		return err
	}

	if *dropDB {
		if _, err := db.Exec(`DROP TABLE IF EXISTS urls`); err != nil {
			return err
		}
		if _, err := db.Exec(`DROP TABLE IF EXISTS counters`); err != nil {
			return err
		}
	}

	if _, err := db.Exec(urlTableSchema); err != nil {
		return err
	}

	if _, err := db.Exec(counterTableSchema); err != nil {
		return err
	}

	// Write the first counter if not present.
	// We intentionally fail on errors other than "duplicate key".
	if _, err := db.Exec(`INSERT INTO counters VALUES ($1, $2) ON CONFLICT (id) DO NOTHING`,
		counterName, checkParseCounter(initialCounter)); err != nil {
		return err
	}

	// Write all reserved shortys.
	// WARNING: if new ones are added, they may overwrite existing shortys.
	for s := range reservedShortys {
		if _, err := db.Exec(`INSERT INTO urls (id, reserved) VALUES ($1, TRUE) ON CONFLICT (id) DO UPDATE SET reserved = TRUE`,
			s); err != nil {
			return err
		}
	}

	shortyDB = db
	return nil
}

// sqlExecutor is an interface needed for basic queries.
// It is implemented by both sql.DB and sql.Txn.
type sqlExecutor interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
}

func getShorty(shortURL string) (*Shorty, error) {
	if shortyDB == nil {
		log.Fatal("getShorty called before DB setup")
	}
	if err := validateShortURL(shortURL); err != nil {
		return nil, err
	}
	res := &Shorty{ShortURL: shortURL}
	err := shortyDB.QueryRow(
		`SELECT url, reserved, custom, public, date_added, added_by FROM urls WHERE id = $1`,
		shortURL).Scan(&res.LongURL,
		&res.Reserved,
		&res.Custom,
		&res.Public,
		&res.DateAdded,
		&res.AddedBy)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	// Reserved is the same as "not found"
	if res.Reserved {
		return nil, nil
	}

	return res, nil
}

// getShortysByOwner returns the list of shortys that have
// an 'added by' field matching 'owner'.
// Specify the empty string to get anonymous shortys.
// Does not return "reserved" keys.
// TODO(marc): we should make an index for this.
func getShortysByOwner(owner string) ([]*Shorty, error) {
	res := make([]*Shorty, 0, 0)
	rows, err := shortyDB.Query(`SELECT id, url, public, date_added FROM urls WHERE added_by = $1 AND reserved = false ORDER BY date_added DESC`,
		owner)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		shorty := &Shorty{}
		if err := rows.Scan(&shorty.ShortURL, &shorty.LongURL, &shorty.Public, &shorty.DateAdded); err != nil {
			return nil, err
		}
		res = append(res, shorty)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return res, nil
}

func getCounter(e sqlExecutor) (int64, error) {
	var counter int64
	err := e.QueryRow(`SELECT counter FROM counters WHERE id = $1`, counterName).Scan(&counter)
	return counter, err
}

func saveCounter(e sqlExecutor, counter int64) error {
	_, err := e.Exec(`UPDATE counters SET counter = $1 WHERE id = $2`, counter, counterName)
	return err
}

func shortyExists(e sqlExecutor, shortURL string) (bool, error) {
	var exists bool
	err := e.QueryRow(`SELECT EXISTS (SELECT id FROM urls WHERE id = $1)`, shortURL).Scan(&exists)
	return exists, err
}

func insertShorty(e sqlExecutor, shorty Shorty) error {
	_, err := e.Exec(`INSERT INTO urls (id, url, custom, public, added_by) VALUES ($1, $2, $3, $4, $5)`,
		shorty.ShortURL,
		shorty.LongURL,
		shorty.Custom,
		shorty.Public,
		shorty.AddedBy)
	return err
}

func addNewShorty(shorty Shorty) (string, error) {
	isCustom := shorty.Custom
	txErr := crdb.ExecuteTx(shortyDB, func(tx *sql.Tx) error {
		var err error
		var counter int64
		if !isCustom {
			// Initialize counter for automatic generation.
			if counter, err = getCounter(tx); err != nil {
				return err
			}
		}

		for {
			if !isCustom {
				// Make URL from counter.
				shorty.ShortURL = counterFromInt(counter)
				counter++
			}

			// Check if the short URL exists.
			var exists bool
			if exists, err = shortyExists(tx, shorty.ShortURL); err != nil {
				return err
			} else if exists {
				if isCustom {
					return fmt.Errorf("short URL %s already exists", shorty.ShortURL)
				}
				continue
			}

			// Insert new URL.
			if err = insertShorty(tx, shorty); err != nil {
				return err
			}

			if !isCustom {
				// Make sure we save the latest counter.
				if err := saveCounter(tx, counter); err != nil {
					return err
				}
			}
			return nil
		}
	})

	if txErr != nil {
		return "", txErr
	}

	return shorty.ShortURL, nil
}
