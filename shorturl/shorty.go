package main

import (
	"fmt"
	"log"
	"regexp"
	"strconv"
	"time"
)

const (
	// the first auto shorty: reserve all urls < 5 chars.
	initialCounter = "10000"
	// allowed characters (for custom shortys).
	validChars      = "0123456789abcdefghijklmnopqrstuvwxyz_.-~"
	shortyMaxLength = 20
)

var (
	validShortyRE   = regexp.MustCompile(fmt.Sprintf(`^[%s]+$`, validChars))
	reservedShortys = map[string]struct{}{
		"pub": {},
	}
)

// validateCounter returns an error if the counter fails parsing.
func validateCounter(c string) error {
	_, err := parseCounter(c)
	return err
}

// parseCounter parses a string counter into an integer.
func parseCounter(c string) (int64, error) {
	return strconv.ParseInt(c, 36, 64)
}

// checkParseCounter parses a counter and crashes on errors.
func checkParseCounter(c string) int64 {
	ret, err := parseCounter(c)
	if err != nil {
		log.Fatalf("failed to parse counter %q: %v", c, err)
	}
	return ret
}

func counterFromInt(c int64) string {
	return strconv.FormatInt(c, 36)
}

// validateShortURL verifies that a short URL is valid.
func validateShortURL(s string) error {
	if len(s) == 0 {
		return fmt.Errorf("short URL cannot be blank")
	}
	if len(s) > shortyMaxLength {
		return fmt.Errorf("short URL cannot be longer than %d characters", shortyMaxLength)
	}
	if !validShortyRE.MatchString(s) {
		return fmt.Errorf("short URL must only contain the characters: %q", validChars)
	}
	return nil
}

type Shorty struct {
	ShortURL  string
	LongURL   string
	Reserved  bool
	Custom    bool
	Public    bool
	DateAdded time.Time
	AddedBy   string
}
