package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"

	// Hook up profiling handlers.
	_ "net/http/pprof"
)

var httpHost = flag.String("host", "localhost", "Interface host to listen on")
var httpPort = flag.Int("port", 8080, "Port to listen on")

var createDB = flag.Bool("create", true, "Attempt to create the DB if it does not exist. Root required.")
var dropDB = flag.Bool("drop", false, "Drop tables at startup")

var requireEmail = flag.Bool("require-email", true, "Require X-Forwarded-Email for modifications")

const (
	publicSubDir = "pub/"
	defaultDBURL = "postgresql://root@localhost:26257/shorty?sslmode=disable"
)

var usage = func() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s <db URL>\n\n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	flag.Usage = usage
	flag.Parse()

	dbURL := defaultDBURL
	if flag.NArg() == 1 {
		dbURL = flag.Arg(0)
	}

	if err := SetupDB(dbURL); err != nil {
		log.Fatal(err)
	}

	SetupHandlers()

	hostAddr := fmt.Sprintf("%s:%d", *httpHost, *httpPort)
	log.Printf("Starting http server on %s", hostAddr)
	log.Fatal(http.ListenAndServe(hostAddr, nil))
}
