package main

import (
	"fmt"
	"log"
	"net/http"
	"net/url"
)

// SetupHandlers sets up the http handlers.
func SetupHandlers() {
	http.HandleFunc("/", handleRoot)
	http.HandleFunc(fmt.Sprintf("/%s", publicSubDir), handlePublic)
}

func handlePublic(rw http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		http.Error(rw, "invalid method", http.StatusMethodNotAllowed)
		return
	}

	if len(req.URL.RawQuery) != 0 {
		// No query parameters allowed on public URL.
		// This still lets through '/pub/<something>?'
		http.Error(rw, "invalid request", http.StatusBadRequest)
		return
	}

	path := req.URL.Path[len(publicSubDir)+1:]
	handleRedirect(rw, req, path, true /* isPublic */)
}

func handleRoot(rw http.ResponseWriter, req *http.Request) {
	path := req.URL.Path
	if path == "/" {
		handleSettings(rw, req)
		return
	}
	path = path[1:]
	handleRedirect(rw, req, path, false /* isPublic */)
}

func handleRedirect(rw http.ResponseWriter, req *http.Request, shortURL string, isPublic bool) {
	if err := validateShortURL(shortURL); err != nil {
		http.Error(rw, "invalid short URL", http.StatusBadRequest)
		return
	}

	shorty, err := getShorty(shortURL)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}

	if shorty == nil || (isPublic && !shorty.Public) {
		http.Error(rw, fmt.Sprintf("short URL %q not found", shortURL), http.StatusNotFound)
		return
	}

	http.Redirect(rw, req, shorty.LongURL, http.StatusFound)
}

func handleSettings(rw http.ResponseWriter, req *http.Request) {
	query := req.URL.Query()
	mode := query.Get("mode")
	switch mode {
	case "":
		handleNew(rw, req)
	case "view":
		handleView(rw, req, query)
	case "create":
		handleCreate(rw, req)
	default:
		http.Error(rw, fmt.Sprintf("unknown operation: %s", mode), http.StatusBadRequest)
	}
	return
}

func handleNew(rw http.ResponseWriter, req *http.Request) {
	if err := showCreateForm(rw); err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}

	// Display my own shortys, don't fail on errors.
	email := req.Header.Get("X-Forwarded-Email")
	myShortys, err := getShortysByOwner(email)
	if err != nil {
		log.Printf("Error getting shortys by owner(%s): %v", email, err)
		return
	}

	if err := viewMyShortys(rw, myShortys); err != nil {
		log.Printf("Error displaying shortys by owner(%s): %v", email, err)
		return
	}
}

func handleCreate(rw http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		http.Error(rw, "invalid method", http.StatusMethodNotAllowed)
		return
	}

	email := req.Header.Get("X-Forwarded-Email")
	if *requireEmail && len(email) == 0 {
		http.Error(rw, "missing X-Forwarded-Email header", http.StatusUnauthorized)
		return
	}

	shortURL := req.PostFormValue("short")
	if len(shortURL) != 0 {
		if err := validateShortURL(shortURL); err != nil {
			http.Error(rw, err.Error(), http.StatusBadRequest)
			return
		}
	}

	// Parse and rewrite the url.
	longURL := req.PostFormValue("url")
	if len(longURL) == 0 {
		http.Error(rw, "no URL specified", http.StatusBadRequest)
		return
	}

	parsedURL, err := url.Parse(longURL)
	if err != nil {
		http.Error(rw, fmt.Sprintf("invalid URL: %v", err), http.StatusBadRequest)
		return
	}
	if len(parsedURL.Host) == 0 || len(parsedURL.Scheme) == 0 {
		http.Error(rw, "URL needs to be of the form http(s)://hostname.com", http.StatusBadRequest)
		return
	}

	shorty := Shorty{
		ShortURL: shortURL,
		LongURL:  parsedURL.String(),
		Custom:   len(shortURL) != 0,
		Public:   req.PostFormValue("public") == "on",
		AddedBy:  email,
	}

	newShortURL, err := addNewShorty(shorty)
	if err != nil {
		// We're assuming "bad request" here, but it could also be an internal error.
		http.Error(rw, err.Error(), http.StatusBadRequest)
		return
	}

	// Redirect to the viewer for confirmation:
	http.Redirect(rw, req, fmt.Sprintf("/?mode=view&url=%s", newShortURL), http.StatusFound)
}

func handleView(rw http.ResponseWriter, req *http.Request, query url.Values) {
	if req.Method != http.MethodGet {
		http.Error(rw, "invalid method", http.StatusMethodNotAllowed)
		return
	}

	shortURL := query.Get("url")
	if len(shortURL) == 0 {
		http.Error(rw, "empty short URL requested", http.StatusBadRequest)
		return
	}

	if err := validateShortURL(shortURL); err != nil {
		http.Error(rw, "invalid short URL", http.StatusBadRequest)
		return
	}

	shorty, err := getShorty(shortURL)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}

	if shorty == nil {
		http.Error(rw, fmt.Sprintf("short URL %q not found", shortURL), http.StatusNotFound)
		return
	}

	if err := viewShorty(rw, shorty); err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}
}
