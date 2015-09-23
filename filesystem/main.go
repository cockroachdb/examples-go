// Copyright 2015 The Cockroach Authors.
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
// Author: Marc Berhault (marc@cockroachlabs.com)

// This is a simple fuse filesystem that stores all metadata and data
// in cockroach.
//
// Inode relationships are stored in the namespace table, and inodes
// themselves in the inode table.
//
// Basic functionality is implemented, including:
// - mk/rm directory
// - create/rm files
// - read/write files
// - rename
//
// One caveat of the implemented features is that handles are not
// reference counted so if an inode is deleted, all open file descriptors
// pointing to it become invalid.
//
// Some TODOs (definitely not a comprehensive list):
// - support basic attributes (mode, timestamps)
// - support other types: symlinks, hard links
// - add ref counting (and handle open/release)
// - store data outside the inode (eg: separate data table)

package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	_ "bazil.org/fuse/fs/fstestutil"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/security/securitytest"
	"github.com/cockroachdb/cockroach/server"
	_ "github.com/cockroachdb/cockroach/sql/driver"
)

var usage = func() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s <mountpoint>\n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	flag.Usage = usage
	flag.Parse()

	if flag.NArg() != 1 {
		usage()
		os.Exit(2)
	}
	mountpoint := flag.Arg(0)

	security.SetReadFileFn(securitytest.Asset)
	serv := server.StartTestServer(nil)
	defer serv.Stop()
	url := "https://root@" + serv.ServingAddr() + "?certs=test_certs"

	// Open DB connection first.
	db, err := sql.Open("cockroach", url)
	if err != nil {
		log.Fatal(err)
	}
	// defer db.Close()

	cfs := CFS{db}
	if err := cfs.initSchema(); err != nil {
		log.Fatal(err)
	}

	{
		// For testing only.
		if err := cfs.create(rootNodeID, Node{Name: "myfile", IsDir: false}); err != nil {
			log.Fatal(err)
		}
		if err := cfs.create(rootNodeID, Node{Name: "mydir", IsDir: true}); err != nil {
			log.Fatal(err)
		}
		results, err := cfs.list(0)
		if err != nil {
			log.Fatal(err)
		}
		log.Print(results)
	}

	// Mount filesystem.
	c, err := fuse.Mount(
		mountpoint,
		fuse.FSName("CockroachFS"),
		fuse.Subtype("CockroachFS"),
		fuse.LocalVolume(),
		fuse.VolumeName(""),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt)
		<-sig
		fuse.Unmount(mountpoint)
	}()

	// Serve root.
	err = fs.Serve(c, cfs)
	if err != nil {
		log.Fatal(err)
	}

	// check if the mount process has an error to report
	<-c.Ready
	if err := c.MountError; err != nil {
		log.Fatal(err)
	}
}
