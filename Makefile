# Copyright 2014 The Cockroach Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License. See the AUTHORS file
# for names of contributors.
#
# Author: Andrew Bonventre (andybons@gmail.com)
# Author: Shawn Morel (shawnmorel@gmail.com)
# Author: Spencer Kimball (spencer.kimball@gmail.com)
#

# Cockroach build rules.
GO ?= go
# Allow setting of go build flags from the command line.
GOFLAGS :=
# Set to 1 to use static linking for all builds (including tests).
STATIC :=

ifeq ($(STATIC),1)
# The netgo build tag instructs the net package to try to build a
# Go-only resolver.
TAGS += netgo
# The installsuffix makes sure we actually get the netgo build, see
# https://github.com/golang/go/issues/9369#issuecomment-69864440
GOFLAGS += -installsuffix netgo
LDFLAGS += -extldflags "-static"
endif

.PHONY: all
all: build check

.PHONY: build
build: block_writer

.PHONY: block_writer
block_writer:
	$(GO) build -tags '$(TAGS)' $(GOFLAGS) -ldflags '$(LDFLAGS)' -v -i -o block_writer/block_writer ./block_writer

.PHONY: check
check:
	@echo "checking for tabs in shell scripts"
	@! git grep -F '	' -- '*.sh'
	@echo "checking for \"path\" imports"
	@! git grep -F '"path"' -- '*.go'
	@echo "errcheck"
	@errcheck ./...
	@echo "vet"
	@! go tool vet . 2>&1 | \
	  grep -vE '^vet: cannot process directory .git'
	@echo "vet --shadow"
	@! go tool vet --shadow . 2>&1 | \
	  grep -vE '(declaration of err shadows|^vet: cannot process directory \.git)'
	@echo "golint"
	@! golint ./... | grep -vE '(\.pb\.go)'
	@echo "varcheck"
	@varcheck -e ./...
	@echo "gofmt (simplify)"
	@! gofmt -s -d -l . 2>&1 | grep -vE '^\.git/'
	@echo "goimports"
	@! goimports -l . | grep -vF 'No Exceptions'
