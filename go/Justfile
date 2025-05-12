# Copyright 2024 ScopeDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set dotenv-load

GO := "GO111MODULE=on go"

default:
    just --list

build:
	{{GO}} build ./...

test:
	{{GO}} test -v -bench=. ./...

golangci-lint:
	# Build from source is not recommend. See https://golangci-lint.run/welcome/install/
	GOBIN=$(pwd)/dev/bin {{GO}} install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.1.6

check-static: golangci-lint
	GO111MODULE=on CGO_ENABLED=0 dev/bin/golangci-lint run -v

fix-static: golangci-lint
	GO111MODULE=on CGO_ENABLED=0 dev/bin/golangci-lint run --fix -v

check-mod-tidy:
	{{GO}} mod tidy
	git diff --exit-code go.sum

fix-mod-tidy:
	{{GO}} mod tidy

check: check-mod-tidy check-static

fix: fix-mod-tidy fix-static
