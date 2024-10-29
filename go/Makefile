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

GO := GO111MODULE=on go

.PHONY: build
build:
	$(GO) build ./...

.PHONY: test
test:
	$(GO) test -v ./...

tools/bin/golangci-lint:
	# Build from source is not recommend. See https://golangci-lint.run/usage/install/
	GOBIN=$(shell pwd)/tools/bin $(GO) install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.61.0

.PHONY: check-static
check-static: tools/bin/golangci-lint
	GO111MODULE=on CGO_ENABLED=0 tools/bin/golangci-lint run -v

.PHONY: check-mod-tidy
check-mod-tidy:
	$(GO) mod tidy
	git diff --exit-code go.sum

.PHONY: check
check: check-mod-tidy check-static
