.DEFAULT_GOAL = run

.PHONY: lint 
lint:
	golangci-lint run --config=.golangci.yml ./...

.PHONY: lint-fix
lint-fix:
	golangci-lint run --enable-only govet --fix ./...

.PHONY: test 
test: lint
	#go test -v -fullpath -count=1 -short -race ./...
	go test -v -fullpath -count=1 -short ./...

.PHONY: test-full 
test-full: lint
	go clean -testcache
	# go test -v -fullpath -count=1 -race ./...
	go test -fullpath -count=1 ./...

.PHONY: cover 
cover: lint
	go clean -testcache
	go test -fullpath -count=1 -race --coverprofile=coverage.out ./...
	go tool cover -html=coverage.out

.PHONY: verify 
verify: lint test
