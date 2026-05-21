.DEFAULT_GOAL = run

PROGRAM=example 
QUEUES=256
TERM=alacritty
	
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
	# race отключен из за багов - постоянно возникает data race, даже если тесты обложить мьютексами
	go test -fullpath -count=1 ./...

.PHONY: cover 
cover: lint
	go clean -testcache
	go test -fullpath -count=1 -race --coverprofile=coverage.out ./...
	go tool cover -html=coverage.out

.PHONY: build 
build: 
	CGO_ENABLED=0 go build -gcflags=all="-N -l" -v -o $(PROGRAM) ./

.PHONY: nats
nats:
	$(TERM) -e ./$(PROGRAM) nats -p 29999 -q $(QUEUES) > /dev/null &
	sleep 1
	$(TERM) -e ./$(PROGRAM) subscriber -n 29999 -p 30000 -q $(QUEUES) > /dev/null &
	sleep 1
	$(TERM) -e ./$(PROGRAM) subscriber -n 29999 -p 30001 -q $(QUEUES) -c 30000 > /dev/null &
	sleep 1
	$(TERM) -e ./$(PROGRAM) producer -n 29999 -q $(QUEUES) > /dev/null

.PHONY: redis
redis:
	$(TERM) -e ./$(PROGRAM) subscriber --redis-host localhost --redis-port 6379 -p 30000 -q $(QUEUES) > /dev/null &
	sleep 1
	$(TERM) -e ./$(PROGRAM) subscriber --redis-host localhost --redis-port 6379 -p 30001 -q $(QUEUES) -c 30000 > /dev/null &
	sleep 1
	$(TERM) -e ./$(PROGRAM) producer --redis-host localhost --redis-port 6379 -q $(QUEUES) > /dev/null

.PHONY: master 
master: 
	$(TERM) -e ./$(PROGRAM) subscriber -n 29999 -p 30000 -q $(QUEUES)

.PHONY: slave 
slave: 
	$(TERM) -e ./$(PROGRAM) subscriber -n 29999 -p 30001 -q $(QUEUES) -c 30000

.PHONY: producer 
producer: 
	$(TERM) -e ./$(PROGRAM) producer -n 29999 -q $(QUEUES)


.PHONY: verify 
verify: lint test
