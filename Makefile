

.PHONY: build run stop down test count-tests count-disconnect-tests reset

reset: down environment

environment:
	docker compose up -d

stop: down

down:
	docker compose down

test:
	go test -timeout 450s -race -count=1 ./... > parallel.test.log

test-sequentially:
	go test -timeout 900s -race -parallel 1 -count=1 ./... > sequential.test.log

count-tests:
	grep -REn 'func Test.+\(.+testing\.T.*\)' . | wc -l

count-disconnect-tests:
	grep -REn 'func Test.+WithDisconnect.*\(.+testing\.T.*\)' . | wc -l


pool.TestBatchSubscriberMaxBytes:
	go test -timeout 0m30s github.com/jxsl13/amqpx/pool -run ^TestBatchSubscriberMaxBytes$  -v -count=1 -race 2>&1 > debug
	.test.log
	cat test.log | grep 'INFO: session' | sort | uniq -c