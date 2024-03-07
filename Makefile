


environment:
	docker-compose up -d

down:
	docker-compose down

test:
	go test -timeout 900s -v -race -count=1 ./...

count-tests:
	grep -REn 'func Test.+\(.+testing\.T.*\)' . | wc -l

count-disconnect-tests:
	grep -REn 'func Test.+WithDisconnect.*\(.+testing\.T.*\)' . | wc -l