


environment:
	docker-compose up -d

test:
	go test -v -race -count=1 ./...