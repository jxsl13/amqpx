


environment:
	docker-compose up -d

test:
	go test -race -count=1 ./...