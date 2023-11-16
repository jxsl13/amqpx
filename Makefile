


environment:
	docker-compose up -d

down:
	docker-compose down

test:
	go test -v -race -count=1 ./...
