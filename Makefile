


environment:
	docker-compose up -d

down:
	docker-compose down

test:
	go test -timeout 900s -v -race -count=1 ./...
