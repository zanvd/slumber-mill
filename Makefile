.PHONY: up down tenant test test-unit vet fmt tidy cover

up:
	docker compose up -d --build

down:
	docker compose --profile dev down --remove-orphans

tenant:
	docker compose run --rm api tenant create -slug $(SLUG)

test:
	docker compose run --rm dev

test-unit:
	docker compose run --rm --no-deps dev go test -race ./...

vet:
	docker compose run --rm dev go vet -tags integration ./...

fmt:
	docker compose run --rm --no-deps dev gofmt -l .

tidy:
	docker compose run --rm --no-deps dev go mod tidy

cover:
	docker compose run --rm dev go test -tags integration -coverprofile=/out/cover.out ./...
