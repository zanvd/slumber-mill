.PHONY: up down tenant test test-unit vet fmt tidy cover process-checks checkly-agent checkly-test checkly-deploy

up:
	docker compose up -d --build

down:
	docker compose --profile dev --profile checkly --profile checkly-agent down --remove-orphans

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

process-checks:
	bash scripts/process-checks.sh

checkly-agent:
	docker compose --profile checkly-agent up -d checkly-agent

checkly-test:
	docker compose --profile checkly run --rm checkly npx checkly test --private-location slumber-mill-local --record

checkly-deploy:
	docker compose --profile checkly run --rm checkly npx checkly deploy
