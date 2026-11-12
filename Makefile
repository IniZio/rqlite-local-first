.PHONY: up down demo test isolate heal logs

up:
	docker compose up -d --build
	@echo "Device A http://localhost:8081  B http://localhost:8082  C http://localhost:8083"

down:
	docker compose down -v

demo:
	scripts/demo.sh

test:
	cd app && go test ./...

isolate:
	scripts/net.sh isolate $(DEVICE)

heal:
	scripts/net.sh heal $(DEVICE)

logs:
	docker compose logs -f app-a app-b app-c
