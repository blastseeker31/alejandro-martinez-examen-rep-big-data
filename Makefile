.PHONY: check build up down logs test topics load-test scale-consumers verify reset-demo

check:
	python3 -m compileall producer_api consumer web_ui shared scripts tests
	python3 -m ruff check .

build:
	docker compose build

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f --tail=100

test:
	python3 -m pytest -q

topics:
	docker compose run --rm kafka_init

load-test:
	python3 scripts/run_load_tests.py

scale-consumers:
	docker compose up -d --scale consumer=3

verify:
	python3 scripts/verify_system.py

reset-demo:
	@printf 'Esto eliminara los datos de demostracion de AgroStream. Escriba RESET para continuar: '; read answer; test "$$answer" = "RESET"
	docker compose down -v
