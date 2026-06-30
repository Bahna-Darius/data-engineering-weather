.PHONY: test lint run docker-up docker-down dbt-run

test:
	.venv/bin/pytest -v

lint:
	.venv/bin/flake8 src/ test/ --max-line-length=100

run:
	cd src && ../.venv/bin/python pipeline.py

docker-up:
	docker-compose up -d db_weather

docker-down:
	docker-compose down

dbt-run:
	cd weather_dbt_pipeline && dbt run && dbt test
