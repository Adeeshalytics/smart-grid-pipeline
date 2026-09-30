# Convenience targets. Every target is a plain docker compose command, so the
# project also works without make (see README).

.PHONY: up down reset ps logs test dashboards realtime report replay chaos-stop-source chaos-start-source

up:            ## Build and start the whole pipeline
	docker compose up -d --build

down:          ## Stop the pipeline, keep the data
	docker compose down

reset:         ## Stop the pipeline and delete all data (fresh simulated clock)
	docker compose down -v

ps:            ## Show service status
	docker compose ps

logs:          ## Follow the structured logs of the pipeline stages
	docker compose logs -f --tail=20 meter-simulator tariff-dropper speed-layer airflow api

test:          ## Run the unit and Spark tests inside the Spark image
	docker compose run --rm --no-deps --entrypoint "" speed-layer python -m pytest -q

dashboards:    ## Regenerate the Grafana dashboards from tools/build_dashboards.py
	python tools/build_dashboards.py

realtime:      ## Real-time grid load and renewable mix by zone
	curl -s http://localhost:8000/api/v1/grid/realtime

report:        ## Open the latest daily billing report
	curl -s http://localhost:8000/api/v1/reports/latest -o reports/latest.html

replay:        ## Reprocess one simulated day: make replay DAY=2026-01-02
	docker compose exec airflow airflow dags trigger grid_daily_billing --conf '{"sim_date": "$(DAY)"}'

chaos-stop-source:   ## Stop the meter stream to demonstrate the "no data" alert
	docker compose stop meter-simulator

chaos-start-source:  ## Start the meter stream again
	docker compose start meter-simulator
