.PHONY: help setup start stop restart clean test logs dashboard

help:
	@echo "Retail Analytics Big Data - Make Commands"
	@echo ""
	@echo "Available commands:"
	@echo "  make setup       - Setup environment and install dependencies"
	@echo "  make start       - Start all Docker services"
	@echo "  make stop        - Stop all services"
	@echo "  make restart     - Restart all services"
	@echo "  make clean       - Remove containers and volumes"
	@echo "  make test        - Run tests"
	@echo "  make logs        - View logs"
	@echo "  make dashboard   - Launch dashboard"
	@echo "  make batch       - Run batch jobs"
	@echo "  make stream      - Start streaming"

setup:
	@echo "Setting up environment..."
	./scripts/setup_environment.sh
	./scripts/download_data.sh

start:
	@echo "Starting services..."
	docker-compose up -d
	@echo "Waiting for services to be ready..."
	sleep 30
	python scripts/init_elasticsearch.py

stop:
	@echo "Stopping services..."
	docker-compose down

restart: stop start

clean:
	@echo "Cleaning up..."
	docker-compose down -v
	rm -rf data/processed/*
	rm -rf data/checkpoints/*
	rm -rf logs/*

test:
	@echo "Running tests..."
	python -m pytest tests/ -v

logs:
	docker-compose logs -f

dashboard:
	@echo "Launching dashboard..."
	streamlit run dashboard/app.py

batch:
	@echo "Running batch jobs..."
	./scripts/run_batch_jobs.sh

stream:
	@echo "Starting streaming..."
	python src/ingestion/kafka_producer.py \
		--csv data/raw/online_retail.csv \
		--speed 1
	docker exec -it spark-master spark-submit \
		--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.elasticsearch:elasticsearch-spark-30_2.12:7.17.7 \
		/opt/spark-apps/speed_layer/streaming_processor.py
