# Makefile for Docker operations
# Simplifies common Docker commands

.PHONY: help build up down logs clean test shell prod dev

# Colors for output
GREEN := \033[0;32m
YELLOW := \033[1;33m
RED := \033[0;31m
NC := \033[0m # No Color

help: ## Show this help message
	@echo '$(GREEN)PowerApps ETL Pipeline Docker Commands:$(NC)'
	@echo ''
	@echo '$(YELLOW)Usage:$(NC)'
	@echo '  make $(GREEN)<target>$(NC)'
	@echo ''
	@echo '$(YELLOW)Targets:$(NC)'
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-20s$(NC) %s\n", $$1, $$2}'

build: ## Build the Docker image
	@echo '$(GREEN)Building Docker image...$(NC)'
	docker-compose -f docker-compose.yml build

up: ## Start all services
	@echo '$(GREEN)Starting services...$(NC)'
	docker-compose -f docker-compose.yml up -d
	@echo '$(GREEN)Services started. Use "make logs" to view output.$(NC)'

down: ## Stop all services
	@echo '$(YELLOW)Stopping services...$(NC)'
	docker-compose -f docker-compose.yml down

dev: ## Start development environment (with Jupyter and pgAdmin)
	@echo '$(GREEN)Starting development environment...$(NC)'
	docker-compose -f docker-compose.yml --profile dev up -d
	@echo '$(GREEN)Development environment started.$(NC)'
	@echo '  Jupyter: http://localhost:8888 (token: $$$$JUPYTER_TOKEN)'
	@echo '  pgAdmin: http://localhost:5050'

prod: ## Start production environment (pipeline only)
	@echo '$(GREEN)Starting production environment...$(NC)'
	docker-compose -f docker-compose.yml up -d pipeline
	@echo '$(GREEN)Pipeline started in production mode.$(NC)'

logs: ## View logs
	docker-compose -f docker-compose.yml logs -f

pipeline-logs: ## View pipeline logs only
	docker logs -f powerapps-etl-pipeline

shell: ## Open a shell in the pipeline container
	docker exec -it powerapps-etl-pipeline /bin/bash

test: ## Run tests in container
	@echo '$(GREEN)Running tests...$(NC)'
	docker-compose -f docker-compose.yml run --rm pipeline python -m pytest tests/ -v

clean: ## Remove all containers, volumes, and images
	@echo '$(RED)Cleaning up Docker resources...$(NC)'
	docker-compose -f docker-compose.yml down -v
	docker system prune -f

ps: ## Show running containers
	docker-compose -f docker-compose.yml ps

stats: ## Show container stats
	docker stats

backup: ## Backup database
	@echo '$(GREEN)Backing up database...$(NC)'
	docker exec powerapps-postgres pg_dump -U pipeline powerapps_warehouse > backup_$$(date +%Y%m%d_%H%M%S).sql

restore: ## Restore database (usage: make restore file=backup.sql)
	@if [ -z "$(file)" ]; then \
		echo '$(RED)Please specify backup file: make restore file=backup.sql$(NC)'; \
		exit 1; \
	fi
	cat $(file) | docker exec -i powerapps-postgres psql -U pipeline powerapps_warehouse

init: ## Initialize environment (copy .env.example)
	@if [ ! -f .env ]; then \
		cp .env.example .env; \
		echo '$(GREEN)Created .env file. Please edit it with your settings.$(NC)'; \
	else \
		echo '$(YELLOW).env file already exists.$(NC)'; \
	fi

# Database commands
db-shell: ## Open PostgreSQL shell
	docker exec -it powerapps-postgres psql -U pipeline powerapps_warehouse

db-migrate: ## Run database migrations
	@echo '$(GREEN)Running migrations...$(NC)'
	cat init-db.sql | docker exec -i powerapps-postgres psql -U pipeline powerapps_warehouse

# Pipeline commands
run-pipeline: ## Run the pipeline once
	docker exec powerapps-etl-pipeline python pipeline_orchestrator.py --days $(or $(days),1)

generate-data: ## Generate sample data
	docker exec powerapps-etl-pipeline python export_simulator.py --days $(or $(days),7)

# Monitoring
monitor: ## Show resource usage
	@echo '$(GREEN)Container Resource Usage:$(NC)'
	docker stats --no-stream

# Security
scan: ## Scan images for vulnerabilities
	docker scan powerapps-etl-pipeline
