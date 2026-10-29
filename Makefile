ROOT := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
export AUS_HAZARD_ROOT := $(ROOT)
export PATH := $(HOME)/.local/bin:$(PATH)
UV ?= uv

.PHONY: up down stream backfill replay transform test lint dashboard all quality freshness

up:
	docker compose up -d

down:
	docker compose down

stream:
	$(UV) run python -m aus_hazard.stream --once

backfill:
	$(UV) run python -m aus_hazard.fetch

replay:
	$(UV) run python -m aus_hazard.pipeline

transform:
	$(UV) run python -m aus_hazard.publish

dashboard:
	$(UV) run python -m aus_hazard.dashboard

quality:
	$(UV) run python -m aus_hazard.quality

freshness:
	$(UV) run dbt source freshness --project-dir dbt --profiles-dir dbt

test:
	$(UV) run pytest

lint:
	$(UV) run ruff check src tests --ignore E501
	$(UV) run ruff format --check src tests
	$(UV) run mypy

all: replay transform dashboard quality test

.DEFAULT_GOAL := all
