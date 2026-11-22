# Scientific Event Data Lakehouse — developer commands.
#
# Most targets run inside the project virtualenv and set JAVA_HOME for Spark.
# Override RUN_ID to operate on a different ingestion run.

PYTHON ?= .venv/bin/python
PIP ?= .venv/bin/pip
export JAVA_HOME ?= /opt/homebrew/opt/openjdk@17
export SPARK_LOCAL_IP ?= 127.0.0.1
export PYTHONPATH := $(CURDIR)

RUN_ID ?= 3d8d9a251f33489e807356890c692789

.PHONY: help setup test lint format validate bronze profile benchmark clean-warehouse

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

setup: ## Install core + Spark dependencies into the virtualenv
	$(PIP) install -q -r requirements.txt -r requirements-spark.txt

test: ## Run the full test suite (unit + Spark integration)
	$(PYTHON) -m pytest -q

lint: ## Run ruff linting
	$(PYTHON) -m ruff check .

format: ## Apply ruff formatting
	$(PYTHON) -m ruff format .

validate: lint ## Lint + verify formatting without modifying files
	$(PYTHON) -m ruff format --check .

bronze: ## Build structured Bronze Parquet for RUN_ID
	$(PYTHON) -m src.bronze.build_bronze --run-id $(RUN_ID)

silver: ## Build Silver canonical model + data quality for RUN_ID
	$(PYTHON) -m src.silver.build_silver --run-id $(RUN_ID)

delta: ## Merge Silver outputs into transactional Delta tables for RUN_ID
	$(PYTHON) -m src.delta_lake.build_delta --run-id $(RUN_ID)

gold: ## Build Gold dimensional model + analytical datasets
	$(PYTHON) -m src.gold.build_gold

pipeline: bronze silver delta gold ## Run Bronze -> Silver -> Delta -> Gold for RUN_ID

profile: ## Profile a completed ingestion run (RUN_ID)
	$(PYTHON) -m src.profiling.profile_bronze --run-id $(RUN_ID)

benchmark: ## Measure Bronze build performance (3 repetitions)
	$(PYTHON) scripts/benchmark_bronze.py --run-id $(RUN_ID)

terraform-validate: ## Validate Terraform/OpenTofu infrastructure (no apply)
	cd infrastructure/terraform && tofu fmt -check -recursive .
	cd infrastructure/terraform/environments/dev && tofu init -backend=false && tofu validate

clean-warehouse: ## Remove structured warehouse outputs (keeps raw archive)
	rm -rf data/warehouse
