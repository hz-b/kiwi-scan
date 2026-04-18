SHELL := /bin/bash
VERSION ?= 0.1.0
.DEFAULT_GOAL := help

BASH_COMPLETION_DIR ?= ~/.bash_completion.d
WITH_VENV = if [ -z "$$VIRTUAL_ENV" ]; then source "$(CURDIR)/mkvenv.sh"; fi

SCRIPTS = \
    scan_runner \
    actuator_runner \
    scanplotter_cli_completion

SCRIPT_PATHS = $(foreach script,$(SCRIPTS),$(BASH_COMPLETION_DIR)/$(script))
MOCK_IOC_SCRIPT = tests/hasmi_mock_ioc.py

.PHONY: help all install_completion uninstall_completion clean cscope tag lint test

help: ## Show available development targets
	@echo "kiwi-scan development targets"
	@echo
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z0-9_.-]+:.*## / { printf "  %-22s %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

all: install_completion ## Install bash completion scripts

install_completion: ## Install bash completion files and add source lines to ~/.bashrc
	@mkdir -p $(BASH_COMPLETION_DIR)
	@for script in $(SCRIPTS); do \
		cp bash-completion/$$script $(BASH_COMPLETION_DIR)/$$script; \
		grep -q "$$script" ~/.bashrc || echo "source $(BASH_COMPLETION_DIR)/$$script" >> ~/.bashrc; \
	done
	@echo "Bash completion for $(SCRIPTS) installed. Reload your shell to activate."

uninstall_completion: ## Remove installed bash completion files
	@for script in $(SCRIPTS); do \
		rm -f $(BASH_COMPLETION_DIR)/$$script; \
	done
	@echo "Bash completion for $(SCRIPTS) removed. Edit your .bashrc and remove the source lines."

clean: ## Remove local virtualenv, caches, tags, and egg-info
	@rm -rf .venv
	@rm -f tags cscope.files cscope.out
	@find src -maxdepth 1 -type d -name "*.egg-info" -exec rm -rf {} +
	@find . -type d -name "__pycache__" -exec rm -r {} +

cscope: ## Build cscope and ctags indexes for the repository
	find . -path ./.venv -prune -o -name "*.py" -print > cscope.files
	cscope -b -i cscope.files
	ctags -R --languages=Python .

tag: ## Create a version+timestamp git tag from HEAD
	@ts=$$(git show -s --format=%cd --date=format:%Y%m%d.%H%M%S HEAD); \
	tag="$(VERSION)+$$ts"; \
	echo "Creating tag $$tag"; \
	git tag -a $$tag -m "Release $$tag"

lint: ## Run pylint on src/kiwi_scan (uses mkvenv.sh when needed)
	@echo 'For pylint install extra packages: pip install -e ".[dev]"'
	@$(WITH_VENV); \
	PYTHONPATH=src pylint --disable=C,R,W src/kiwi_scan || true

test: ## Run the current test scripts (uses mkvenv.sh when needed)
	@$(WITH_VENV); \
	python3 tests/test_actuator.py; \
	EPICS_WRITETEST=1 python3 tests/test_epics_wrapper_integration.py; \
	python3 tests/test_stats.py; \
	python3 tests/test_registry_trigger_and_callbacks.py; \
	python3 tests/test_subscription_manager.py; \
	python3 tests/test_scanlib.py
