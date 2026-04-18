# This script sets up or activates a Python virtual environment in the .venv directory,
# installs or updates the kiwi-scan package, and modifies the shell prompt to indicate the (development) environment.
# It must be sourced to affect the current shell

export VIRTUAL_ENV_DISABLE_PROMPT=1

# Function to clean the shell prompt
add_kiwi_to_prompt() {
    # Check if "KIWI" is already in PS1
    if [[ "$PS1" != *"KIWI "* ]]; then
        # Add "KIWI " to the beginning of PS1, preserving escape sequences
        PS1="KIWI $PS1"
    fi
}

# Check if .venv directory exists
if [ -d ".venv" ]; then
    # .venv exists
    # Check if a virtual environment is currently activated
    if [[ -n "${VIRTUAL_ENV:-}" ]]; then
        # VIRTUAL_ENV is non-empty
        echo "Virtual environment is currently activated: $VIRTUAL_ENV"
        echo "Updating kiwi-scan package"
        pip uninstall kiwi-scan -y
	pip install -e .[dev]
    else
        # VIRTUAL_ENV is empty
        echo "No virtual environment is activated."
        echo "Activating .venv"
        source .venv/bin/activate
    fi
else
    # .venv does not exist
    echo "Creating virtual environment and installing packages"
    python3 -m venv .venv
    source .venv/bin/activate
    pip install --upgrade pip
    pip install build wheel
    pip install -e .[dev]
fi

# Update the shell prompt to indicate the kiwi-scan development environment
add_kiwi_to_prompt

