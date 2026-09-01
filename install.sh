#!/bin/bash

# Installed script to clone all em* repositories and install them in a conda
# or Python venv environment

# If any command fails, exit with failure
set -e

# Define color variables for readability
RED='\033[91m'
GREEN='\033[92m'
BOLD='\033[1m'
NORMAL='\033[0m' # Resets the color to default

SOURCE="source"
CURRENT_STEP=""

# Error handler function
error_handler() {
  echo -e "\n${RED}${BOLD}======================================${NORMAL}"
  echo -e "${RED}${BOLD}ERROR:${NORMAL} Installation failed during: ${GREEN}${CURRENT_STEP}${NORMAL}"
  echo -e "${RED}Script terminated at line $1${NORMAL}"
  echo -e "${RED}${BOLD}======================================${NORMAL}"
  exit 1
}

# Set up the trap to catch errors
trap 'error_handler ${LINENO}' ERR

# Echo the command and arguments with some color code
# and then execute it
run_cmd() {
  echo -e "- ${BOLD} $@ ${NORMAL}"
  "$@"
}

# Clone a development repo and pip install it
clone() {
  CURRENT_STEP="cloning ${1}"
  echo -e ">>> Installing ${GREEN} ${1} ${NORMAL}..."
  run_cmd git clone --branch ${2} https://github.com/3dem/${1}.git ${SOURCE}/${1}
  if [ "$#" -lt 3 ]; then
    CURRENT_STEP="pip install ${1}"
    run_cmd pip install -e ${SOURCE}/${1}
  fi
}

# Detect the Python environment that is currently active, either a conda
# environment or a Python venv (virtualenv / "python -m venv"). Conda takes
# precedence when both happen to be detectable. Sets ENV_TYPE to "conda",
# "venv" or "" (nothing detected).
detect_python_env() {
  CURRENT_STEP="detecting Python environment"
  echo -e ">>> Detecting Python environment..."

  ENV_TYPE=""
  CONDA_BASE=""
  CONDA_ENV=""
  CONDA_ENV_PATH=""
  VENV_PATH=""

  # Prefer an active conda environment
  if command -v conda &> /dev/null && [ -n "$CONDA_DEFAULT_ENV" ]; then
    ENV_TYPE="conda"
    CONDA_BASE=$(conda info --base)
    CONDA_ENV="$CONDA_DEFAULT_ENV"
    CONDA_ENV_PATH="$CONDA_PREFIX"
    echo -e "    Conda found at: ${GREEN}${CONDA_BASE}${NORMAL}"
    echo -e "    Active environment: ${GREEN}${CONDA_ENV}${NORMAL}"
    echo -e "    Environment path: ${GREEN}${CONDA_ENV_PATH}${NORMAL}"
  # Otherwise, fall back to an active Python venv
  elif [ -n "$VIRTUAL_ENV" ]; then
    ENV_TYPE="venv"
    VENV_PATH="$VIRTUAL_ENV"
    echo -e "    Python venv detected: ${GREEN}${VENV_PATH}${NORMAL}"
  else
    if command -v conda &> /dev/null; then
      echo -e "    ${RED}Conda is installed, but no conda environment is currently active${NORMAL}"
    else
      echo -e "    ${RED}Conda not found in PATH${NORMAL}"
    fi
    echo -e "    ${RED}No active conda or venv environment detected${NORMAL}"
    return 1
  fi

  return 0
}

# Validate that the currently active environment (conda or venv, as detected
# by detect_python_env) provides a Python >= MIN_PY_MAJOR.MIN_PY_MINOR and a
# working pip. Must run after detect_python_env has succeeded.
check_python_version() {
  CURRENT_STEP="checking Python version and pip"
  echo -e ">>> Checking Python version and pip..."

  local MIN_PY_MAJOR=3
  local MIN_PY_MINOR=8

  local PYTHON_BIN=""
  if command -v python &> /dev/null; then
    PYTHON_BIN="python"
  elif command -v python3 &> /dev/null; then
    PYTHON_BIN="python3"
  else
    echo -e "    ${RED}No 'python' or 'python3' executable found in PATH${NORMAL}"
    return 1
  fi

  local PY_VERSION
  PY_VERSION=$("$PYTHON_BIN" -c 'import sys; print("%d.%d" % sys.version_info[:2])')
  local PY_MAJOR=${PY_VERSION%%.*}
  local PY_MINOR=${PY_VERSION##*.}

  if [ "$PY_MAJOR" -lt "$MIN_PY_MAJOR" ] || { [ "$PY_MAJOR" -eq "$MIN_PY_MAJOR" ] && [ "$PY_MINOR" -lt "$MIN_PY_MINOR" ]; }; then
    echo -e "    ${RED}Python ${MIN_PY_MAJOR}.${MIN_PY_MINOR}+ is required, found ${PY_VERSION} (${PYTHON_BIN})${NORMAL}"
    return 1
  fi
  echo -e "    Python version: ${GREEN}${PY_VERSION}${NORMAL} (${PYTHON_BIN})"

  if ! "$PYTHON_BIN" -m pip --version &> /dev/null; then
    echo -e "    ${RED}No working pip found for ${PYTHON_BIN} (tried: ${PYTHON_BIN} -m pip)${NORMAL}"
    return 1
  fi

  local PIP_VERSION
  PIP_VERSION=$("$PYTHON_BIN" -m pip --version)
  echo -e "    pip: ${GREEN}${PIP_VERSION}${NORMAL}"

  return 0
}

# Generate activation script for later use, matching whichever environment
# type was detected by detect_python_env (conda or venv).
generate_activate_script() {
  CURRENT_STEP="generating activation script"
  local SOURCE_FILE="bashrc"
  # Create empty placeholder file
  touch "$SOURCE_FILE"

  if [ -z "$ENV_TYPE" ]; then
    echo -e ">>> ${RED}Activation script not created: no conda or venv environment detected${NORMAL}"
    echo -e "    Empty file created at: ${SOURCE_FILE}"
    echo -e "    Activate a conda environment or a Python venv before running the installer,"
    echo -e "    then edit ${SOURCE_FILE} manually to source it."
    return 1
  fi

  echo -e ">>> Generating activation script: ${GREEN}${SOURCE_FILE}${NORMAL}"

  if [ "$ENV_TYPE" = "conda" ]; then
    cat > "$SOURCE_FILE" << EOF
#!/bin/bash
# Auto-generated source file for the '${CONDA_ENV}' conda environment
# Generated on: $(date)
# Usage: source ${SOURCE_FILE}

# Conda configuration
CONDA_BASE="${CONDA_BASE}"
ENV_NAME="${CONDA_ENV}"
EMSTACK_DIR="$(pwd)/${SOURCE}"

# Initialize conda
if [ -f "\$CONDA_BASE/etc/profile.d/conda.sh" ]; then
    . "\$CONDA_BASE/etc/profile.d/conda.sh"
else
    export PATH="\$CONDA_BASE/bin:\$PATH"
fi

# Activate the environment
conda activate "\$ENV_NAME"

# Set emstack environment variables
export EMSTACK_HOME="\$EMSTACK_DIR"

# Verify activation
if [ "\$CONDA_DEFAULT_ENV" = "\$ENV_NAME" ]; then
    echo "Activated conda \$CONDA_BASE (environment = \$ENV_NAME)"
    echo "EMSTACK_HOME=\$EMSTACK_HOME"
else
    echo "Warning: Failed to activate \$ENV_NAME"
    return 1
fi
EOF
  else
    cat > "$SOURCE_FILE" << EOF
#!/bin/bash
# Auto-generated source file for the Python venv at ${VENV_PATH}
# Generated on: $(date)
# Usage: source ${SOURCE_FILE}

# Venv configuration
VENV_PATH="${VENV_PATH}"
EMSTACK_DIR="$(pwd)/${SOURCE}"

# Activate the environment
if [ -f "\$VENV_PATH/bin/activate" ]; then
    . "\$VENV_PATH/bin/activate"
else
    echo "Warning: Could not find activate script at \$VENV_PATH/bin/activate"
    return 1
fi

# Set emstack environment variables
export EMSTACK_HOME="\$EMSTACK_DIR"

# Verify activation
if [ "\$VIRTUAL_ENV" = "\$VENV_PATH" ]; then
    echo "Activated venv \$VENV_PATH"
    echo "EMSTACK_HOME=\$EMSTACK_HOME"
else
    echo "Warning: Failed to activate venv at \$VENV_PATH"
    return 1
fi
EOF
  fi

  chmod +x "$SOURCE_FILE"
  echo -e "    To reload environment later, run: ${BOLD}source ${SOURCE_FILE}${NORMAL}"
}

# ============================================================================
# Pre-flight checks: an active conda or venv environment with a suitable
# Python and pip is required before doing anything else.
# ============================================================================
detect_python_env
check_python_version

if [ -d "$SOURCE" ]; then
    echo -e "${RED}Installation folder ${SOURCE} exists, delete it and run the installer again.${NORMAL}"
    exit 1
fi

CURRENT_STEP="creating source directory"
run_cmd mkdir ${SOURCE}
clone emtools devel
clone emhub devel
clone emwrap main

# Generate the activation script matching the environment detected above
generate_activate_script

# NOTE: forms and workflows are loaded directly from
# ${SOURCE}/emwrap/config/{forms,workflows} by ProcessingConfig (see
# get_forms_dir() / get_workflows_dir()). The local configuration file
# (emwrap.bashrc) and the 'scripts' folder are set up below by
# 'emh-tomo --update' (which also pulls the latest changes for the
# source checkouts on later runs). The emhub instance (and its processing
# extras) is created on demand the first time 'emh-tomo --run' is used.

CURRENT_STEP="setting up configuration files and scripts folder"
run_cmd emh-tomo --update

echo -e "\n${GREEN}${BOLD}Installation complete!${NORMAL}"

# TO install conda
# mkdir miniconda3 && wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O miniconda.sh && bash ./miniconda.sh -b -u -p ./miniconda3

# TO create a Python venv instead of conda
# python3 -m venv ./venv && source ./venv/bin/activate
