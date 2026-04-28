#!/bin/bash

# Installed script to clone all em* repositories and install them in a conda environment

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

# Detect conda installation and active environment
detect_conda() {
  CURRENT_STEP="detecting conda installation"
  echo -e ">>> Detecting conda installation..."
  
  CONDA_BASE=""
  CONDA_ENV=""
  CONDA_ENV_PATH=""

  # Check if conda command exists
  if command -v conda &> /dev/null; then
    CONDA_BASE=$(conda info --base)
    echo -e "    Conda found at: ${GREEN}${CONDA_BASE}${NORMAL}"
    
    # Detect active environment using environment variables
    if [ -n "$CONDA_DEFAULT_ENV" ]; then
      CONDA_ENV="$CONDA_DEFAULT_ENV"
      CONDA_ENV_PATH="$CONDA_PREFIX"
      echo -e "    Active environment: ${GREEN}${CONDA_ENV}${NORMAL}"
      echo -e "    Environment path: ${GREEN}${CONDA_ENV_PATH}${NORMAL}"
    else
      echo -e "    ${RED}No conda environment is currently active${NORMAL}"
    fi
    return 0
  else
    echo -e "    ${RED}Conda not found in PATH${NORMAL}"
    return 1
  fi  
}

# Generate activation script for later use
generate_activate_script() {
  CURRENT_STEP="generating activation script"
  local SOURCE_FILE="bashrc"
  # Create empty placeholder file
  touch "$SOURCE_FILE"

  # Check if conda was detected
  if [ -z "$CONDA_BASE" ]; then
    echo -e ">>> ${RED}Activation script not created: conda installation not detected${NORMAL}"
    echo -e "    Empty file created at: ${SOURCE_FILE}"
    return 1
  fi
  
  echo -e ">>> Generating activation script: ${GREEN}${SOURCE_FILE}${NORMAL}"
  
  cat > "$SOURCE_FILE" << EOF
#!/bin/bash
# Auto-generated source file for ${CONDA_ENV} environment
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

  chmod +x "$SOURCE_FILE"
  echo -e "    To reload environment later, run: ${BOLD}source ${SOURCE_FILE}${NORMAL}"
}

copy_templates() {
  local source_dir=$1
  local target_dir=$2
  echo -e ">>> Copying templates from ${source_dir} to ${target_dir}..."
  for script in ${source_dir}/*.template; do
    run_cmd cp ${script} ${target_dir}/$(basename ${script} .template)
  done
}

link_scripts() {
  CURRENT_STEP="linking scripts"
  run_cmd cp ${SOURCE}/emwrap/config/scripts/update.sh.template update.sh
  run_cmd cp ${SOURCE}/emwrap/config/scripts/run.sh.template run.sh
  
  echo -e "    To update the environment later, run: ${BOLD}./update.sh${NORMAL}"
  echo -e "    To run the server, run: ${BOLD}./run.sh${NORMAL}"
}

if [ -d "$SOURCE" ]; then
    echo -e "${RED}Installation folder ${SOURCE} exists, delete it and run the installer again.${NORMAL}"
    exit 1
fi

CURRENT_STEP="creating source directory"
run_cmd mkdir ${SOURCE}
clone emtools devel
clone emhub devel
clone emwrap main

CURRENT_STEP="copying templates scripts"
copy_templates ${SOURCE}/emwrap/config/ ./
run_cmd chmod +x update.sh run.sh
run_cmd mkdir scripts
copy_templates ${SOURCE}/emwrap/config/scripts scripts
run_cmd mkdir workflows
copy_templates ${SOURCE}/emwrap/config/workflows workflows

# Detect conda installation
detect_conda || true
generate_activate_script

CURRENT_STEP="creating minimal instance"
emh-data --create_minimal instance

CURRENT_STEP="copying processing extras"
run_cmd cp -r ${SOURCE}/emhub/extras/processing instance/extra 
echo -e "\n${GREEN}${BOLD}Installation complete!${NORMAL}"

# TO install conda
# mkdir miniconda3 && wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O miniconda.sh && bash ./miniconda.sh -b -u -p ./miniconda3

