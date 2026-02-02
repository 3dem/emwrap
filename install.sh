#!/bin/bash

# Installed script to clone all em* repositories and install them in a conda environment

# If any command fails, exit with failure
set -e

# Define color variables for readability
RED='\033[91m'
GREEN='\033[92m'
BOLD='\033[1m'
NORMAL='\033[0m' # Resets the color to default

DIR="source"


# Echo the command and arguments with some color code
# and then execute it
run_cmd() {
  echo -e "- ${BOLD} $@ ${NORMAL}"
  "$@"
}

# Clone a development repo and pip install it
clone() {
  echo -e ">>> Installing ${GREEN} ${1} ${NORMAL}..."
  run_cmd git clone --branch ${2} https://github.com/3dem/${1}.git ${DIR}/${1}
  if [ "$#" -lt 3 ]; then
    run_cmd pip install -e ${DIR}/${1}
  fi
}

# Detect conda installation and active environment
detect_conda() {
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
  local SOURCE_FILE="${DIR}/bashrc"
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
EMSTACK_DIR="$(pwd)/${DIR}"

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

link_scripts() {
  run_cmd ln -s ${DIR}/emconfig/scripts/update.sh update.sh
  run_cmd ln -s ${DIR}/emconfig/scripts/run.sh run.sh
  run_cmd chmod +x update.sh run.sh
  echo -e "    To update the environment later, run: ${BOLD}./update.sh${NORMAL}"
  echo -e "    To run the server, run: ${BOLD}./run.sh${NORMAL}"
}

if [ -d "$DIR" ]; then
    echo -e "${RED}Installation folder ${DIR} exists, delete it and run the installer again.${NORMAL}"
    exit 1
fi


run_cmd mkdir ${DIR}
clone emtools devel
clone emhub devel
clone emwrap main
clone emconfig main pip_install=false

# Detect conda installation
detect_conda || true
generate_activate_script
link_scripts
emh-data --create_minimal instance 
run_cmd cp -r ${DIR}/emhub/extras/processing instance/extra 
echo -e "\n${GREEN}${BOLD}Installation complete!${NORMAL}"


