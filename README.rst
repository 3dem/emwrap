
.. |logo_image| image:: https://github.com/3dem/emhub/wiki/images/emwrap-logo.png
   :height: 60px

|logo_image|

Python wrappers for CryoEM/CryoET programs that can be executed from the command line. The wrappers have been 
designed to be compatible with the definition of external jobs in Relion. They have only two arguments: input JSON values 
with key=value pairs and the output folder. The processing workflow can be launched and monitored through the EMhub web interface. 

Installation
============

*emwrap* is currently under development, and installation instructions might change. Current instructions are intended for a development environment. 

Installation should work with any Python 3.8+ environment, but we have tested it with conda environments. If you need to install conda, you can follow the instructions:

.. code-block:: bash

   # Download and run the install script
   mkdir miniconda3 && wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O miniconda.sh && bash ./miniconda.sh -b -u -p ./miniconda3


Once you have conda activated, you can install emwrap with the following commands:

.. code-block:: bash

   # Create a folder for the installation
   mkdir emstack && cd emstack

   # Create a conda environment and activate it
   conda create -y --name=emstack python=3.8 && conda activate emstack

   # Download and run the install script
   wget -qO- https://raw.githubusercontent.com/3dem/emwrap/refs/heads/main/install.sh | bash

   # Run the server
   ./run.sh


Configuration
=============

The installation script will create a `emwrap.bashrc` file in the installation folder. This is the main configuration file
that has references to other files and settings. From there, the `bashrc` file is sourced to load the required Python/Conda
environment. The environment variable `EMWRAP_CONFIG` is defined in the `emwrap.bashrc` file, as a JSON literal. You should 
modify its content to adapt to your computing needs regarding programs, queues, and other settings. 

Python Environment
------------------

While running the installation, the install script will try to determine the Conda path and the activated environment (usually `emstack`).
Based on that, it will create the `bashrc` file that will be sourced from `emwrap.bashrc`. If you are not using Conda or the `bashrc` file is not correct, you should modify it to properly load the Python environment for launching **emhub/emwrap**.

.. code-block:: bash

   # Edit the bashrc file
   vim bashrc

Program Launchers
-----------------

In **emwrap**, external programs can be defined by specifying "program launchers". The idea of the launcher is to create a bash script that wraps the program call and sets up the necessary environment. For example, the launcher can load cluster modules, source bash files, or set up environment variables. In that way, the code from **emwrap** just needs to call the launcher without taking care of local installation details. 

There is a section in the *EMWRAP_CONFIG* variable related to the launchers: 

.. code-block:: json

   "programs": {
        "WARP": {"launcher": "$SCRIPTS/warp_launcher.sh"},
        "PYTOM": {"launcher": "$SCRIPTS/pytom_launcher.sh"},
        "RELION": {"launcher": "$SCRIPTS/relion_launcher.sh"},
        "IMOD": {"launcher": "$SCRIPTS/imod_launcher.sh"},
        "MOTIONCOR2": {"launcher": "$SCRIPTS/motioncor2.sh"},
        "MOTIONCOR3": {"launcher": "$SCRIPTS/motioncor3.sh"},
        "ARETOMO2": {"launcher": "$SCRIPTS/aretomo2.sh"},
        "ARETOMO3": {"launcher": "$SCRIPTS/aretomo3.sh"},
        "CTFFIND": {"launcher": "$SCRIPTS/ctffind5.sh", "version": 5},
        "CRYOCARE": {"launcher": "$SCRIPTS/cryocare_launcher.sh"}
    }

After the installation, there is a *scripts* folder that is created with some of the launcher scripts, but YOU MIGHT NEED TO MODIFY them to work in your environment. In the following subsections, there are some examples of launchers.

Warp Launcher
.............

In the following example, Warp is loaded from the available modules, together with Aretomo2, version 1.0.0.

.. code-block:: bash

   #!/bin/bash

   PROGRAM=$1
   shift

   export MODULES="warp/2.0dev33-latest aretomo2/1.0.0"
   echo Loading modules $MODULES
   module load -s $MODULES

   $PROGRAM $@

Or, if we are loading Warp from an SBGrid installation, the launcher could be something like:

.. code-block:: bash

   #!/bin/bash

   PROGRAM=$1
   shift

   export SBGRID=/programs/sbgrid.shrc
   source $SBGRID
   echo "Loading Warp from SBGrid file: ${SBGRID}."

   $PROGRAM $@

Relion Launcher
...............

In the case of the Relion launcher, the first argument is the program name, and the second is the number of MPI processes. The wrapper will take care of adding the *_mpi* suffix to the program and also the *mpirun* command. For example:

.. code-block:: bash

    #!/bin/bash

    export SBGRID=/programs/sbgrid.shrc
    source $SBGRID
    echo "Loading Relion from SBGrid file: ${SBGRID}."

    export PROGRAM=$1
    shift
    export MPI=$1
    shift

    if [ "$MPI" -eq 1 ]; then
        export CMD="${PROGRAM} $@"
    else
        export CMD="mpirun.relion --oversubscribe -np ${MPI} ${PROGRAM}_mpi $@"
    fi

    echo Running command: ${CMD}
    $CMD

Other Launchers
...............

**emwrap** is still under development, and more tools will be integrated in the future. Right now, apart from Warp and Relion, it might be helpful to configure the following launchers:

* PyTOM launcher: for 3D template matching
* IMOD launcher: for etomo tilt-series alignment


Cluster Queues
--------------

After the program launchers, the next section is the definition of cluster queues. You can define as many queues as you need, and each queue can have a different template, submit command, and parameters. 
The following is an example defining three queues: two of them use LSF and the third one uses SLURM. In one of the queues, it is possible to select the GPU type for the job.

.. code-block:: json

    "queues": [
        {
            "name": "cryoem",
            "template": "$SCRIPTS/lsf_template.sh",
            "submit": "$SCRIPTS/lsf_submit.sh {job_script}",
            "params": [
                {
                    "name": "queue_name",
                    "default": "cryoem",
                    "condition": "false"
                },
                {
                    "name": "gpu_type",
                    "label": "GPU type",
                    "help": "Select the GPU type if you need an specific one for this job.",
                    "default": "any",
                    "paramClass": "EnumParam",
                    "choices": ["any", "V100", "A100"]
                }
            ]
        },
        {
            "name": "cryo_core",
            "template": "$SCRIPTS/lsf_template.sh",
            "submit": "$SCRIPTS/lsf_submit.sh {job_script}",
            "params": [
                {
                    "name": "queue_name",
                    "default": "cryo_core",
                    "condition": "false"
                }
            ]
        },
        {
            "name": "rtx5000",
            "template": "$SCRIPTS/slurm_rtx5000_template.sh",
            "submit": "sbatch {job_script}",
            "params": [
                {
                    "name": "queue_name",
                    "default": "rtx5000",
                    "condition": "false"
                }
            ]
        }
    ]

Job Script Template
...................

For each queue, a submission template is required to create the job script for each job. The template is a bash script that will be executed by the cluster scheduler. All the parameters defined in the queue will be passed in 
a dictionary to the template. Additional parameters that will be accessible to the template (and submit command) are:

* **jobId**: the project job id (and folder) not the scheduler job id
* **command**: the command to execute
* **gpu_line**: this is specific for LSF clusters, where CPU only jobs avoid the line for GPU requests. 
* **job_id**: the job id
* **gpus**: number of GPUs requested by the job.
* **cpus**: number of CPUs requested by the job.
* **working_dir**: the working directory for the job (the project folder).
* **job_out**: the path to the job output file.
* **job_err**: the path to the job error file.

The following is an example of a SLURM template:

.. code-block:: bash

    #!/bin/bash

    #SBATCH --partition={queue_name}
    #SBATCH --nodes=1
    #SBATCH --tasks=1
    #SBATCH --cpus-per-task={cpus}
    #SBATCH --gres=gpu:{gpus}
    #SBATCH --mem=200G
    #SBATCH --output={job_out}
    #SBATCH --error={job_err}

    cd {working_dir}
    hostname -f
    {command}

And the following is an example of a LSF template:

.. code-block:: bash

    #!/bin/bash

    #BSUB -P emwrap-tomo
    {gpu_line}
    #BSUB -R "rusage[mem=5000]"
    #BSUB -q {queue_name}
    #BSUB -n {cpus}
    #BSUB -R "span[ptile={cpus}]"
    #BSUB -e {working_dir}/{jobId}/run.err -o {working_dir}/{jobId}/run.out

    cd {working_dir}
    hostname -f
    {command}


Workflows
---------

**WORK IN PROGRESS**

Workflows are defined in the `workflows` folder. Each workflow is a JSON file that defines the jobs to be executed in sequence. The jobs are defined by their type and the parameters to be passed to them. 
The idea is that from EMhub-Tomo, processing pipelines can be exported as workflows and reused in other projects. 


Tomography
==========

.. _Warp: https://warpem.github.io/
.. _PyTOM: https://github.com/SBC-Utrecht/pytom-match-pick
.. _Relion: https://relion.readthedocs.io/en/latest/STA_tutorial/Introduction.html

.. list-table:: Jobs
   :header-rows: 1
   :widths: 30 10 10 10

   * - Job
     - Description
     - Commands
     - Packages
   * - emw-import-ts
     - Import raw frames and MDOC files
     - 
     - emwrap
   * - emw-warp-mctf
     - Warp's motion correction and CTF
     - create_settings, fs_motion_and_ctf
     - `Warp`_
   * - emw-warp-aretomo
     - Tilt series alignment with Aretomo through Warp's wrapper.
     - ts_import, create_settings, ts_aretomo
     - `Warp`_, AreTomo2
   * - emw-warp-ctfrec
     - Warp 3D CTF and reconstruction
     - ts_ctf, ts_reconstruct
     - `Warp`_
   * - emw-warp-pytom
     - Particle picking by template matching 
     - pytom_match_pick, pytom_extract
     - `PyTOM`_
   * - emw-relion-tomorecons
     - Reconstruct an initial volume from input sub-tomograms
     - WORK-IN-PROGRESS
     - `Relion`_
   * - emw-relion-tomorefine
     - 3D Refine sub-tomogram particles
     - WORK-IN-PROGRESS
     - `Relion`_


    
