
.. |logo_image| image:: https://github.com/3dem/emhub/wiki/images/emwrap-logo.png
   :height: 60px

|logo_image|

Python wrappers for CryoEM/CryoET programs that can be executed from the command line. The wrappers have been 
designed to be compatible with the definition of external jobs in Relion. They have only two arguments: input JSON values 
with key=value pairs and the output folder. 

Installation
============

.. code-block:: bash

    pip install emwrap

Or for development:

.. code-block:: bash

    git clone git@github.com:3dem/emwrap.git
    pip install -e emwrap/

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
