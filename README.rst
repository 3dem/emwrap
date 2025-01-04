
.. |logo_image| image:: https://github.com/3dem/emhub/wiki/images/emtools-logo.png
   :height: 60px

|logo_image|

Python wrappers for CryoEM programs

Installation
============

.. code-block:: bash

    pip install emwrap

Or for development:

.. code-block:: bash

    git clone git@github.com:3dem/emwrap.git
    pip install -e emwrap/

Testing
=======

Motioncor3
----------

.. code-block:: bash

    # Test a batch run:
    python -m python -m unittest emwrap.motioncor.tests.TestMotioncor.test_batch

    # Test the entire pipeline:
    python -m python -m unittest emwrap.motioncor.tests.TestMotioncor.test_pipeline

