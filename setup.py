# **************************************************************************
# *
# * Authors:     J.M. de la Rosa Trevin (delarosatrevin@gmail.com)
# *
# * This program is free software; you can redistribute it and/or modify
# * it under the terms of the GNU General Public License as published by
# * the Free Software Foundation; either version 3 of the License, or
# * (at your option) any later version.
# *
# * This program is distributed in the hope that it will be useful,
# * but WITHOUT ANY WARRANTY; without even the implied warranty of
# * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# * GNU General Public License for more details.
# *
# **************************************************************************

"""A setuptools based setup module.
See:
https://packaging.python.org/en/latest/distributing.html
https://github.com/pypa/sampleproject
"""

import os
# Always prefer setuptools over distutils
from setuptools import setup, find_packages
# To use a consistent encoding
from codecs import open
from os import path
import emwrap

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

# Read requirements.txt
with open(os.path.join(here, 'requirements.txt')) as f:
    requirements = f.read().splitlines()

# Arguments marked as "Required" below must be included for upload to PyPI.
# Fields marked as "Optional" may be commented out.

setup(
    name='emwrap',  # Required
    version=emwrap.__version__,  # Required
    description='Utilities for CryoEM data manipulation',  # Required
    long_description=long_description,  # Optional
    url='https://github.com/3dem/emwrap',  # Optional
    author='J.M. De la Rosa Trevin, Grigory Sharov',  # Optional
    author_email='delarosatrevin@gmail.com, gsharov@mrc-lmb.cam.ac.uk',  # Optional
    classifiers=[  # Optional
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Programming Language :: Python :: 3'
    ],
    keywords='electron-microscopy cryo-em structural-biology image-processing',  # Optional
    packages=find_packages(),
    project_urls={  # Optional
        'Bug Reports': 'https://github.com/3dem/emwrap/issues',
        'Source': 'https://github.com/3dem/emwrap',
    },
    include_package_data=True,
    install_requires=requirements,
    entry_points={  # Optional
       'console_scripts': [
           'emw-motioncor = emwrap.motioncor.__main__:main',
           'emw-aretomo = emwrap.aretomo.aretomo_pipeline:main',
           'emw-otf = emwrap.mix.otf:main',
           'emw-preprocessing = emwrap.mix.preprocessing_pipeline:main',
           'emw-rln2d = emwrap.relion.classify2d_pipeline:main',
           'emw-import-movies = emwrap.base.import_movies:main',
           'emw-mc-tomo = emwrap.motioncor.mcpipeline_tomo:main',
           'emw = emwrap.base:ProjectManager.main'
       ],

    }
)
