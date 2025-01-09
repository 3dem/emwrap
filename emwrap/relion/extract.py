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


#  ==================== autopick.star ========================
"""
# version 30001

data_coordinate_files

loop_
_rlnMicrographName #1
_rlnMicrographCoordinates #2
MotionCorr/job002/data/Images-Disc1/GridSquare_23333186/Data/FoilHole_23379462_Data_23378980_4_20240423_193203_fractions.mrc AutoPick/job007/data/Images-Disc1/GridSquare_23333186/Data/FoilHole_23379462_Data_23378980_4_20240423_193203_fractions_autopick.star
MotionCorr/job002/data/Images-Disc1/GridSquare_23333186/Data/FoilHole_23379462_Data_23378992_4_20240423_193157_fractions.mrc AutoPick/job007/data/Images-Disc1/GridSquare_23333186/Data/FoilHole_23379462_Data_23378992_4_20240423_193157_fractions_autopick.star
MotionCorr/job002/data/Images-Disc1/GridSquare_23333186/Data/FoilHole_23379462_Data_23379004_4_20240423_193200_fractions.mrc AutoPick/job007/data/Images-Disc1/GridSquare_23333186/Data/FoilHole_23379462_Data_23379004_4_20240423_193200_fractions_autopick.star
"""

# ================== micrographs_ctf.star ==============================
"""
# version 30001

data_optics

loop_ 
_rlnOpticsGroupName #1 
_rlnOpticsGroup #2 
_rlnMicrographOriginalPixelSize #3 
_rlnVoltage #4 
_rlnSphericalAberration #5 
_rlnAmplitudeContrast #6 
_rlnMicrographPixelSize #7 
opticsGroup1            1     0.648500   300.000000     2.700000     0.100000     0.648500 
 

# version 30001

data_micrographs

loop_ 
_rlnMicrographName #1 
_rlnOpticsGroup #2 
_rlnCtfImage #3 
_rlnDefocusU #4 
_rlnDefocusV #5 
_rlnCtfAstigmatism #6 
_rlnDefocusAngle #7 
_rlnCtfFigureOfMerit #8 
_rlnCtfMaxResolution #9 
MotionCorr/job002/data/Images-Disc1/GridSquare_23333186/Data/FoilHole_23379462_Data_23378980_4_20240423_193203_fractions.mrc            1 CtfFind/job005/data/Images-Disc1/GridSquare_23333186/Data/FoilHole_23379462_Data_23378980_4_20240423_193203_fractions_PS.ctf:mrc 19128.781250 18814.488281   314.292969     7.771090     0.151332     3.547526 
MotionCorr/job002/data/Images-Disc1/GridSquare_23333186/Data/FoilHole_23379462_Data_23378992_4_20240423_193157_fractions.mrc            1 CtfFind/job005/data/Images-Disc1/GridSquare_23333186/Data/FoilHole_23379462_Data_23378992_4_20240423_193157_fractions_PS.ctf:mrc 19331.462891 19187.615234   143.847656    29.402908     0.134823     3.428710 

"""

import os
import subprocess
import shutil
import sys
import json
import argparse
from pprint import pprint

from emtools.utils import Color, Timer, Path, Process
from emtools.jobs import Args
from emtools.metadata import Table, Column, StarFile, StarMonitor, TextFile


class RelionExtract:
    def __init__(self, *args, **kwargs):
        self.path = '/usr/local/em/scripts/relion_extract.sh'
        self.args = Args(kwargs.get('extra_args', {}))

    def process_batch(self, batch, **kwargs):
        t = Timer()

        # COMMAND:
        # `which relion_preprocess_mpi`
        # --i CtfFind/job005/micrographs_ctf.star
        # --coord_list AutoPick/job007/autopick.star
        # --part_star Extract/job008/particles.star --part_dir Extract/job008/
        # --extract --extract_size 512 --float16  --scale 128 --norm --bg_radius 27
        # --white_dust -1 --black_dust -1 --invert_contrast   --pipeline_control Extract/job008/

        args = Args({
            self.path: '',
            '--i': 'micrographs.star',
            '--coord_list': 'coordinates.star',
            '--part_star': 'particles.star',
            '--extract': '',
            '--float16': '',
            '--norm': '',
            '--white_dust': -1,
            '--black_dust': -1,
            '--invert_contrast': ''
        })
        args.update(self.args)
        batch.call(self.path, args, batch.join('extract_log.txt'))

        batch.info.update({
            'extract_elapsed': str(t.getElapsedTime())
        })



