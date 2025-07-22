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

import os
import argparse
import glob
import numpy as np
import time
import json

from emtools.utils import Color, Process, System, Path, GpuMonitor
from emtools.jobs import BatchManager, Args
from emtools.metadata import Table, StarFile

from emwrap.motioncor import Motioncor, McPipeline
from emwrap.tests import RelionTutorial

acquisition = {
    "voltage": 300.0,
    "magnification": 165000.0,
    "pixel_size": 1.19,
    "dose": 0.038,
    "total_dose": 3.5,
    "cs": 2.7,
    "gain": "data/20250419_040033_EER_GainReference.gain"
}

gpus = System.gpus()
ngpus = len(gpus)
gpuConfigs = []

ngpus = os.environ.get('EMWRAP_TEST_NGPUS', "4")
angular_search = int(os.environ.get('EMWRAP_TEST_ANGULAR_SEARCH', 12))

for n in ngpus.split(':'):
    gpuConfigs.append(list(range(int(n))))

perDeviceList = [2]  #, 4]

ppstar = 'default_pipeline.star'

if os.path.exists(ppstar):
    # Read job counter from the default_pipeline.star
    with StarFile(ppstar) as sf:
        jobCounter = int(sf.getTable('pipeline_general')[0].rlnPipeLineJobCounter)
else:
    jobCounter = 1


def runJob(cmd, argsJson, inputStr, alias):
    global jobCounter

    argsFn = f"args{jobCounter:03d}_{alias}.json"
    with open(argsFn, 'w') as f:
        json.dump(argsJson, f, indent=4)
    gm = GpuMonitor()
    gm.outputLog = f"gpu_monitor_{jobCounter:03d}.json"
    gm.start()
    Process.system(cmd.format(argsFn=argsFn, inputStr=inputStr))
    jobCounter += 1
    gm.stop()


def lastJob():
    return f"External/job{jobCounter - 1:03d}"


def lastInfo():
    with open(os.path.join(lastJob(), 'info.json')) as f:
        return json.load(f)


def mctf(gpus="0 1 2 3", perdevice=2):
    argsJson = {
        "acquisition": acquisition,
        "emw-warp-mctf": {
            "in_movies": "FIXME: EER pattern",
            "output": "FIXME: OUTPUT DIRECTORY",
            "gpu": gpus,
            "create_settings": {
                "--bin": 0,
                "--eer_ngroups": 6
            },
            "fs_motion_and_ctf": {
                "extra_args": {
                    "--c_range_max": 5,
                    "--c_range_min": 40,
                    "--c_defocus_max": 7,
                    "--c_defocus_min": 1.5,
                    "--c_use_sum": "",
                    "--out_averages": "",
                    "--out_average_halves": "",
                    "--perdevice": perdevice
                }
            }
        }
    }
    cmd = 'emw-relion -r "python -m emwrap.warp.warp_mctf --json {argsFn} -i {inputStr}" -w'
    runJob(cmd, argsJson, 'data/frames/*.eer', 'warp_mctf')


def aretomo(inputJob, gpus="0 1 2 3", perdevice=2):
    argsJson = {
        "acquisition": acquisition,
        "emw-warp-aretomo": {
            "in_movies": "FIXME: WARP FOLDER",
            "output": "FIXME: OUTPUT DIRECTORY",
            "gpu": gpus,
            "ts_import": {
                "--mdocs": "data/mdocs",
                "--min_ntilts": 35,
                "--override_axis": -95.5
            },
            "create_settings": {
                "--tomo_dimensions": "4096x4096x1600"
            },
            "ts_aretomo": {
                "extra_args": {
                    "--angpix": 9.52,
                    "--alignz": 1800,
                    "--axis_iter": 0,
                    "--axis_batch": 0,
                    "--perdevice": perdevice
                }
            }
        }
    }
    # python -m emwrap.warp.warp_aretomo --json args.json -i External/job004/
    cmd = 'emw-relion -r "python -m emwrap.warp.warp_aretomo --json {argsFn} -i {inputStr}" -w'
    runJob(cmd, argsJson, inputJob, 'warp_aretomo')


def ctfrec(inputJob, gpus="0 1 2 3", perdevice=2):
    argsJson = {
        "acquisition": acquisition,
        "emw-warp-ctfrec": {
            "in_movies": "FIXME: WARP FOLDER",
            "output": "FIXME: OUTPUT DIRECTORY",
            "gpu": gpus,
            "ts_ctf": {
                "extra_args": {
                    "--range_low": 35,
                    "--range_high": 6,
                    "--defocus_max": 7.5,
                    "--defocus_min": 1.5,
                    "--perdevice": perdevice
                }
            },
            "ts_reconstruct": {
                "extra_args": {
                    "--angpix": 9.52,
                    "--halfmap_frames": "",
                    "--perdevice": 2
                }
            }
        }
    }
    cmd = 'emw-relion -r "python -m emwrap.warp.warp_ctfrec --json {argsFn} -i {inputStr}" -w'
    runJob(cmd, argsJson, inputJob, 'warp_ctfrec')


def pytom(inputJob, gpus="0 1 2 3"):
    argsJson = {
        "acquisition": acquisition,
        "emw-pytom": {
            "in_movies": "FIXME: WARP FOLDER",
            "output": "FIXME: OUTPUT DIRECTORY",
            "gpu": gpus,
            "pytom": {
                "extra_args": {
                    "--angular-search": angular_search,
                    "--template": "data/pytom_templates/apoF/apoF_b64_repicked-BIN8.mrc",
                    "--mask": "data/pytom_templates/apoF/apoF_b64_BIN8_mask.mrc",
                    "--particle-diameter": 140,
                    "--defocus": 3,
                    "--high-pass": 400,
                    "--low-pass": 19.04,
                    "--random-phase-correction": "",
                    "--per-tilt-weighting": "",
                    "--tomogram-ctf-model": "phase-flip",
                    "--rng-seed": 420,
                    "--voltage": 300,
                    "-s": "4 4 4"
                }
            }
        }
    }
    cmd = 'emw-relion -r "python -m emwrap.pytom.pytom_pipeline --json {argsFn} -i {inputStr}" -w'
    runJob(cmd, argsJson, inputJob, 'pytom')


def all_single():
    mctf()
    aretomo(lastJob())
    ctfrec(lastJob())
    pytom(os.path.join(lastJob(), 'warp_tomostar'))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('step', choices=["mctf", "aretomo", "ctfrec", "pytom",
                                         "all_single"],
                        help="")
    parser.add_argument('config', nargs='?', default=argparse.SUPPRESS)
    args = parser.parse_args()

    if args.step == 'mctf':
        mctf()
    elif args.step == 'aretomo':
        aretomo(lastJob())
    elif args.step == 'ctfrec':
        ctfrec(lastJob())
    elif args.step == 'pytom':
        pytom(os.path.join(lastJob(), 'warp_tomostar'))
    elif args.step == 'all_single':
        all_single()


if __name__ == '__main__':
    main()
