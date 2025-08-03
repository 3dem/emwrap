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
    "pixel_size": 0.595,
    "dose": 0.038,
    "total_dose": 3.5,
    "cs": 2.7,
    "gain": "FIXME: 4K or 8K"
}

sampling = "4k"
gpus = System.gpus()
ngpus = len(gpus)
gpuConfigs = []

VARS = {
    "WARPTOOLS_PATH": None,
    "ARETOMO_PATH": None,
    "ARETOMO_VERSION": None,
    "RELION_TOMOREFINE": None,

    'EMWRAP_TEST_NGPUS': "4",
    "EMWRAP_TEST_PERDEVICE": "2",
    "EMWRAP_TEST_ANGULAR_SEARCH": "20"
}


def _getVar(key):
    return os.environ.get(key, VARS[key])


ngpus = _getVar('EMWRAP_TEST_NGPUS')
perdevice = _getVar('EMWRAP_TEST_PERDEVICE')
angular_search = int(_getVar('EMWRAP_TEST_ANGULAR_SEARCH'))


def _printHeader(msg):
    sep = '-' * 20
    print(f"\n# {sep} {msg} {sep}")


def _printVars():
    _printHeader(Color.green("External Programs"))
    checkVars = True
    for k, v in VARS.items():
        if checkVars and k.startswith('EMWRAP_TEST'):
            _printHeader(Color.cyan("Test Configuration Vars"))
            checkVars = False
        print(f"export {k}={_getVar(k)}")
    print()



for n in ngpus.split(':'):
    gpuConfigs.append(' '.join(str(g) for g in range(int(n))))

perDeviceList = perdevice.split(':')

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

def iterGpuConfigs():
    for gpus in gpuConfigs:
        for perdevice in perDeviceList:
            yield gpus, perdevice

def mctf():
    for gpus, perdevice in iterGpuConfigs():
        run_mctf(gpus, perdevice)


def run_mctf(gpus, perdevice):
    binning = 1 if acquisition['sampling'] == '8k' else 0

    argsJson = {
        "acquisition": acquisition,
        "emw-warp-mctf": {
            "gpu": gpus,
            "create_settings": {
                "--bin": binning,
                "--eer_ngroups": 11
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


def aretomo(inputJob):
    for gpus, perdevice in iterGpuConfigs():
        run_aretomo(inputJob, gpus, perdevice)


def run_aretomo(inputJob, gpus, perdevice):
    argsJson = {
        "acquisition": acquisition,
        "emw-warp-aretomo": {
            "gpu": gpus,
            "ts_import": {
                "--mdocs": "data/mdocs",
                # "--min_ntilts": 5,
                "--tilt_offset": 11,
                "--override_axis": 85
            },
            "create_settings": {
                "--tomo_dimensions": "4096x4096x1600"
            },
            "ts_aretomo": {
                "extra_args": {
                    "--angpix": 9.52,
                    "--alignz": 1800,
                    "--axis_iter": 2,
                    "--axis_batch": 15,
                    "--perdevice": 1
                }
            }
        }
    }
    # python -m emwrap.warp.warp_aretomo --json args.json -i External/job004/
    cmd = 'emw-relion -r "python -m emwrap.warp.warp_aretomo --json {argsFn} -i {inputStr}" -w'
    runJob(cmd, argsJson, inputJob, 'warp_aretomo')


def ctfrec(inputJob):
    for gpus, perdevice in iterGpuConfigs():
        run_ctfrec(inputJob, gpus, perdevice)


def run_ctfrec(inputJob, gpus, perdevice):
    argsJson = {
        "acquisition": acquisition,
        "emw-warp-ctfrec": {
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
                    "--angpix": 9.52,  # FIXME
                    "--halfmap_frames": "",
                    "--perdevice": 2
                }
            }
        }
    }
    cmd = 'emw-relion -r "python -m emwrap.warp.warp_ctfrec --json {argsFn} -i {inputStr}" -w'
    runJob(cmd, argsJson, inputJob, 'warp_ctfrec')


def pytom(inputTomostar):
    for gpus in gpuConfigs:
        run_ctfrec(inputTomostar, gpus)


def run_pytom(inputTomostar, gpus):
    argsJson = {
        "acquisition": acquisition,
        "emw-pytom": {
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
    runJob(cmd, argsJson, inputTomostar, 'pytom')


def export(inputCoords):
    for gpus, perdevice in iterGpuConfigs():
        run_export(inputCoords, gpus, perdevice)


def run_export(inputCoords, gpus, perdevice):
    argsJson = {
        "acquisition": acquisition,
        "emw-warp-export": {
            "gpu": gpus,
            "ts_export_particles": {
                "extra_args": {
                    "--box": 64,
                    "--diameter": 140,
                    "--coords_angpix": 9.52,  # FIXME
                    "--output_angpix": 4.76,  # FIXME
                    "--device_list": gpus,
                    "--perdevice": perdevice,
                    "--2d": ""  # or --3d
                }
            }
        }
    }
    cmd = 'emw-relion -r "python -m emwrap.warp.warp_export_particles --json {argsFn} -i {inputStr}" -w'
    runJob(cmd, argsJson, inputCoords, 'warp_export')


def run_relion_tomorefine(inputJob):
    inputParticles = os.path.join(inputJob, 'warp_particles.star')
    argsJson = {
        "acquisition": acquisition,
        "emw-relion-tomorefine": {
            "relion_refine": {
                "extra_args": {
                    "--particle_diameter": 140,
                    "--sym": "O",
                }
            }
        }
    }
    cmd = 'emw-relion -r "python -m emwrap.relion.tomorefine --json {argsFn} -i {inputStr}" -w'
    runJob(cmd, argsJson, inputParticles, 'relion_tomorefine')


def run_aretomo3():
    gpus = gpuConfigs[0]
    argsJson = {
        "acquisition": acquisition,
        "emw-aretomo": {
            "mdoc": "data/mdocs/*.mdoc",
            "gpu": gpus,
            "aretomo": {
                "extra_args": {
                    "-AtBin": 8,
                    "-TiltAxis": 85,
                    "-VolZ": 1600,
                    "-TotalDose": 3.5,
                    "-McBin": 2,
                    "-EerSampling": 2,
                    "-FmIntFile": "data/fmint.txt"
                }
            }
        }
    }
    cmd = 'emw-relion -r "python -m emwrap.aretomo.aretomo_pipeline --json {argsFn} -i {inputStr}" -w'
    runJob(cmd, argsJson, "data/frames", 'aretomo3')


def all_single():
    gpus = gpuConfigs[0]
    perdevice = perDeviceList[0]
    run_mctf(gpus, perdevice)
    run_aretomo(lastJob(), gpus, perdevice)
    run_ctfrec(lastJob(), gpus, perdevice)
    inputTomostar = os.path.join(lastJob(), 'warp_tomostar')
    run_pytom(inputTomostar, gpus)
    inputCoords = os.path.join(lastJob(), 'Coordinates')
    run_export(inputCoords, gpus, perdevice)
    run_relion_tomorefine(lastJob())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('step', choices=["env", "mctf", "aretomo", "ctfrec", "pytom",
                                         "export", "refine", "all_single", "aretomo3"],
                        help="")
    parser.add_argument('sampling', nargs='?', default='8k',
                        choices=['4k', '8k'])
    args = parser.parse_args()

    gainDict = {
        '4k': "data/20250627_094601_EER_GainReference.gain",
        '8k': "data/GainReference_8k.mrc"
    }

    acquisition['gain'] = gainDict[args.sampling]
    acquisition['sampling'] = args.sampling

    if args.step == 'env':
        _printVars()
    elif args.step == 'mctf':
        mctf()
    elif args.step == 'aretomo':
        aretomo(lastJob())
    elif args.step == 'ctfrec':
        ctfrec(lastJob())
    elif args.step == 'pytom':
        pytom(os.path.join(lastJob(), 'warp_tomostar'))
    elif args.step == 'export':
        export(os.path.join(lastJob(), 'Coordinates'))
    elif args.step == 'refine':
        run_relion_tomorefine(lastJob())
    elif args.step == 'all_single':
        all_single()
    elif args.step == 'aretomo3':
        run_aretomo3()


if __name__ == '__main__':
    main()
