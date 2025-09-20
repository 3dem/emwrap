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
    "WARP_LOADER": None,
    "ARETOMO_PATH": None,
    "ARETOMO2": None,
    "RELION_TOMOREFINE": None,

    'CUDA_VISIBLE_DEVICES': None,
    'EMWRAP_TEST_GPU_LIST': "",
    'EMWRAP_TEST_NGPUS': "4",
    "EMWRAP_TEST_PERDEVICE": "2",
    "EMWRAP_TEST_ANGULAR_SEARCH": "20"
}


def _getVar(key):
    return os.environ.get(key, VARS[key])


gpuList = _getVar('EMWRAP_TEST_GPU_LIST')
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


if gpuList:
    gpuConfigs.append(gpuList)
else:
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


class Mctf:
    def __init__(self, gpus, perdevice):
        binning = 1 if acquisition['sampling'] == '8k' else 0
        self.argsJson = {
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

    def run(self):
        cmd = 'emw-relion -r "python -m emwrap.warp.warp_mctf --json {argsFn} -i {inputStr}" -w'
        runJob(cmd, self.argsJson, 'data/frames/*.eer', 'warp_mctf')

    @staticmethod
    def run_all():
        for gpus, perdevice in iterGpuConfigs():
            Mctf(gpus, perdevice).run()


class Aretomo:
    def __init__(self, gpus, perdevice):
        self.argsJson = {
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

    def run(self, inputJob):
        # python -m emwrap.warp.warp_aretomo --json args.json -i External/job004/
        cmd = 'emw-relion -r "python -m emwrap.warp.warp_aretomo --json {argsFn} -i {inputStr}" -w'
        runJob(cmd, self.argsJson,  inputJob, 'warp_aretomo')

    @staticmethod
    def run_all(inputJob):
        for gpus, _ in iterGpuConfigs():
            Aretomo(gpus).run(inputJob)


class Ctfrec:
    def __init__(self, gpus, perdevice):
        self.argsJson = {
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
                        "--perdevice": perdevice
                    }
                }
            }
        }

    def run(self, inputJob):
        cmd = 'emw-relion -r "python -m emwrap.warp.warp_ctfrec --json {argsFn} -i {inputStr}" -w'
        runJob(cmd, self.argsJson, inputJob, 'warp_ctfrec')

    @staticmethod
    def run_all(inputJob):
        for gpus, perdevice in iterGpuConfigs():
            Ctfrec(gpus, perdevice).run(inputJob)


def pytom(inputTomostar):
    for gpus in gpuConfigs:
        run_pytom(inputTomostar, gpus)


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


def run_mcore(inputJob, gpus, perdevice):
    argsJson = {
        "acquisition": acquisition,
        "emw-warp-mcore": {
            "gpu": gpus
        }
    }
    cmd = 'emw-relion -r "python -m emwrap.warp.warp_mcore --json {argsFn} -i {inputStr}" -w'
    runJob(cmd, argsJson, inputJob, 'warp_mcore')


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
                    "-AlignZ": 1200,
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


def run_cryocare_train(inputJob):
    gpus = gpuConfigs[0]
    argsJson = {
        "acquisition": acquisition,
        "emw-cryocare-train": {
            "gpu": gpus
        }
    }
    cmd = 'emw-relion -r "python -m emwrap.cryocare.cryocare_train --json {argsFn} -i {inputStr}" -w'
    inputPattern = os.path.join(inputJob, "TS/*/*[EVN|ODD]_Vol.mrc")
    runJob(cmd, argsJson, inputPattern, 'cryocare_train')


def all_single():
    gpus = gpuConfigs[0]
    perdevice = perDeviceList[0]
    Mctf(gpus, perdevice).run()
    Aretomo(gpus, perdevice).run(lastJob())
    Ctfrec(gpus, perdevice).run(lastJob())
    inputTomostar = os.path.join(lastJob(), 'warp_tomostar')
    run_pytom(inputTomostar, gpus)
    inputCoords = os.path.join(lastJob(), 'Coordinates')
    run_export(inputCoords, gpus, perdevice)
    run_relion_tomorefine(lastJob())
    run_mcore(lastJob(), gpus, perdevice)


def run_preprocessing():
    gpus = gpuConfigs[0]
    perdevice = perDeviceList[0]
    # Create a single configuration file
    argsJson = {
        "acquisition": acquisition,
        "emw-warp-preprocessing": {
            "gpu": gpus,
            "mdocs": "data/mdocs/Position_*[0-9].mdoc"
        }
    }
    for _class in [Mctf, Aretomo, Ctfrec]:
        r = _class(gpus, perdevice)
        argsJson.update(r.argsJson)

    cmd = 'emw-relion -r "python -m emwrap.warp.warp_preprocessing --json {argsFn} -i {inputStr}" -w'
    runJob(cmd, argsJson, "data/frames", 'warp_preprocessing')


def printStats(folder, asJson):
    jobsMapping = {
        "job001": "warp-motion-ctf",
        "job002": "warp-aretomo2",
        "job003": "warp-ctf-reconstruct",
        "job004": "pytom-match-pick",
        "job005": "warp-export-particles",
        "job006": "relion-tomorefine",
        "job007": "warp-m-refinement"
    }
    print(f"\n>>>> Run: {folder}")
    infoFiles = glob.glob(f"{folder}/External/job0*/info.json")
    infoFiles.sort()
    headers = ["JOBID", "JOBNAME", "START", "END", "ELAPSED"]
    format_str = u'   {:<10}{:<25}{:<25}{:<25}{:<25}'
    rows = []

    if not asJson:
        print(format_str.format(*headers))

    for fn in infoFiles:
        with open(fn) as f:
            info = json.load(f)
            jobid = os.path.basename(os.path.dirname(fn))
            run = info['runs'][-1]
            jobname = jobsMapping[jobid]
            rowData = [
                jobid,
                jobname,
                run['start'] or '',
                run['end'] or '',
                run['elapsed'] or ''
            ]
            rows.append({k: v for k, v in zip(headers, rowData)})
            if not asJson:
                print(format_str.format(*rowData))
    if asJson:
        print(json.dumps(rows, indent=None))


def main():
    parser = argparse.ArgumentParser()
    g = parser.add_mutually_exclusive_group(required=True)
    g.add_argument('--warp', '-w', choices=["mctf", "aretomo", "ctfrec", "pytom", "export", "refine", "mcore",
                                            "all", "preprocessing"])
    g.add_argument('--at3', '-a', choices=["aretomo3", "cryocare_train", "all"])
    g.add_argument('--env', action="store_true",
                   help="Print current environment for the running the tests.")
    g.add_argument('--stats', nargs='+', default=None,
                   help="Input several run to compute timing stats. ")
    parser.add_argument('--json', action="store_true",
                        help="Output stats in JSON format.")

    parser.add_argument('--sampling', nargs='?', default='8k',
                        choices=['4k', '8k'])

    args = parser.parse_args()

    gainDict = {
        '4k': "data/20250627_094601_EER_GainReference.gain",
        '8k': "data/GainReference_8k.mrc"
    }

    acquisition['gain'] = gainDict[args.sampling]
    acquisition['sampling'] = args.sampling

    if args.env:
        _printVars()
    elif warp_step := args.warp:
        if warp_step == 'mctf':
            Mctf.run_all()
        elif warp_step == 'aretomo':
            Aretomo.run_all(lastJob())
        elif warp_step == 'ctfrec':
            Ctfrec.run_all(lastJob())
        elif warp_step == 'pytom':
            pytom(os.path.join(lastJob(), 'warp_tomostar'))
        elif warp_step == 'export':
            export(os.path.join(lastJob(), 'Coordinates'))
        elif warp_step == 'refine':
            run_relion_tomorefine(lastJob())
        elif warp_step == 'all':
            all_single()
        elif warp_step == 'mcore':
            gpus = gpuConfigs[0]
            perdevice = perDeviceList[0]
            run_mcore(lastJob(), gpus, perdevice)
        elif warp_step == 'preprocessing':
            run_preprocessing()
    elif at3_step := args.at3:
        if at3_step == 'aretomo3':
            run_aretomo3()
        elif at3_step == 'cryocare_train':
            run_cryocare_train(lastJob())
    elif args.stats:
        for folder in args.stats:
            printStats(folder, args.json)


if __name__ == '__main__':
    main()
