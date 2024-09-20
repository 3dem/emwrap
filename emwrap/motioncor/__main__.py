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

"""

"""

import os
import subprocess
import sys
import json
import argparse
from pprint import pprint

from emtools.utils import Color, Timer, Path
from emtools.jobs import Pipeline, BatchManager
from emtools.metadata import StarFile, StarMonitor


class McPipeline(Pipeline):
    """ Pipeline specific to Motioncor processing. """
    def __init__(self, args):
        Pipeline.__init__(self)

        self.program = args.get('motioncor_path',
                                os.environ.get('MOTIONCOR_PATH', None))
        self.extraArgs = args.get('motioncor_args', '')
        self.gpuList = args['gpu_list'].split()
        self.outputDir = args['output_dir']
        self.inputStar = args['input_star']
        self.batchSize = args.get('batch_size', 32)
        self.optics = None

    def _build(self):
        def _movie_filename(row):
            return row.rlnMicrographMovieName

        monitor = StarMonitor(self.inputStar, 'movies',
                              _movie_filename, timeout=30)

        batchMgr = BatchManager(self.batchSize, monitor.newItems(), self.outputDir,
                                itemFileNameFunc=_movie_filename)
        #

        g = self.addGenerator(batchMgr.generate)
        outputQueue = None
        print(f"Creating {len(self.gpuList)} processing threads.")
        for gpu in self.gpuList:
            p = self.addProcessor(g.outputQueue,
                                  self.get_motioncor_proc(gpu),
                                  outputQueue=outputQueue)
            outputQueue = p.outputQueue

        self.addProcessor(outputQueue, self._output)

    def motioncor(self, gpu, batch):
        batch_dir = batch['path']

        def _path(p):
            return os.path.join(batch_dir, p)

        os.mkdir(_path('output'))
        os.mkdir(_path('log'))

        logFn = _path('motioncor_log.txt')
        args = [self.program]

        ext = Path.getExt(batch['items'][0].rlnMicrographMovieName)
        extLower = ext.lower()

        if extLower.startswith('.tif'):
            inArg = '-InTiff'
        elif extLower.startswith('.mrc'):
            inArg = '-InMrc'
        else:
            raise Exception(f"Unsupported movie format: {ext}")

        # Load acquisition parameters from the optics table
        acq = self.optics[0]
        ps = acq.rlnMicrographOriginalPixelSize
        voltage = acq.rlnVoltage
        cs = acq.rlnSphericalAberration

        opts = f"{inArg} ./ -OutMrc output/aligned_ -InSuffix {ext} "
        opts += f"-Serial 1  -Gpu {gpu} -LogDir log/ "
        opts += f"-PixSize {ps} -kV {voltage} -Cs {cs} "
        opts += self.extraArgs
        args.extend(opts.split())
        batchStr = Color.cyan("BATCH_%02d" % batch['index'])
        t = Timer()

        print(f">>> {batchStr}: Running {Color.green(self.program)} {Color.bold(opts)}")

        with open(logFn, 'w') as logFile:
            subprocess.call(args, cwd=batch_dir, stderr=logFile, stdout=logFile)

        print(f">>> {batchStr}: Done! Elapsed: {t.getToc()}. "
              f"Log file: {Color.bold(logFn)}")

        return batch

    def get_motioncor_proc(self, gpu):
        def _motioncor(batch):
            return self.motioncor(gpu, batch)

        return _motioncor

    def _output(self, batch):
        batch_dir = batch['path']
        # FIXME: Check what we want to move to output
        #os.system(f'mv {batch_dir}/* {self.outputDir}/ && rm -rf {batch_dir}')
        return batch

    def run(self):
        with StarFile(self.inputStar) as sf:
            self.optics = sf.getTable('optics')
        self._build()
        print(f"Batch size: {self.batchSize}")
        Pipeline.run(self)

#
#
# def motioncor():
#     argsDict = {
#         '-Throw': 0 if self.isEER else (frame0 - 1),
#         '-Trunc': 0 if self.isEER else (numbOfFrames - frameN),
#         '-Patch': f"{self.patchX} {self.patchY}",
#         '-MaskCent': f"{self.cropOffsetX} {self.cropOffsetY}",
#         '-MaskSize': f"{cropDimX} {cropDimY}",
#         '-FtBin': self.binFactor.get(),
#         '-Tol': self.tol.get(),
#         '-PixSize': inputMovies.getSamplingRate(),
#         '-kV': inputMovies.getAcquisition().getVoltage(),
#         '-Cs': 0,
#         '-OutStack': 1 if self.doSaveMovie else 0,
#         '-Gpu': '%(GPU)s',
#         '-SumRange': "0.0 0.0",  # switch off writing out DWS,
#         '-LogDir': './'
#         # '-FmRef': 0
#     }


def main():
    p = argparse.ArgumentParser(prog='emw-motioncor')
    p.add_argument('--json', '-j',
                   help="Input all arguments through this JSON file. "
                        "The other arguments will be ignored. ")
    p.add_argument('--input', '-i')
    p.add_argument('--output', '-o')
    p.add_argument('--motioncor_path', '-p')
    p.add_argument('--motioncor_args', '-a', default='')
    p.add_argument('--batch_size', '-b', type=int, default=8)
    p.add_argument('--gpu')

    args = p.parse_args()

    if args.json:
        raise Exception("JSON input not yet implemented.")
    else:
        argsDict = {
            'input_star': args.input,
            'output_dir': args.output,
            'motioncor_args': args.motioncor_args,
            'gpu_list': args.gpu,
            'batch_size': args.batch_size
        }
        mc = McPipeline(argsDict)
        mc.run()


if __name__ == '__main__':
    main()
