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
import shutil
import sys
import json
import argparse
from pprint import pprint

from emtools.utils import Color, Timer, Path
from emtools.jobs import ProcessingPipeline, BatchManager, Args
from emtools.metadata import Table, Column, StarFile, StarMonitor, TextFile

from .motioncor import Motioncor


# def _optics_to_acq(t):
#     r = t[0]
#     print("Optics row:", r)
#     return {
#         'pixel_size': r.rlnMicrographOriginalPixelSize,
#         'voltage': r.rlnVoltage,
#         'cs': r.rlnSphericalAberration,
#         'amplitud_constrast': r.rlnAmplitudeContrast
#     }


class McPipeline(ProcessingPipeline):
    """ Pipeline specific to Motioncor processing. """
    def __init__(self, args):
        ProcessingPipeline.__init__(self, **args)

        self.gpuList = args['gpu_list'].split()
        self.outputMicDir = self.join('Micrographs')
        self.inputStar = args['input_star']
        self.batchSize = args.get('batch_size', 32)
        self.optics = None
        with StarFile(self.inputStar) as sf:
            self.optics = sf.getTable('optics')

        self._outputDict = {
            'rlnMicrographName': '.mrc',
            'rlnCtfPowerSpectrum': '_Ctf.mrc',
            'rlnMicrographMetadata': '_Ctf.txt',
            'rlnOpticsGroup': None
        }

        o = self.optics[0]  # shortcut
        mc_args = {
            '-PixSize': o.rlnMicrographOriginalPixelSize,
            '-kV': o.rlnVoltage,
            '-Cs': o.rlnSphericalAberration
        }
        mc_args.update(args.get('motioncor_args', {}))
        self.mc = Motioncor(mc_args, **args)

    def _build(self):
        def _movie_filename(row):
            return row.rlnMicrographMovieName

        monitor = StarMonitor(self.inputStar, 'movies',
                              _movie_filename, timeout=30)

        batchMgr = BatchManager(self.batchSize, monitor.newItems(), self.outputDir,
                                itemFileNameFunc=_movie_filename)

        g = self.addGenerator(batchMgr.generate)
        outputQueue = None
        print(f"Creating {len(self.gpuList)} processing threads.")
        for gpu in self.gpuList:
            p = self.addProcessor(g.outputQueue,
                                  self.get_motioncor_proc(gpu),
                                  outputQueue=outputQueue)
            outputQueue = p.outputQueue

        self.addProcessor(outputQueue, self._output)

    def get_motioncor_proc(self, gpu):
        def _motioncor(batch):
            return self.mc.process_batch(gpu, batch)
        return _motioncor

    def _output(self, batch):
        self.mc.parse_batch(batch, self.outputMicDir)
        for item, r in zip(batch['items'], batch['results']):
            if not 'error' in r:
                self._outSf.writeRowValues([
                    r['rlnMicrographName'],
                    r['rlnMicrographMetadata'],
                    1,  # fixme: optics group
                    -999.0, -999.0, -999.0  # fixme motion
                ])
            else:
                pass  # TODO write failed items for inspection or retry

        return batch

    def _writeMicrographsTableHeader(self):
        columns = [
            'rlnMicrographName',
            'rlnMicrographMetadata',
            'rlnOpticsGroup',
            'rlnAccumMotionTotal',
            'rlnAccumMotionEarly',
            'rlnAccumMotionLate'
        ]
        # FIXME: Now we are ignoring the CTF results
        # if self.mc.version >= 3:
        #     columns.extend([
        #         'rlnCtfImage',
        #         'rlnDefocusU',
        #         'rlnDefocusV',
        #         'rlnCtfAstigmatism',
        #         'rlnDefocusAngle',
        #         'rlnCtfFigureOfMerit',
        #         'rlnCtfMaxResolution'])

        self.micTable = Table(columns=columns)
        self._outSf.writeHeader('micrographs', self.micTable)
        self._outSf.flush()

    def prerun(self):
        with StarFile(self.inputStar) as sf:
            self.optics = sf.getTable('optics')

        # Create a new optics table adding pixelSize
        cols = list(self.optics.getColumns())
        cols.append(Column('rlnMicrographPixelSize', type=float))
        self.newOptics = Table(columns=cols)

        for row in self.optics:
            d = row._asdict()
            d['rlnMicrographPixelSize'] = row.rlnMicrographOriginalPixelSize  #FIXME incorrect with binning
            self.newOptics.addRowValues(**d)

        if not os.path.exists(self.outputMicDir):
            os.mkdir(self.outputMicDir)

        self._build()
        print(f"Batch size: {self.batchSize}")
        outName = 'corrected_micrographs.star'
        self._outFn = self.join(outName)
        self._outFile = open(self._outFn, 'w')  #FIXME improve for continue
        self._outSf = StarFile(self._outFile)
        self._outSf.writeTable('optics', self.newOptics)
        self._writeMicrographsTableHeader()
        # Write Relion-compatible nodes files
        """
        data_output_nodes
loop_
_rlnPipeLineNodeName #1
_rlnPipeLineNodeType #2
External/job006/coords_suffix_topaz.star            2 
        """
        with StarFile(self.join('RELION_OUTPUT_NODES.star'), 'w') as sf:
            t = Table(columns=['rlnPipeLineNodeName', 'rlnPipeLineNodeType'])
            t.addRowValues(self._outFn, 1)
            sf.writeTable('output_nodes', t)

        with open(self.join('job.json'), 'w') as f:
            json.dump({
                'inputs': [self.inputStar],
                'outputs': [self._outFn]
            }, f)

    def postrun(self):
        self._outSf.close()

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
    p.add_argument('--json',
                   help="Input all arguments through this JSON file. "
                        "The other arguments will be ignored. ")
    p.add_argument('--in_movies', '-i')
    p.add_argument('--output', '-o')
    p.add_argument('--motioncor_path', '-p')
    p.add_argument('--motioncor_args', '-a', default='')
    p.add_argument('--batch_size', '-b', type=int, default=8)
    p.add_argument('--j', help="Just to ignore the threads option from Relion")
    p.add_argument('--gpu')

    args = p.parse_args()

    if args.json:
        raise Exception("JSON input not yet implemented.")
    else:
        argsDict = {
            'input_star': args.in_movies,
            'output_dir': args.output,
            'motioncor_path': args.motioncor_path,
            'motioncor_args': args.motioncor_args,
            'gpu_list': args.gpu,
            'batch_size': args.batch_size
        }
        mc = McPipeline(argsDict)
        mc.run()


if __name__ == '__main__':
    main()
