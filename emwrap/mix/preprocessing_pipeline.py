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
from emtools.metadata import Table, Column, StarFile, StarMonitor, TextFile

from emwrap.base import ProcessingPipeline
from emwrap.relion import RelionStar
from .preprocessing import Preprocessing


class PreprocessingPipeline(ProcessingPipeline):
    """ Pipeline to run Preprocessing in batches. """
    def __init__(self, args):
        ProcessingPipeline.__init__(self, **args)

        self.gpuList = args['gpu_list'].split()
        self.outputMicDir = self.join('Micrographs')
        self.inputStar = args['input_star']
        self.batchSize = args.get('batch_size', 32)
        self.acq = RelionStar.get_acquisition(self.inputStar)
        pp_args = args['preprocessing_args']
        self.preprocessing = Preprocessing(pp_args)

    def _build(self):
        g = self.addMoviesGenerator(self.inputStar, self.batchSize)
        outputQueue = None
        print(f"Creating {len(self.gpuList)} processing threads.")
        for gpu in self.gpuList:
            p = self.addProcessor(g.outputQueue,
                                  self.get_preprocessing(gpu),
                                  outputQueue=outputQueue)
            outputQueue = p.outputQueue

        self.addProcessor(outputQueue, self._output)

    def get_preprocessing(self, gpu):
        def _preprocessing(batch):
            return self.preprocessing.process_batch(batch, gpu=gpu)
        return _preprocessing

    def _output(self, batch):
        return batch

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


def main():
    p = argparse.ArgumentParser(prog='emw-preprocessing')
    p.add_argument('--json',
                   help="Input all arguments through this JSON file. "
                        "The other arguments will be ignored. ")
    p.add_argument('--in_movies', '-i')
    p.add_argument('--preprocessing_args', '-a')
    p.add_argument('--output', '-o')
    p.add_argument('--batch_size', '-b', type=int, default=8)
    p.add_argument('--j', help="Just to ignore the threads option from Relion")
    p.add_argument('--gpu')

    args = p.parse_args()

    if args.json:
        raise Exception("JSON input not yet implemented.")
    else:
        with open(args.preprocessing_args) as f:
            preprocessing_args = json.load(f)

        argsDict = {
            'input_star': args.in_movies,
            'output_dir': args.output,
            'gpu_list': args.gpu,
            'batch_size': args.batch_size,
            'preprocessing_args': preprocessing_args
        }
        mc = PreprocessingPipeline(argsDict)
        mc.run()


if __name__ == '__main__':
    main()
