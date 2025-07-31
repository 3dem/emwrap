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
import subprocess
import pathlib
import sys
import json
import argparse
import math
from glob import glob
from pprint import pprint

from emtools.utils import Color, Timer, Path, Process
from emtools.jobs import MdocBatchManager, Args
from emtools.metadata import Mdoc, StarFile

from emwrap.base import ProcessingPipeline


class AreTomoPipeline(ProcessingPipeline):
    """ Pipeline specific to AreTomo processing. """
    name = 'emw-aretomo'
    input_name = 'in_movies'

    def __init__(self, input_args):
        ProcessingPipeline.__init__(self, input_args)
        args = self._args
        self.program = args.get('aretomo_path',
                                os.environ.get('ARETOMO_PATH', None))
        self.extraArgs = args['aretomo'].get('extra_args', {})
        self.gpuList = args['gpu'].split()
        self.outputTsDir = 'TS'
        self.inputMovies = args['in_movies']
        self.inputMdocPattern = args['mdoc']
        self.mdoc_suffix = args.get('mdoc_suffix', None)
        self.acq = self.loadAcquisition()
        self.inputGain = self.acq.get('gain', None)

    def aretomo(self, batch, **kwargs):
        gpu = kwargs['gpu']
        tsName = batch['tsName']
        batch.mkdir('output')
        logFn = batch.join('output', f'{tsName}_aretomo_log.txt')
        mdoc = batch['mdoc']
        ps = self.acq.pixel_size

        localMdoc = f"{batch['tsName']}.mdoc"

        # Let's write a local MDOC file with fixed filenames
        for _, section in mdoc.zvalues:
            section['SubFramePath'] = Mdoc.getSubFrameBase(section)
        mdoc.write(batch.join(localMdoc))

        args = Args({
            "-Cmd": 0,
            "-InMdoc": localMdoc,
            "-InSuffix": ".mdoc",
            "-OutDir": "output",
            "-Gpu": gpu,
            "-PixSize": ps,
            "-kV": self.acq.voltage,
            "-Cs": self.acq.cs,
            "-AmpContrast": self.acq.amplitude_contrast,
            "-AtBin": "8",  # FIXME
            "-AtPatch": "4 4",
            "-Wbp": "",
            "-TiltAxis": 85,
            "-TiltCor": 1,
            "-FlipVol": ""
        })
        args.update(self.extraArgs)
        # Make a local link to the input frame integration file
        args["-FmIntFile"] = batch.link(args["-FmIntFile"])
        # TODO: create the file if it does not exist
        startDiv = math.ceil(self.acq.total_dose)

        # Link the gain reference
        if self.inputGain:
            args['-Gain'] = batch.link(self.inputGain)

        batch.call(self.program, args)

        return batch

    def get_aretomo_proc(self, gpu):
        def _aretomo(batch):
            try:
                batch.tic()
                batch.log(f"Running aretomo, using {Color.cyan(f'GPU = {gpu}')}", flush=True)
                batch = self.aretomo(batch, gpu=gpu)
                batch.toc()
            except Exception as e:
                batch.error = e

            return batch

        return _aretomo

    def _output(self, batch):
        batch.log(f"Storing batch output", flush=True)
        if batch.error:
            batch.log(f"ERROR: {batch.error}")
        else:
            batchFolder = self.join(self.outputTsDir, batch['tsName'])
            Process.system(f"mv {batch.join('output')} {batchFolder}")
        self.updateBatchInfo(batch)
        totalInput = len(self.inputMdocs)
        totalOutput = len(self.info['batches'])
        percent = totalOutput * 100 / totalInput
        batch.log(f">>> Processed {Color.green(totalOutput)} out of "
                  f"{Color.red(totalInput)} "
                  f"({Color.bold('%0.2f' % percent)} %)", flush=True)
        return batch

    def _iterMdocs(self):
        # TODO: support for streaming
        for mdocFn in self.inputMdocs:
            mdoc = Mdoc.parse(mdocFn)
            mdoc['MdocFile'] = {'Path': mdocFn}
            yield mdoc

    def prerun(self):
        self.log(f"Using GPUs: {Color.cyan(str(self.gpuList))}", flush=True)
        self.inputMdocs = glob(self.inputMdocPattern)
        if not self.inputMdocs:
            raise Exception(f"No mdoc files were found with pattern: "
                            f"{self.inputMdocPattern}")
        batchMgr = MdocBatchManager(self._iterMdocs(), self.tmpDir,
                                    suffix=self.mdoc_suffix,
                                    movies=self.inputMovies)
        g = self.addGenerator(batchMgr.generate, queueMaxSize=4)
        outputQueue = None
        self.mkdir(self.outputTsDir)
        self.log(f"Creating {len(self.gpuList)} processing threads.")
        for gpu in self.gpuList:
            p = self.addProcessor(g.outputQueue,
                                  self.get_aretomo_proc(gpu),
                                  outputQueue=outputQueue)
            outputQueue = p.outputQueue

        self.addProcessor(outputQueue, self._output)


def main():
    AreTomoPipeline.runFromArgs()


if __name__ == '__main__':
    main()
