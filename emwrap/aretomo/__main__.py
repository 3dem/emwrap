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
import pathlib
import sys
import json
import argparse
from pprint import pprint
from glob import glob

from emtools.utils import Color, Timer, Path
from emtools.jobs import ProcessingPipeline, BatchManager
from emtools.metadata import Mdoc


def _baseSubframe(section):
    """ Extract the subframe base filename. """
    subFramePath = section.get('SubFramePath', '')
    return pathlib.PureWindowsPath(subFramePath).parts[-1]


class MdocBatchManager(BatchManager):
    """ Batch manager for Tilt-series. """
    def __init__(self, tsIterator, workingPath):
        """
        Args:
            tsIterator: input tilt-series iterator
            workingPath: path where the batches folder will be created
            itemFileNameFunc: function to extract a filename from each item
        """
        BatchManager.__init__(self, 0, tsIterator, workingPath,
                              itemFileNameFunc=lambda item: item[1]['SubFramePath'])

    def _subframePath(self, mdocFn, section):
        return os.path.join(os.path.dirname(mdocFn), _baseSubframe(section))
        section['SubFramePath'] = self.join(os.path.dirname(mdocFn), base)

    def generate(self):
        """ Generate batches based on the input items. """
        for mdoc in self._items:
            mdocFn = mdoc['MdocFile']['Path']
            self._itemFileNameFunc = lambda item: self._subframePath(mdocFn, item[1])
            batch = self._createBatch(mdoc.zvalues)
            batch['mdoc'] = mdoc
            # Link also the mdoc file
            #self.createBatchLink(batch['path'], mdoc['MdocFile']['Path'])
            yield batch


class AreTomoPipeline(ProcessingPipeline):
    """ Pipeline specific to AreTomo processing. """
    def __init__(self, args):
        ProcessingPipeline.__init__(self, os.getcwd(), args['output_dir'])
        self.program = args.get('aretomo_path',
                                os.environ.get('ARETOMO_PATH', None))
        self.extraArgs = args.get('aretomo_args', '')
        self.gpuList = args['gpu_list'].split()
        self.outputMicDir = self.join('Micrographs')
        self.inputMdocs = args['input_mdocs']

    def aretomo(self, gpu, batch):
        batch_dir = batch['path']

        def _path(p):
            return os.path.join(batch_dir, p)

        os.mkdir(_path('output'))
        os.mkdir(_path('logs'))
        logFn = _path('aretomo_log.txt')
        args = [self.program]
        mdoc = batch['mdoc']
        ps = mdoc['global']['PixelSpacing']

        # Let's write a local MDOC file with fixed filenames
        for _, section in mdoc.zvalues:
            section['SubFramePath'] = _baseSubframe(section)
        mdoc.write(_path('local.mdoc'))

        opts = f"-Cmd 0 -InMdoc local.mdoc -InSuffix .mdoc -OutDir output "
        opts += f"-Gpu {gpu} -McPatch 5 5 -McBin 2 -Group 4 8 -AtBin 4 -AtPatch 4 4"
        opts += f"-PixSize {ps} "
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

    def get_aretomo_proc(self, gpu):
        def _aretomo(batch):
            try:
                batch = self.aretomo(gpu, batch)
            except Exception as e:
                batch['error'] = str(e)
            return batch

        return _aretomo

    def _output(self, batch):
        if 'error' in batch:
            print(f"Failed batch {batch['id']}, error: {batch['error']}")
        return batch

    def _iterMdocs(self):
        # TODO: support for streaming
        for mdocFn in glob(self.inputMdocs):
            mdoc = Mdoc.parse(mdocFn)
            mdoc['MdocFile'] = {'Path': mdocFn}
            yield mdoc

    def run(self):
        batchMgr = MdocBatchManager(self._iterMdocs(), self.outputDir)
        g = self.addGenerator(batchMgr.generate)
        outputQueue = None
        print(f"Creating {len(self.gpuList)} processing threads.")
        for gpu in self.gpuList:
            p = self.addProcessor(g.outputQueue,
                                  self.get_aretomo_proc(gpu),
                                  outputQueue=outputQueue)
            outputQueue = p.outputQueue

        self.addProcessor(outputQueue, self._output)

        ProcessingPipeline.run(self)


def main():
    p = argparse.ArgumentParser(prog='emw-aretomo')
    p.add_argument('--json',
                   help="Input all arguments through this JSON file. "
                        "The other arguments will be ignored. ")
    p.add_argument('--in_movies', '-i')
    p.add_argument('--output', '-o')
    p.add_argument('--aretomo_path', '-p')
    p.add_argument('--aretomo_args', '-a', default='')
    p.add_argument('--batch_size', '-b', type=int, default=8)
    p.add_argument('--j', help="Just to ignore the threads option from Relion")
    p.add_argument('--gpu', default='0')

    args = p.parse_args()

    if len(sys.argv) == 1:
        p.print_help()
        sys.exit(0)

    if args.json:
        raise Exception("JSON input not yet implemented.")
    else:
        argsDict = {
            'input_mdocs': args.in_movies,
            'output_dir': args.output,
            'aretomo_args': args.aretomo_args,
            'gpu_list': args.gpu,
            'batch_size': args.batch_size
        }
        aretomo = AreTomoPipeline(argsDict)
        aretomo.run()


if __name__ == '__main__':
    main()
