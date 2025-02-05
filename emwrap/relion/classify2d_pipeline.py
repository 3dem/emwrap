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
import time
import argparse
from datetime import timedelta, datetime
from glob import glob

from emtools.utils import Color, Timer, Path, Process, FolderManager
from emtools.jobs import Batch
from emtools.metadata import Mdoc, StarFile

from emwrap.base import ProcessingPipeline


class StarBatchManager(FolderManager):
    """ Batch manager for input particles, grouped by Micrograph or GridSquare.
    """

    def __init__(self, outputPath, inputStar, groupColumn, **kwargs):
        """
        Args:
            outputPath: path where the batches folder will be created
            inputStar: input particles star file.
            groupColumn: column used to group particles.
                Usually gridSquare or micrographName
            minSize: minimum size for each batch
        """
        FolderManager.__init__(self, outputPath)
        self._inputStar = inputStar
        self._outputPath = outputPath
        self._groupColumn = groupColumn
        self._minSize = kwargs.get('minSize', 0)
        self._wait = kwargs.get('wait', 60)
        self._timeout = timedelta(seconds=kwargs.get('timeout', 7200))
        self._lastCheck = None  # Last timestamp when input was checked
        self._lastUpdate = None  # Last timestamp when new items were found
        self._count = 0
        self._rows = []
        self._batches = {}

    def generate(self):
        """ Generate batches based on the input items. """
        while not self.timedOut():
            mTime = datetime.fromtimestamp(os.path.getmtime(self._inputStar))
            if self._lastCheck is None or mTime > self._lastCheck:
                for batch in self._createNewBatches():
                    yield batch
            time.sleep(self._wait)

    def _createNewBatches(self):
        with StarFile(self._inputStar) as sf:
            tOptics = sf.getTable('optics')
            tParticles = sf.getTableInfo('particles')
            rows = []
            lastValue = None
            lastIndex = 0

            for row in sf.iterTable('particles'):
                value = getattr(row, 'rlnMicrographName')
                if lastValue is not None and lastValue != value and len(rows) > self._minSize:
                    yield self._createBatch(tOptics, tParticles, rows)
                rows.append(row)
                lastValue = value

            if rows:
                _writeStar(0)  # Write all remaining

    def _createBatch(self, tOptics, tParticles, rows):
        self._count += 1
        prefix = f'b{self._count:02}'
        batch_id = '2d_' + prefix
        batch = Batch(id=batch_id,
                      index=self._count,
                      path=self.join(batch_id))
        batch.create()

        outStarFile = batch.join(f'{prefix}_particles.star')
        with StarFile(outStarFile, 'w') as sfOut:
            sfOut.writeTimeStamp()
            sfOut.writeTable('optics', tOptics)
            sfOut.writeHeader('particles', tParticles)
            for row in rows:
                sfOut.writeRow(row)
        self.rows = []



    def timedOut(self):
        """ Return True when there has been timeout seconds
        since last new items were found. """
        if self._lastCheck is None or self._lastUpdate is None:
            return False
        else:
            return (self._lastCheck - self._lastUpdate) > self._timeout


class Relion2DPipeline(ProcessingPipeline):
    """ Pipeline specific to AreTomo processing. """
    def __init__(self, args):
        ProcessingPipeline.__init__(self, **args)
        self.gpuList = args['gpu_list'].split()

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
        else:
            output = os.path.join(batch['path'], 'output')
            Process.system(f"mv {output} {self.outputTsDir}/{batch['tsName']}")

        return batch

    def _iterMdocs(self):
        # TODO: support for streaming
        for mdocFn in glob(self.inputMdocs):
            mdoc = Mdoc.parse(mdocFn)
            mdoc['MdocFile'] = {'Path': mdocFn}
            yield mdoc

    def prerun(self):
        batchMgr = MdocBatchManager(self._iterMdocs(), self.tmpDir,
                                    suffix=self.mdoc_suffix)
        g = self.addGenerator(batchMgr.generate)
        outputQueue = None
        Process.system(f"mkdir -p {self.outputTsDir}")
        print(f"Creating {len(self.gpuList)} processing threads.")
        for gpu in self.gpuList:
            p = self.addProcessor(g.outputQueue,
                                  self.get_aretomo_proc(gpu),
                                  outputQueue=outputQueue)
            outputQueue = p.outputQueue

        self.addProcessor(outputQueue, self._output)


def main():
    p = argparse.ArgumentParser(prog='emw-aretomo')
    p.add_argument('--json',
                   help="Input all arguments through this JSON file. "
                        "The other arguments will be ignored. ")
    p.add_argument('--in_movies', '-i')
    p.add_argument('--output', '-o')
    p.add_argument('--aretomo_path', '-p')
    p.add_argument('--aretomo_args', '-a', default='')
    p.add_argument('--scratch', '-s', default='',
                   help="Scratch directory where to store intermediate "
                        "results of the processing. ")
    p.add_argument('--batch_size', '-b', type=int, default=8)
    p.add_argument('--j', help="Just to ignore the threads option from Relion")
    p.add_argument('--gpu', default='0')
    p.add_argument('--mdoc_suffix', '-m',
                   help="Suffix to be removed from the mdoc file names to "
                        "assign each tilt series' name. ")

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
            'batch_size': args.batch_size,
            'mdoc_suffix': args.mdoc_suffix
        }
        aretomo = AreTomoPipeline(argsDict)
        aretomo.run()


if __name__ == '__main__':
    main()
