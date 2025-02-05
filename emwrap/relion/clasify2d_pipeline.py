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

from emtools.utils import Color, Timer, Path, Process, FolderManager
from emtools.jobs import BatchManager
from emtools.metadata import Mdoc

from emwrap.base import ProcessingPipeline


def _baseSubframe(section):
    """ Extract the subframe base filename. """
    subFramePath = section.get('SubFramePath', '')
    return pathlib.PureWindowsPath(subFramePath).parts[-1]


class StarBatchManager(FolderManager):
    """ Batch manager for input particles, grouped by Micrograph or GridSquare.
    """

    def __init__(self, outputPath, inputStar, groupColumn, **kwargs):
        """
        Args:
            outputPath: path where the batches folder will be created
            inputStar: input particles star file.
            groupColumn: column used to group particles.
                Usuaully gridSquare or micrographName
            minSize: minimum size for each batch
        """
        FolderManager.__init__(outputPath)
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

    def _subframePath(self, mdocFn, section):
        return os.path.join(os.path.dirname(mdocFn), _baseSubframe(section))
        section['SubFramePath'] = self.join(os.path.dirname(mdocFn), base)

    def _tsName(self, mdocFn):
        name = Path.removeBaseExt(mdocFn)
        if self._suffix:
            name = name.replace(self._suffix, '')
        return name

    def generate(self):
        """ Generate batches based on the input items. """
        while not self.timedOut():
            now = datetime.now()
            mTime = datetime.fromtimestamp(os.path.getmtime(self.fileName))

            if self._lastCheck is None or mTime > self._lastCheck:
                for batch in self._createNewBatches():
                    yield batch

            time.sleep(self._wait)

    def _createBatch(self):
        self.count += 1

        outStarFile = Path.replaceExt(starFile, f'_{count:03}.star')
        with StarFile(outStarFile, 'w') as sfOut:
            sfOut.writeTimeStamp()
            sfOut.writeTable('optics', tOptics)
            sfOut.writeHeader('particles', tParticles)
            for row in rows:
                sfOut.writeRow(row)
        self.rows = []

    def _createNewBatches(self):
        with StarFile(self._inputStar) as sf:
            tOptics = sf.getTable('optics')
            tParticles = sf.getTableInfo('particles')
            self.rows = []

            def _writeStar():


            lastValue = None
            lastIndex = 0

            for row in sf.iterTable('particles'):
                value = getattr(row, column)
                if lastValue is not None and lastValue != value and len(rows) > self._minSize:
                    yield _writeStar()
                rows.append(row)
                lastValue = value

            if rows:
                _writeStar(0)  # Write all remaining


    def timedOut(self):
        """ Return True when there has been timeout seconds
        since last new items were found. """
        if self._lastCheck is None or self._lastUpdate is None:
            return False
        else:
            return (self._lastCheck - self._lastUpdate) > self._timeout


class AreTomoPipeline(ProcessingPipeline):
    """ Pipeline specific to AreTomo processing. """
    def __init__(self, args):
        ProcessingPipeline.__init__(self, **args)
        self.program = args.get('aretomo_path',
                                os.environ.get('ARETOMO_PATH', None))
        self.extraArgs = args.get('aretomo_args', '')
        self.gpuList = args['gpu_list'].split()
        self.outputTsDir = self.join('TS')
        self.inputMdocs = args['input_mdocs']
        self.mdoc_suffix = args['mdoc_suffix']

    def aretomo(self, gpu, batch):
        batch_dir = batch['path']

        def _path(*p):
            return os.path.join(batch_dir, *p)

        tsName = batch['tsName']
        os.mkdir(_path('output'))
        logFn = _path('output', f'{tsName}_aretomo_log.txt')
        args = [self.program]
        mdoc = batch['mdoc']
        ps = mdoc['global']['PixelSpacing']

        localMdoc = f"{batch['tsName']}.mdoc"

        # Let's write a local MDOC file with fixed filenames
        for _, section in mdoc.zvalues:
            section['SubFramePath'] = _baseSubframe(section)
        mdoc.write(_path(localMdoc))

        opts = f"-Cmd 0 -InMdoc {localMdoc} -InSuffix .mdoc -OutDir output "
        opts += f"-Gpu {gpu} -PixSize {ps} "
        # Example of extraArgs:
        # -McPatch 5 5 -McBin 2 -Group 4 8 -AtBin 4 -AtPatch 4 4
        opts += self.extraArgs
        args.extend(opts.split())

        batchStr = Color.cyan(f"BATCH_{batch['index']:02} - {batch['tsName']}")
        t = Timer()
        print(f">>> {batchStr}: Running:  {Color.green(self.program)} {Color.bold(opts)}")

        with open(logFn, 'w') as logFile:
            logFile.write(f"\n{self.program} {opts}\n\n")
            logFile.flush()

            subprocess.call(args, cwd=batch_dir, stderr=logFile, stdout=logFile)

            elapsed = f"Elapsed: {t.getToc()}"
            logFile.write(f"\n{elapsed}\n\n")

        print(f">>> {batchStr}: Done! {elapsed}. "
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
