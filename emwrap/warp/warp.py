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

from emtools.utils import FolderManager, Path, Process
from emwrap.base import ProcessingPipeline
from emtools.metadata import StarFile, Acquisition


def get_loader():
    varPath = 'WARP_LOADER'

    if program := os.environ.get(varPath, None):
        if not os.path.exists(program):
            raise Exception(f"PyTom path ({varPath}={program}) does not exists.")
    else:
        raise Exception(f"PyTom path variable {varPath} is not defined.")

    return program


class WarpBasePipeline(ProcessingPipeline):
    """ Base class to organize common functions/properties of different
    Warp pipelines.
    """
    FRAMES = 'frames'
    MDOCS = 'mdocs'
    FS = 'warp_frameseries'
    FSS = f'{FS}.settings'
    TS = 'warp_tiltseries'
    TSS = f'{TS}.settings'
    TM = 'warp_tomostar'
    WARP_FOLDERS = [FS, TS, TM]

    INPUTS = {
        'fs': FS,
        'fss': FSS,
        'ts': TS,
        'tss': TSS,
        'tm': TM,
        FRAMES: FRAMES,
        MDOCS: MDOCS
    }

    @classmethod
    def copyInputs(cls, inputFolder, outputFolder, keys=None, gain=None, force=False):
        """ Inspect the input run folder and copy or link input folder/files
        if necessary.

        Args:
            inputFolder: the input folder containing settings and xml files
            outputFolder: should not exist. It will be created and setup
                as a proper warp folder to run commands.
            keys: input keys to import, if None, all inputs will be imported
            gain: if not None, it will be linked
            force: if True, the output folder will be clean if exists.
        """
        keys = cls.INPUTS.keys() if keys is None else keys

        def _getFM(i):
            return i if isinstance(i, FolderManager) else FolderManager(i)

        ifm = _getFM(inputFolder)
        ofm = _getFM(outputFolder)

        if ofm.exists() and not force:
            raise Exception("Output folder already exist.")

        ofm.create()

        inputs = [ifm.join(cls.INPUTS[k]) for k in keys]
        if m := [fn for fn in inputs if not os.path.exists(fn)]:
            raise Exception("Missing expected paths: " + str(m))

        def _copyFolder(inputFolder):
            baseFolder = os.path.basename(inputFolder)
            inputFm = FolderManager(inputFolder)
            outputFm = FolderManager(ofm.join(baseFolder))
            outputFm.create()
            for fn in inputFm.listdir():
                inputPath = inputFm.join(fn)
                if os.path.isdir(inputPath):
                    outputFm.link(inputPath)
                else:
                    outputFm.copy(inputPath)

        for inputPath in inputs:
            if inputPath.endswith('.settings'):
                ofm.copy(inputPath)
            elif inputPath.endswith(cls.TS):
                _copyFolder(inputPath)
            else:  # warp_frameseries and warp_tomostar
                ofm.link(inputPath)

        # Link input gain file
        if gain:
            ofm.link(gain)

    def __init__(self, args, output):
        ProcessingPipeline.__init__(self, args, output)
        gpus = self._args.get('gpus', '')
        self.gpuList = self.get_gpu_list(gpus) if gpus else []
        self.acq = self.loadAcquisition()
        self.loader = get_loader()
        if gainFile := self.acq.get('gain', None):
            self.gain = os.path.basename(gainFile)
        else:
            self.gain = None

    def _importInputs(self, inputRunFolder, keys=None):
        """ Inspect the input run folder and copy or link input folder/files
        if necessary. If gain is present in the acquisition, it will be linked.

        Args:
            inputRunFolder: the input run folder
            keys: input keys to import, if None, all inputs will be imported
        """
        print(f"{self.name}: Import inputs ", self.gain)
        keys = self.INPUTS.keys() if keys is None else keys

        if isinstance(inputRunFolder, FolderManager):
            ifm = inputRunFolder
        else:
            ifm = FolderManager(inputRunFolder)

        inputs = [ifm.join(self.INPUTS[k]) for k in keys]
        if m := [fn for fn in inputs if not os.path.exists(fn)]:
            raise Exception("Missing expected paths: " + str(m))

        def _copyFolder(inputFolder):
            baseFolder = os.path.basename(inputFolder)
            inputFm = FolderManager(inputFolder)
            outputFm = FolderManager(self.join(baseFolder))
            outputFm.create()
            for fn in inputFm.listdir():
                inputPath = inputFm.join(fn)
                if os.path.isdir(inputPath):
                    outputFm.link(inputPath)
                else:
                    outputFm.copy(inputPath)

        for inputPath in inputs:
            if inputPath.endswith('.settings'):
                self.copy(inputPath)
            elif inputPath.endswith(self.TS):
                _copyFolder(inputPath)
            else:  # warp_frameseries and warp_tomostar
                self.link(inputPath)

        # Link input gain file
        if gain := self.acq.get('gain', None):
            self.log(f"{self.name}: Linking gain gain: {gain}")
            self.link(gain)

    def batch_execute(self, label, batch, args, logfile=None):
        """ Shortcut to execute a batch. """
        logfile = logfile or self.join('run.out')
        with batch.execute(label):
            batch.call(self.loader, args, logfile=logfile)
