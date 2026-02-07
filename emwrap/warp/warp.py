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

from emtools.utils import FolderManager
from emtools.metadata import StarFile, Table
from emtools.jobs import Batch, Args
from emtools.image import Image
from emwrap.base import ProcessingPipeline


class WarpBasePipeline(ProcessingPipeline):
    """ Base class to organize common functions/properties of different
    Warp pipelines.
    """
    PROGRAM = 'WARP'  # Key used in the config for getting the launcher
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
                    if fn.endswith('logs'):
                        outputFm.mkdir('logs')  # Don't copy logs
                    else:
                        outputFm.link(inputPath)
                else:
                    outputFm.copy(inputPath)

        for inputPath in inputs:
            if inputPath.endswith('.settings'):
                self.copy(inputPath)
            elif inputPath.endswith(self.TS) or inputPath.endswith(self.TM):
                _copyFolder(inputPath)
            else:  # warp_frameseries
                self.link(inputPath)

        # Link input gain file
        if gain := self.acq.get('gain', None):
            self.log(f"{self.name}: Linking gain gain: {gain}")
            self.link(gain)

    def prerunTs(self):
        """ Common operations for tilt-series prerun implementation in subclasses. """
        self.inputTs = self._args['input_tiltseries']
        batch = Batch(id=self.name, path=self.path)
        if self._args['__j'] != 'only_output':
            self.log("Running Warp commands.")
            self.runBatch(batch, inputTs=self.inputTs)
        else:
            self.log("Received special argument 'only_output', "
                     "only generating STAR files. ")

        self._output(batch)

    def write_ts_table(self, tableName, table, starFile):
        self.log(f"Writing: {starFile}")
        with StarFile(starFile, 'w') as sfOut:
            sfOut.writeTable(tableName, table, computeFormat='left', timeStamp=True)
    
    def get_launcher_arg(self, argName, varName):
        return self._args.get(argName, None) or ProcessingPipeline.get_launcher(varName)

    def _get_launcher(self):
        return self.get_launcher_arg('launcher_warp', 'WARP')


class WarpBaseTsAlign(WarpBasePipeline):
    """ Base class for all Warp TS alignment wrappers:
        ts_aretomo, ts_aretomo3
        ts_etomo_patches, ts_etomo_fiducials.
    It will run:
        - ts_import -> mdocs
        - create_settings -> warp_tiltseries.settings
        - run the specific alignment step
    """

    def _getInfo(self, tsAllTable):
        """ Load input or output information. """
        first = tsAllTable[0]
        ps = first.rlnTomoTiltSeriesPixelSize
        tsTable = StarFile.getTableFromFile(first.rlnTomoName, first.rlnTomoTiltSeriesStarFile)
        N = len(tsAllTable)
        n = len(tsTable)
        movieFn = tsTable[0].rlnMicrographMovieName
        dim = Image.get_dimensions(movieFn)
        self.log(f"get_dimensions: {dim}")
        x = dim[0]
        y = dim[1]
        return N, x, y, n, ps

    def runAlignment(self, batch):
        """ Abstract method that should be implemented in subclasses. """
        raise Exception("Missing implementation in base class.")

    def runBatch(self, batch, importInputs=True, **kwargs):
        # Input run folder from the Motion correction and CTF job
        inputTs = kwargs['inputTs']
        tsAllTable = StarFile.getTableFromFile('global', inputTs)
        N, x, y, n, ps = self._getInfo(tsAllTable)

        self.inputs = {
            'TiltSeries': {
                'label': 'Tilt Series',
                'type': 'TiltSeries',
                'info': f"{N} items, {x} x {y} x {n}, {ps:0.3f} Å/px",
                'files': [
                    [inputTs, 'TomogramGroupMetadata.star.relion.tomo.motioncorr']
                ]
            }
        }
        self.writeInfo()

        inputFolder = FolderManager(os.path.dirname(inputTs))

        # FIXME: Add validations if the input star exists and required warp folders
        batch.mkdir(self.TS)
        batch.mkdir(self.TM)

        # Link input frameseries folder, settings and gain reference
        if importInputs:
            self._importInputs(inputFolder, keys=['fs', 'fss', 'frames', 'mdocs'])

        # Run ts_import
        args = Args({
            'WarpTools': 'ts_import',
            '--frameseries': self.FS,
            '--tilt_exposure': self.acq['total_dose'],
            '--output': self.TM,
            '--mdocs': 'mdocs'
        })
        subargs = self.get_subargs('ts_import', '--')
        args.update(subargs)
        self.batch_execute('ts_import', batch, args)

        # Run create_settings
        args = Args({
            'WarpTools': 'create_settings',
            '--folder_data': self.TM,
            '--extension': "*.tomostar",
            '--folder_processing': self.TS,
            '--output': self.TSS,
            '--angpix': ps,
            '--exposure': self.acq['total_dose']
        })
        subargs = self.get_subargs('create_settings', '--')
        args.update(subargs)
        self.batch_execute('create_settings', batch, args)

        self.runAlignment(batch)

        self.updateBatchInfo(batch)

    def _output(self, batch):
        """ Register output STAR files. """
        def _float(v):
            return round(float(v), 2)

        batch.mkdir('tilt_series')
        self.log("Registering output STAR files.")
        tsAllTable = StarFile.getTableFromFile('global', self.inputTs)

        newTsStarFile = batch.join('tilt_series_aln.star')

        newTsAllTable = Table(tsAllTable.getColumnNames() + ['rlnTiltSeriesAligned'])
        dims = 0, 0, 0
        for tsRow in tsAllTable:
            tsName = tsRow.rlnTomoName
            # FIXME: The proper star files for each aligned TS needs to be generated
            tsStarFile = self.join('tilt_series', tsName + '.star')
            tsAligned = self.join(self.TS, 'tiltstack', tsName, f"{tsName}_aligned.mrc")
            if not os.path.exists(tsAligned):
                self.log(f"ERROR: Missing expected aligned TS: {tsAligned}")
                tsAligned = "None"  # FIXME Handle missing aligned TS
            else:
                newDims = Image.get_dimensions(tsAligned)
                if newDims[2] > dims[2]:
                    dims = newDims
            tsDict = tsRow._asdict()
            tsDict.update({
                'rlnTomoTiltSeriesStarFile': tsStarFile,
                'rlnTiltSeriesAligned': tsAligned
            })
            newTsAllTable.addRowValues(**tsDict)

        self.write_ts_table('global', newTsAllTable, newTsStarFile)
        N = len(newTsAllTable)
        # ps = newTsAllTable[0].rlnTomoTiltSeriesPixelSize
        newPs = float(self._args[self.output_angpix])
        x, y, n = dims
        self.outputs = {
            'TiltSeriesAligned': {
                'label': 'Tilt Series Aligned',
                'type': 'TiltSeriesAligned',
                'info': f"{N} items, {x} x {y} x {n}, {newPs:0.3f} Å/px",
                'files': [
                    [newTsStarFile, 'TomogramGroupMetadata.star.relion.tomo.aligntiltseries']
                ]
            }
        }
        self.updateBatchInfo(batch)

    def prerun(self):
        self.prerunTs()
