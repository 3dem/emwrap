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
import shutil
import numpy as np
from collections import defaultdict
from glob import glob

from emtools.utils import FolderManager, Path
from emtools.metadata import StarFile, Table, RelionStar, WarpXml, Imod
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
    M = 'm'
    WARP_FOLDERS = [FS, TS, TM]

    INPUTS = {
        'fs': FS,
        'fss': FSS,
        'ts': TS,
        'tss': TSS,
        'tm': TM,
        FRAMES: FRAMES,
        MDOCS: MDOCS,
        M: M
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
        if gpus is not None and gpus != '':
            gpus = str(gpus).strip()
        else:
            gpus = ''
        self.gpuList = self.get_gpu_list(gpus) if gpus else []
        inputStar = (self._args.get('input_tiltseries')
                     or self._args.get('input_tomograms'))
        self.acq = self.loadAcquisition(inputStar)
        if gainFile := self.acq.get('gain', None):
            self.gain = os.path.basename(gainFile)
        else:
            self.gain = None

    def _importInputs(self, inputRunFolder, keys=None, dest=None):
        """ Inspect the input run folder and copy or link input folder/files
        if necessary. If gain is present in the acquisition, it will be linked.

        Args:
            inputRunFolder: the input run folder
            keys: input keys to import, if None, all inputs will be imported
            dest: optional destination folder (defaults to the job folder)
        """
        print(f"{self.name}: Import inputs ", self.gain)
        if keys is None:
            keys = [k for k in self.INPUTS if k != self.M]  # all keys except m

        if isinstance(inputRunFolder, FolderManager):
            ifm = inputRunFolder
        else:
            ifm = FolderManager(inputRunFolder)

        if dest is None:
            destFm = self
        elif isinstance(dest, FolderManager):
            destFm = dest
        else:
            destFm = FolderManager(dest)

        inputs = [ifm.join(self.INPUTS[k]) for k in keys]
        if m := [fn for fn in inputs if not os.path.exists(fn)]:
            raise Exception("Missing expected paths: " + str(m))

        def _copyFolder(inputFolder):
            baseFolder = os.path.basename(inputFolder)
            inputFm = FolderManager(inputFolder)
            outputFm = FolderManager(destFm.join(baseFolder))
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

        def _copyMFolder(inputFolder):
            if dest is not None:
                raise Exception("Cannot import 'm' folder into a subfolder.")
            dst = self.mkdir(self.M)
            Path.rsync(inputFolder, dst, '--exclude', 'versions')

        for inputPath in inputs:
            if inputPath.endswith('.settings'):
                destFm.copy(inputPath)
            elif inputPath.endswith('/m'):
                _copyMFolder(inputPath)
            elif inputPath.endswith(self.TS) or inputPath.endswith(self.TM):
                _copyFolder(inputPath)
            else:  # warp_frameseries
                destFm.link(inputPath)

        # Link input gain file (only at job root)
        if dest is None and (gain := self.acq.get('gain', None)):
            self.log(f"{self.name}: Linking gain gain: {gain}")
            self.link(gain)

    def prerunTs(self):
        """ Common operations for tilt-series prerun implementation in subclasses. """
        self.inputTs = self._args['input_tiltseries']
        batch = Batch(id=self.name, path=self.path)
        if not self._register_output_only():
            self.log("Running Warp commands.")
            self.runBatch(batch, inputTs=self.inputTs)
        else:
            self.log("Received special argument 'register_output_only', "
                     "only generating STAR files. ")

        self._output(batch)

    def write_ts_table(self, tableName, table, starFile):
        self.log(f"Writing: {starFile}")
        with StarFile(starFile, 'w') as sfOut:
            sfOut.writeTable(tableName, table, computeFormat='left', timeStamp=True)

    def appendGlobalTsRow(self, starFile, rowDict):
        """Append one global row; all rows must share the same columns."""
        rowDict = dict(rowDict)
        if os.path.exists(starFile):
            table = StarFile.getTableFromFile('global', starFile)
            colNames = set(table.getColumnNames())
            rowKeys = set(rowDict.keys())
            if rowKeys != colNames:
                missing = sorted(colNames - rowKeys)
                extra = sorted(rowKeys - colNames)
                parts = []
                if missing:
                    parts.append(f"missing columns: {missing}")
                if extra:
                    parts.append(f"extra columns: {extra}")
                raise ValueError(
                    f"Column mismatch appending to {starFile}: {'; '.join(parts)}"
                )
            table.addRowValues(**rowDict)
        else:
            table = Table.fromDict(rowDict)
        self.write_ts_table('global', table, starFile)

    def targetPs(self, inputPs):
        """ Return target pixel size from create_settings.bin_angpix, or inputPs. """
        v = (self._args.get('create_settings.bin_angpix', '') or
             self._args.get('mctf.create_settings.bin_angpix', '') or 0)
        return float(v) or inputPs

    def updateMctfTsDict(self, tsDict, mdocFile, mdocsFm):
        """ Update tsDict with MCTF Relion labels and build the enriched TS table.

        Args:
            tsDict: tilt-series row as a dict (updated in place on success / missing movies)
            mdocFile: source mdoc path for this TS
            mdocsFm: FolderManager where mdocs are copied

        Returns:
            (ok, newTsTable, dims) — ok is False when the TS should go to the
            failed table; newTsTable is the enriched frame table when ok;
            dims are average micrograph dimensions when found.
        """
        def _float(v):
            return round(float(v), 2)

        tsName = tsDict['rlnTomoName']
        tsStarFile = self.join('tilt_series', tsName + '.star')
        newPs = self.targetPs(tsDict['rlnMicrographOriginalPixelSize'])

        if not mdocFile or not os.path.exists(mdocFile):
            self.log(f"Mdoc {mdocFile} not found for TS {tsName}, skipping...")
            return False, None, None

        tsTable = StarFile.getTableFromFile(tsName, tsDict['rlnTomoTiltSeriesStarFile'])

        # Each input movie must have xml + average mrc (same idea as WarpAreTomo
        # requiring aligned stack per TS). Collect missing before building output.
        missing = []
        for frameRow in tsTable:
            moviePrefix = Path.removeBaseExt(frameRow.rlnMicrographMovieName)
            movieMrc = moviePrefix + '.mrc'
            movieXml = self.join(self.FS, moviePrefix + '.xml')
            movieAvgMrc = self.join(self.FS, 'average', movieMrc)
            if not os.path.exists(movieXml):
                missing.append((moviePrefix, 'xml', movieXml))
            if not os.path.exists(movieAvgMrc):
                missing.append((moviePrefix, 'average mrc', movieAvgMrc))

        tsDict.update({
            'rlnTomoTiltSeriesPixelSize': newPs,
            'rlnTomoTiltSeriesStarFile': tsStarFile
        })
        dstMdocFile = mdocsFm.join(f'{tsName}.mdoc')
        shutil.copy(mdocFile, dstMdocFile)
        tsDict['rlnTomoMdocFile'] = dstMdocFile

        if missing:
            for moviePrefix, reason, path in missing:
                self.log(f"ERROR: Missing {reason} for movie {moviePrefix}: {path}")
            tsDict['rlnTomoTiltSeriesStarFile'] = "None"
            return False, None, None

        extra_cols = [
            'rlnCtfPowerSpectrum', 'rlnMicrographName', 'rlnMicrographMetadata',
            'rlnAccumMotionTotal', 'rlnAccumMotionEarly', 'rlnAccumMotionLate',
            'rlnCtfImage', 'rlnDefocusU', 'rlnDefocusV', 'rlnCtfAstigmatism', 'rlnDefocusAngle',
            'rlnCtfFigureOfMerit', 'rlnCtfMaxResolution', 'rlnCtfIceRingDensity',
            'rlnMicrographNameEven', 'rlnMicrographNameOdd'
        ]

        filesMap = {
            'rlnMicrographName': 'average',
            'rlnCtfPowerSpectrum': 'powerspectrum',
            'rlnCtfImage': 'powerspectrum',
            'rlnMicrographNameEven': 'average/even',
            'rlnMicrographNameOdd': 'average/odd'
        }
        
        filesMapExtra = {
            'rlnMicrographNameEven': 'average/even',
            'rlnMicrographNameOdd': 'average/odd'
        }

        if all(os.path.exists(self.join(self.FS, v)) for v in filesMapExtra.values()):
            filesMap.update(filesMapExtra)
            extra_cols.extend(filesMapExtra.keys())
            
        newTsTable = Table(tsTable.getColumnNames() + extra_cols)
        dims = None
        for frameRow in tsTable:
            moviePrefix = Path.removeBaseExt(frameRow.rlnMicrographMovieName)
            movieMrc = moviePrefix + '.mrc'
            frameDict = frameRow._asdict()
            for k, v in filesMap.items():
                movieFn = self.join(self.FS, v, movieMrc)
                frameDict[k] = movieFn if os.path.exists(movieFn) else ""
            frameDict['rlnMicrographMetadata'] = "None"

            avgMrcPath = frameDict['rlnMicrographName']
            if dims is None and os.path.exists(avgMrcPath):
                dims = Image.get_dimensions(avgMrcPath)

            movieXml = self.join(self.FS, moviePrefix + '.xml')
            defocusDict = defaultdict(lambda: 0)

            # xml and average mrc already validated for whole TS above
            ctf = WarpXml(movieXml).getDict('Movie', 'CTF', 'Param')

            defocusDict['rlnDefocusU'] = _float(float(ctf['Defocus']) * 10000)  # Convert to Angstroms
            defocusDict['rlnCtfAstigmatism'] = _float(float(ctf['DefocusDelta']) * 10000)  # Convert to Angstroms
            defocusDict['rlnDefocusV'] = _float(defocusDict['rlnDefocusU'] + defocusDict['rlnCtfAstigmatism'])
            defocusDict['rlnDefocusAngle'] = _float(ctf['DefocusAngle'])

            for k in extra_cols:
                if k.startswith('rlnAccumMotion'):
                    # FIXME: Parse the movie values
                    frameDict[k] = 0
                elif k.startswith('rlnDefocus') or k.startswith('rlnCtf') and k not in frameDict:
                    frameDict[k] = defocusDict[k]

            newTsTable.addRowValues(**frameDict)

        return True, newTsTable, dims

    def alignmentPs(self):
        """ Return alignment output pixel size from job args. """
        key = getattr(self, 'output_angpix', 'ts_aretomo.angpix')
        v = (self._args.get(key, '') or
             self._args.get('wat.ts_aretomo.angpix', '') or 0)
        return float(v)

    def alignmentFiles(self, tsName):
        """Return IMOD transform and tilt-angle files for alignment."""
        imodDir = self.join(
            self.TS, 'tiltstack', tsName, f'{tsName}_Imod'
        )
        return (
            os.path.join(imodDir, f'{tsName}_st.xf'),
            os.path.join(imodDir, f'{tsName}_st.tlt')
        )

    def alignedTS(self, tsName):
        return self.join(self.TS, 'tiltstack', tsName, f"{tsName}_aligned.mrc")
    
    def parseAlignmentParams(self, tsDict, ps):
        """ Parse AreTomo alignment parameters from .st.aln into Relion convention. """
        tsName = tsDict['rlnTomoName']     
        xfFile, tltFile = self.alignmentFiles(tsName)

        self.log(f"Parsing alignments for tomo: {tsName} from: {xfFile}, pixel size: {ps}")

        tlt_angles = Imod.get_angles_from_tlt(tltFile)
        xf_alignments = Imod.get_alignment_from_xf(xfFile)
        
        alignments = RelionStar.alignments_from_imod(tlt_angles, xf_alignments, ps)
        for aln in alignments:
            del aln['tilt']
            del aln['rlnCtfScalefactor']
            aln['rlnTomoYTilt'] *= -1 

        return alignments

    def updateAlignTsDict(self, tsDict, newPs=None):
        """ Update tsDict with alignment labels and write the enriched TS star.

        Args:
            tsDict: tilt-series row as a dict (updated in place)
            newPs: alignment pixel size; defaults to alignmentPs()

        Returns:
            (ok, dims) — ok is False when the aligned stack is missing;
            dims are aligned-stack dimensions when found.
        """
        if newPs is None:
            newPs = self.alignmentPs()

        tsName = tsDict['rlnTomoName']
        inputTsStar = tsDict['rlnTomoTiltSeriesStarFile']
        tsStarFile = self.join('tilt_series', tsName + '.star')
        tsAligned = self.alignedTS(tsName)

        ok = os.path.exists(tsAligned)
        dims = None
        if not ok:
            self.log(f"ERROR: Missing expected aligned TS: {tsAligned}")
            tsDict.update({
                'rlnTomoTiltSeriesStarFile': tsStarFile,
                'rlnTiltSeriesAligned': "None"
            })
            return False, None

        xfFile, tltFile = self.alignmentFiles(tsName)
        
        if not os.path.exists(xfFile) or not os.path.exists(tltFile):
            self.log(f"ERROR: Missing IMOD alignment files for TS {tsName}: "
                     f"{xfFile}, {tltFile}")
            tsDict.update({
                'rlnTomoTiltSeriesStarFile': tsStarFile,
                'rlnTiltSeriesAligned': tsAligned
            })
            return False, None

        dims = Image.get_dimensions(tsAligned)
        tsDict.update({
            'rlnTomoTiltSeriesStarFile': tsStarFile,
            'rlnTiltSeriesAligned': tsAligned
        })

        # Generate the proper metadata star file for this row
        tsTable = StarFile.getTableFromFile(tsName, inputTsStar)
        alignments = self.parseAlignmentParams(tsDict, newPs)
        if len(alignments) != len(tsTable):
            self.log(f"ERROR: Alignment count mismatch for TS {tsName}: "
                     f"{len(alignments)} alignments vs {len(tsTable)} tilts")
            return False, dims
        newTsTable = Table(tsTable.getColumnNames() + RelionStar.TOMO_ALIGNMENT_COLUMNS)
        # Alignments from AreTomo are sorted from negative to positive tilt angle
        # so we need to sort the TS metadata to match that order
        sortedRows = sorted(tsTable, key=lambda r: float(r.rlnTomoNominalStageTiltAngle))
        for aln, tiltRow in zip(alignments, sortedRows):
            tiltRowDict = tiltRow._asdict()
            tiltRowDict.update(aln)
            newTsTable.addRowValues(**tiltRowDict)

        self.write_ts_table(tsName, newTsTable, tsStarFile)
        return ok, dims

    def reconstructPs(self):
        """ Return reconstruction output pixel size from job args. """
        v = (self._args.get('ts_reconstruct.angpix', '') or
             self._args.get('ctfrec.ts_reconstruct.angpix', '') or 0)
        return float(v)

    def updateCtfRecTsDict(self, tsDict, newPs=None):
        """ Update tsDict with CTF/reconstruction Relion labels.

        Args:
            tsDict: tilt-series row as a dict (updated in place)
            newPs: reconstruction pixel size; defaults to reconstructPs()

        Returns:
            (ok, dims) — ok is False when the reconstructed tomogram is missing;
            dims are tomogram dimensions when found.
        """
        def _float(v):
            return round(float(v), 3)

        if newPs is None:
            newPs = self.reconstructPs()

        tsName = tsDict['rlnTomoName']
        recpath = self.join(self.TS, 'reconstruction')

        def _rec(*p):
            return os.path.join(recpath, *p)

        # FIXME: validate for missing tomograms
        tomoFile = ''
        for tfn in glob(_rec(f'{tsName}_*.mrc')):
            base = os.path.basename(tfn)
            suffix = '_' + base.split('_')[-1]
            if base.replace(suffix, '') == tsName:
                tomoFile = base
                break

        ok = bool(tomoFile)
        dims = None
        binning = None
        if ok:
            t, te, to = _rec(tomoFile), _rec('even', tomoFile), _rec('odd', tomoFile)
            dims = Image.get_dimensions(t)
            binning = _float(newPs / float(tsDict['rlnTomoTiltSeriesPixelSize']))
        else:
            t, te, to = '', '', ''

        xmlFile = self.join(self.TS, tsName + '.xml')
        if os.path.exists(xmlFile):
            ctf = WarpXml(xmlFile).getDict('TiltSeries', 'CTF', 'Param')
            defocus = _float(ctf['Defocus'])
        else:
            defocus = 999

        # FIXME: validate for missing tomostar files
        tomostar = self.join(self.TM, tsName + '.tomostar')
        tssFile = self.join(self.TSS)
        if not os.path.exists(tssFile):
            self.log(f"ERROR: Missing {self.TSS} needed for tomogram metadata")
            return False, None

        # For Relion tomogram.star, we need the original tomogram dimensions
        d = WarpXml(tssFile).getDict('Settings', 'Tomo', 'Param')
        # {'DimensionsX': '4400', 'DimensionsY': '6000', 'DimensionsZ': '1000'}

        if not ok:
            self.log(f"ERROR: Missing reconstructed tomogram for TS {tsName} in {recpath}")
            return False, None

        tsDict.update({
            'rlnTomoReconstructedTomogram': t,
            'rlnTomoTomogramBinning': binning,
            'rlnDefocus': defocus,
            'rlnTomoSizeX': d['DimensionsX'],
            'rlnTomoSizeY': d['DimensionsY'],
            'rlnTomoSizeZ': d['DimensionsZ'],
            'rlnTomoReconstructedTomogramHalf1': te,
            'rlnTomoReconstructedTomogramHalf2': to,
            'wrpTomostar': tomostar
        })
        return ok, dims

    def get_launcher_arg(self, argName, varName):
        return self._args.get(argName, None) or ProcessingPipeline.get_launcher(varName)

    def _get_launcher(self):
        return self.get_launcher_arg('launcher_warp', 'WARP')
    
    def get_subargs(self, key, extra_name=None, prefix='--'):
        subargs = self._args.subset(key, prefix, filters=['remove_false', 'remove_empty'])
        if extra_name:
            extra = Args.fromString(self._args.get(extra_name, ''))
            subargs.update(extra)
        return subargs


class WarpBasePopulationPipeline(WarpBasePipeline):
    """Base for Warp pipelines that take a single population path and produce
    an output population (e.g. MCore, EstimateWeights, MTools resample).
    """

    def _split_population(self, population):
        """Split the population path into input folder and relative population."""
        return population.split('/m/')

    def _setup_population_input(self, subargs, population_key='--population', alt_key=None):
        """Parse population from subargs, set self.population, import inputs.
        Returns the population file path (e.g. 'm/name.population').
        """
        pop_arg = subargs.pop(population_key, None)
        if alt_key is not None:
            pop_arg = pop_arg or subargs.pop(alt_key, None)
        if not pop_arg:
            raise ValueError("--population is required.")
        input_warp, self.population = self._split_population(pop_arg)
        population_file = os.path.join('m', self.population)
        self.log(f"Input Warp folder: {input_warp}, population: {self.population}")
        self._importInputs(input_warp, keys=['fs', 'fss', 'ts', 'tss', 'tm', 'm'])
        return population_file

    def _output(self, batch):
        """Register output population."""
        self.log("Registering output population.")
        population_file = self.join(self.M, self.population)
        population_name = self.population.replace('.population', '')
        if os.path.exists(population_file):
            self.outputs['Population'] = {
                'label': 'Population',
                'type': 'WarpPopulation',
                'info': f"Name: {population_name}",
                'files': [[population_file, 'WarpPopulation']]
            }
        else:
            self.log(f"Population file not found: {population_file}")
        self.updateBatchInfo(batch)

    def prerun(self):
        batch = Batch(id=self.name, path=self.path)
        self.runBatch(batch)
        self._output(batch)


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
        if ts_import_extra := self._args.get('extra_ts_import', None):
            subargs.update(Args.fromString(ts_import_extra))

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

    def _only_output(self):
        """ Mainly for debugging purposes. """
        return '--emwrap_output_only' in self._args.get('extra_ts_import', '')

    def _output(self, batch):
        """ Register output STAR files. """
        batch.mkdir('tilt_series')
        self.log("Registering output STAR files.")
        tsAllTable = StarFile.getTableFromFile('global', self.inputTs)
        newPs = self.alignmentPs()

        newTsStarFile = batch.join('aligned_tilt_series.star')
        failedStarFile = batch.join('failed_tilt_series.star')

        newTsAllTable = Table(tsAllTable.getColumnNames() + ['rlnTiltSeriesAligned'])
        failedTable = Table(newTsAllTable.getColumnNames())

        dims = 0, 0, 0
        for tsRow in tsAllTable:
            tsDict = tsRow._asdict()
            ok, tsDims = self.updateAlignTsDict(tsDict, newPs)
            if tsDims is not None and tsDims[2] > dims[2]:
                dims = tsDims
            table = newTsAllTable if ok else failedTable
            table.addRowValues(**tsDict)

        self.write_ts_table('global', newTsAllTable, newTsStarFile)

        N = len(newTsAllTable)
        # ps = newTsAllTable[0].rlnTomoTiltSeriesPixelSize

        x, y, n = dims
        outputNodes = [[newTsStarFile, 'TomogramGroupMetadata.star.emwrap.tsalign']]

        if len(failedTable) > 0:
            self.write_ts_table('global', failedTable, failedStarFile)
            outputNodes.append([failedStarFile, 'TomogramGroupMetadata.star.emwrap.tsalign-failed'])

        self.writeRelionOutputNodes(outputNodes)
        self.updateBatchInfo(batch)

    def prerun(self):
        self.prerunTs()
