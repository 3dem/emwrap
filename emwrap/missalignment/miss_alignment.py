# **************************************************************************
# *
# * Authors:     Daniel Marchan Torres (danielmarchan3@gmail.com)
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
import time
import shlex
import shutil
import subprocess
import csv
import itertools

from emtools.utils import Color, FolderManager, Timer
from emtools.jobs import BatchManager, Args
from emtools.metadata import StarFile, Table, StarMonitor, RelionStar

from emwrap.base import ProcessingPipeline
from emwrap.warp impport WarpBasePipeline


class MissAlignment(ProcessingPipeline):
    """ Wrapper specific to MissAlignment algorithm.
    """
    
    name = 'emw-missalignment'

    def __init__(self, args, output):
        ProcessingPipeline.__init__(self, args, output) # self._args is defined here
        gpus = self._args.get('gpus', '')
        self.gpuList = self.get_gpu_list(str(gpus).strip()) if gpus else []
    
        self.outputTsDir = 'tilt_series'
        self.trainingDir = 'training'
        self.imodAlignmentsDir = 'imod_alignments'

        self.pixelSize = None

        self.inputLen = 0
        self.inputTs = None
        self.inputTsTable = None      # set in prerun via _getInputTsTable
        self.n_training = 0            # set in prerun

        self.trainingBestModel = None  # best epoch*.pth found after training
        self.modelPath = None          # model actually used for inference

        self._allResults = {}  # tsName -> result dict, accumulated by _output
        self.registerOnly = self._register_output_only() # DEBUG flag
        self.launcher_missalignment = self._args.get('launcher_missalignment', None)

        self.acq = self.loadAcquisition(self._args.get('input_tiltseries', None))

    
    # ------------------------------------------------------------------
    # Configuration / column definitions
    # ------------------------------------------------------------------
    def _tomogram_extra_cols(self):
        return [
            # Tomo specific columns
            'rlnTomoReconstructedTomogram',
            'rlnTomoReconstructedTomogramHalf1',
            'rlnTomoReconstructedTomogramHalf2',
        ]
        

    # ------------------------------------------------------------------
    # GUI form-argument helpers
    # ------------------------------------------------------------------
    def _get_launcher(self):
        return self.launcher_missalignment or ProcessingPipeline.get_launcher('MISSALIGNMENT')

    def train_form_args(self):
        """ All GUI parameters under the 'train' tab (train.n_training,
        train.dn3.*), with the 'train.' prefix stripped. """
        subargs = self._args.subset('train', new_prefix="")        
        return subargs

    def inference_form_args(self):
        """ All GUI parameters under the 'infer' tab (infer.model,
        infer.dn3.*), with the 'infer.' prefix stripped. """
        subargs = self._args.subset('infer', new_prefix="")        
        return subargs
    

    # ------------------------------------------------------------------
    # Command-building helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _strip_empty_values(argsDict):
        """ Drop parameters left empty/None/False in the GUI so they are
        simply omitted from the command line, letting denoise3d/predict3d
        fall back to their own internal defaults. """
        cleaned = {}
        for key, value in argsDict.items():
            if value is None:
                continue
            if isinstance(value, str) and value.strip() == '':
                continue
            if isinstance(value, bool) and not value:
                continue
            cleaned[key] = value
        return cleaned
 
    def _build_training_args(self, inputDir, outputDir, metricsFile=None):
        """ Build the full denoise3d argument dict: GUI params directly
        usable on the command line (train.dn3.*) plus the internally
        managed ones (--input, --output, --train_only, --min_selected,
        --max_selected). If metricsFile it is used to point denoise3d
        at the frozen copy of the metrics file inside the training folder,
        instead of the original path which may still be growing under
        AreTomo3's live streaming). """
        cmdArgs = self._strip_empty_values(
            self.train_form_args().subset('dn3', new_prefix="--"))

        if metricsFile:
            cmdArgs['--metrics_file'] = metricsFile

        # Quality-metric thresholds are meaningless (and should not be
        # passed) if no metrics file was supplied.
        if not cmdArgs.get('--metrics_file'):
            cmdArgs.pop('--metrics_file', None)
            for key in self.QUALITY_METRIC_KEYS:
                cmdArgs.pop(key, None)
 
        cmdArgs['--input'] = inputDir
        cmdArgs['--output'] = outputDir
        cmdArgs['--train_only'] = "" # This flag does not expect anything
        # train.n_training (single GUI field) drives both selection bounds:
        # we hand denoise3d exactly the tomograms we want it to use, so
        # min == max == n_training.
        cmdArgs['--min_selected'] = self.n_training
        cmdArgs['--max_selected'] = self.n_training
 
        return cmdArgs
 
    def _build_inference_args(self, inputDir, outputDir, modelPath):
        """ Build the full predict3d argument dict: GUI params directly
        usable on the command line (infer.dn3.*) plus the internally
        managed ones (--input, --output, --model). """
        cmdArgs = self._strip_empty_values(
            self.inference_form_args().subset('dn3', new_prefix="--"))
 
        cmdArgs['--input'] = inputDir
        cmdArgs['--output'] = outputDir
        cmdArgs['--model'] = modelPath
 
        return cmdArgs
    

    # ------------------------------------------------------------------
    # Input/output folder helpers
    # ------------------------------------------------------------------
    # def _getInputTomTable(self):
    #     """ Read input star file and return the 'global' table. """
    #     inputStar = self._args['input_tomograms']
    #     if os.path.exists(inputStar):
    #         with StarFile(inputStar) as sf:
    #             t = sf.getTable('global')
    #             self.inputLen = len(t)  # Let's update the inputLen property
    #             return t
    #     return None

    def _getInputTsTable(self):
        """ Read input star file and return the 'global' table. """
        inputStar = self._args['input_tiltseries']
        if os.path.exists(inputStar):
            with StarFile(inputStar) as sf:
                t = sf.getTable('global')
                self.inputLen = len(t)  # Let's update the inputLen property
                return t
        return None

    def _getOutputTomFolder(self, tsName):
        return FolderManager(self.join(self.outputTomDir, tsName))

    def write_tomo_table(self, tableName, table, starFile):
        self.log(f"Writing: {starFile}")
        with StarFile(starFile, 'w') as sfOut:
            sfOut.writeTable(tableName, table, computeFormat='left', timeStamp=True)
    
    def _filename(self, row):
        """ Helper to get unique name from a tomogram row """
        return row.rlnTomoName

    
    # ------------------------------------------------------------------
    # Relion metadata conversion helpers
    # ------------------------------------------------------------------    
    def _build_tomogram_row(self, tsRow, result):
        """ Build the output tomograms.star row for a tilt series.
 
        NOTE: the denoised tomogram replaces 'rlnTomoReconstructedTomogram'
        so that downstream jobs default to the denoised version. The raw
        (non-denoised) AreTomo3 tomogram remains available from the
        original job's output; if a workflow needs both the raw and the
        denoised path in the same tomograms.star, add a distinct column
        here (e.g. 'rlnTomoReconstructedTomogramDenoised') instead of
        overwriting.
        """
        tomDict = tsRow._asdict()
        tomDict.update({
            'rlnTomoReconstructedTomogram': result.get('rlnTomoReconstructedTomogram', ''),
        })
        return tomDict

    # ------------------------------------------------------------------
    # Output registration
    # ------------------------------------------------------------------
    def _read_csv_rows(self, csvPath):
        with open(csvPath, newline='') as f:
            reader = csv.DictReader(f)
            return reader.fieldnames, list(reader)
    
    def _collect_existing_final_result(self, tomoRow):
        tomoName = tomoRow.rlnTomoName
        result = {'rlnTomoName': tomoName}

        sourcePath = getattr(tomoRow, 'rlnTomoReconstructedTomogram', None)
        if not sourcePath:
            result['error'] = f"Missing source tomogram path for {tomoName}"
            return result

        denoisedPath = self._getOutputTomFolder("").join(os.path.basename(sourcePath))
        if os.path.exists(denoisedPath):
            result['rlnTomoReconstructedTomogram'] = denoisedPath
        else:
            result['error'] = f"Missing denoised tomogram for {tomoName}"

        return result
    
    def _register_existing_final_outputs(self):
        self.inputTomTable = self._getInputTomTable()

        self.log('DEBUG register-only mode: rebuilding outputs from final job folders.',
            flush=True)

        self._allResults = {}

        for row in self.inputTomTable:
            tomoName = row.rlnTomoName
            result = self._collect_existing_final_result(row)
            self._allResults[tomoName] = result

            if 'error' in result:
                self.log(f"DEBUG register-only: {tomoName}: {result['error']}")
            else:
                self.log(f"DEBUG register-only: found final outputs for {tomoName}")

        self._registerOutputs()

        self.info['register_only'] = True
        self.info['denoiset_output'] = len(
            [r for r in self._allResults.values() if 'error' not in r])

    def _registerOutputs(self):
        """Rebuild Relion-style DenoisET outputs.
        Outputs:
            tomograms.star
                Global table containing denoised tomograms, only if
                denoising was produced.

            failed_tomograms.star
                Global table containing tomograms that failed.
        """
        self.log("Registering output STAR files.")

        failedStarFile = self.join('failed_tomograms.star')
        tomogramsStarFile = self.join('tomograms.star')

        inputCols = self.inputTomTable.getColumnNames()
        tomExtraCols = [c for c in self._tomogram_extra_cols() if c not in inputCols]

        failedTable = Table(inputCols)
        tomogramsTable = Table(inputCols + tomExtraCols)

        inputByName = {row.rlnTomoName: row for row in self.inputTomTable}

        for tomoName, result in self._allResults.items():
            tomoRow = inputByName.get(tomoName, None)
            if tomoRow is None:
                self.log(f"WARNING: Result for unknown tomogram {tomoName}, skipping.")
                continue

            if 'error' in result:
                failedTable.addRowValues(**tomoRow._asdict())
                continue

            tomogram = result.get('rlnTomoReconstructedTomogram', None)
            if tomogram and os.path.exists(tomogram):   
                finalTomoRow = self._build_tomogram_row(tomoRow, result)
                tomogramsTable.addRowValues(**finalTomoRow)

        outputNodes = []

        if len(failedTable) > 0:
            self.write_tomo_table('global', failedTable, failedStarFile)
            outputNodes.append(
                [failedStarFile, 'TomogramGroupMetadata.star.relion.tomo.tomograms-failed'])

        self.write_tomo_table('global', tomogramsTable, tomogramsStarFile)
        outputNodes.append([tomogramsStarFile, 'TomogramGroupMetadata.star.relion.tomo.tomograms'])

        self.writeRelionOutputNodes(outputNodes)    
        
    
    # ------------------------------------------------------------------
    # Training
    # ------------------------------------------------------------------
    def launch_training(self, tomTable):
        """ Symlink the training subset into self.trainingDir and run
        denoise3d in --train_only mode. Populates self.trainingBestModel
        once training has finished. """
        self.log(f"Training set size: {len(tomTable)} tomograms")
 
        trainingInputDir = self.join(self.trainingDir)
        trainingOutputDir = self.join(self.trainingDir, 'output')  

        os.makedirs(trainingInputDir, exist_ok=True)
        os.makedirs(trainingOutputDir, exist_ok=True)

        # Keep a copy of the metrics file used for this training run inside
        # the training folder, so the entries denoise3d accepted/discarded
        # via its own quality-based selection can be inspected later.
        metricsFile = self.train_form_args().subset('dn3', new_prefix="").get('metrics_file', '')
        trainingMetricsFile = None
        if metricsFile and os.path.exists(metricsFile):
            trainingMetricsFile = self.join(self.trainingDir, os.path.basename(metricsFile))
            shutil.copy2(metricsFile, trainingMetricsFile)
 
        volumeCols = ['rlnTomoReconstructedTomogram',
                      'rlnTomoReconstructedTomogramHalf1',
                      'rlnTomoReconstructedTomogramHalf2']
 
        # --input: symlink EVN/ODD/full volumes for the training subset
        for row in tomTable:
            tomDict = row._asdict()
            for col in volumeCols:
                srcPath = os.path.abspath(tomDict[col])
                baseName = os.path.basename(srcPath)
                destPath = self.join(self.trainingDir, baseName)
                if not os.path.lexists(destPath):
                    os.symlink(srcPath, destPath)
 
        cmdArgs = self._build_training_args(
            os.path.abspath(trainingInputDir),
            os.path.abspath(trainingOutputDir),
            metricsFile=os.path.abspath(trainingMetricsFile) if trainingMetricsFile else None)
        launcher = self._get_launcher()
        # denoise3d is not part of the launcher itself, so it must be
        # prepended to the argument list before calling it.
        argv = ['denoise3d'] + Args(cmdArgs).toList()
 
        self.log(f"DenoisET denoise3d argv: {launcher} {' '.join(argv)}")
        self.call(launcher, argv)
 
        self.trainingBestModel = self._get_best_training_model(os.path.abspath(trainingOutputDir))
 
        with open(self.join(self.trainingDir, 'training_done.txt'), 'w') as f:
            f.write(f"best_model={self.trainingBestModel}\n")
 
        self.info['training'] = {
            'n_training': len(tomTable),
            'training_stats_csv': os.path.join(trainingOutputDir, 'training_stats.csv'),
            'best_model': self.trainingBestModel,
        }
        self.log(f"Training finished. Best model: {self.trainingBestModel}")
 

    # ------------------------------------------------------------------
    # Batch execution (inference)
    # ------------------------------------------------------------------
    def get_denoiset_proc(self, gpu):
        def _denoiset(batch):
            rows = batch['items']
            batch.create()
            tomoName = self._filename(rows[0])
            batch.log(f"----- Starting new batch: {tomoName} -----")
 
            # --input: symlink the full tomogram(s) for this batch
            baseName = None
            for row in rows:
                srcPath = os.path.abspath(row.rlnTomoReconstructedTomogram)
                baseName = os.path.basename(srcPath)
                destPath = batch.join(baseName)
                if not os.path.lexists(destPath):
                    os.symlink(srcPath, destPath)
 
            # --output: predict3d writes into an 'output' subfolder
            outputDir = 'output'
            os.makedirs(outputDir, exist_ok=True)
 
            cmdArgs = self._build_inference_args(".", outputDir, self.modelPath)
 
            launcher = self._get_launcher()
            # predict3d is not part of the launcher itself, so it must be
            # prepended to the argument list before calling it.
            argv = ['predict3d'] + Args(cmdArgs).toList()
 
            t = Timer()
            batch.log(f"DenoisET predict3d argv: {launcher} {' '.join(argv)}")
            batch.call(launcher, argv)
 
            batch.info.update({
                'denoiset_input': len(rows),
                'denoiset_elapsed': str(t.getElapsedTime()),
            })
 
            outTomogramMrc = batch.join(outputDir, baseName)
            self.__expect(outTomogramMrc)
 
            result = {
                'rlnTomoName': tomoName,
                'rlnTomoReconstructedTomogram': outTomogramMrc,
            }
 
            batch['results'] = [result]
            batch['outputs'] = [outTomogramMrc]
            batch.info.update({'denoiset_output': 1})
 
            return batch
 
        return _denoiset

    def _output(self, batch):
        """ Register output STAR files. Runs per-batch (streaming): each
        call rewrites the aggregate tables to include this batch's result,
        so outputs are available incrementally as each tilt series
        finishes, without waiting for the whole input to be processed. """
        tomoName = self._filename(batch['items'][0])
        batch.log(f"Storing output for batch '{tomoName}'", flush=True)
 
        if batch.error:
            batch.log(f"ERROR: {batch.error}")
            self._allResults[tomoName] = {'error': batch.error}
        else:
            result = batch['results'][0] if batch['results'] else {}
 
            def _copy(srcKey, destFolder):
                src = result.get(srcKey, None)
                if src is None or not os.path.exists(src):
                    return None
                dst = destFolder.join(os.path.basename(src))
                if os.path.abspath(src) != os.path.abspath(dst):
                    shutil.copy2(src, dst)
                result[srcKey] = dst
                return dst
 
            # All denoised tomograms are written into a single shared
            # folder (self.outputTomDir), not one subfolder per tilt series.
            tomFolder = self._getOutputTomFolder("")
            if not os.path.exists(tomFolder.join("")):
                tomFolder.create() 

            _copy('rlnTomoReconstructedTomogram', tomFolder)

            batch.info['result'] = {k: v for k, v in result.items() if k != 'error'}
            self._allResults[tomoName] = result
 
        batch.info['tomoName'] = tomoName
 
        # Rebuild and re-register the aggregate outputs now, so they
        # reflect everything completed so far.
        self._registerOutputs()
        self.updateBatchInfo(batch)
 
        if self.inputLen:
            totalOutput = len(self.info['batches'])
            percent = totalOutput * 100 / self.inputLen
            batch.log(f">>> Processed {Color.green(totalOutput)} out of "
                      f"{Color.red(self.inputLen)} "
                      f"({Color.bold('%0.2f' % percent)} %)", flush=True)
 
        return batch
   
    # ------------------------------------------------------------------
    # Command execution
    # ------------------------------------------------------------------
    def call(self, program, kwargs, logfile=None, verbose=False, cwd=True):
        """ Run `program` with `kwargs` as arguments.
        If cwd is True, call the program from the pipeline's working
        directory. `kwargs` may be a dict (converted via Args), a list
        (used as-is), or a string (shlex-split). """
        if isinstance(kwargs, dict):
            args = Args(kwargs).toList()
        elif isinstance(kwargs, list):
            args = list(kwargs)
        elif isinstance(kwargs, str):
            args = shlex.split(kwargs)
        else:
            raise Exception("Expecting dict, list or str as arguments")
 
        args.insert(0, program)
        logfile = logfile or self.join('batch.log')
        cmdStr = f"{args[0]} {' '.join(args[1:])}"
 
        with open(logfile, 'a') as f:
            self.log(f"{Color.green(args[0])} {Color.bold(' '.join(args[1:]))}")
            f.write(f"\n{cmdStr}\n")
            f.flush()
            popenKwargs = {'stderr': f, 'stdout': f}
            if cwd:
                popenKwargs['cwd'] = self.path
            rc = subprocess.call(args, **popenKwargs)
            if rc != 0:
                raise subprocess.CalledProcessError(rc, args)
    
    def __expect(self, fileName):
        if not os.path.exists(fileName):
            raise Exception(f"Missing expected output: {fileName}")

    # ------------------------------------------------------------------
    # Pipeline lifecycle
    # ------------------------------------------------------------------
    def _wait_for_training_set(self):
        """ Wait until enough tomograms are available for training, and
        return the actual list of rows to train on. """
        metricsFile = self.train_form_args().subset('dn3', new_prefix="").get('metrics_file', '')

        while True:
            if self.inputLen < self.n_training:
                self.log(f"Waiting for enough tomograms "
                          f"({self.inputLen}/{self.n_training})")
            elif metricsFile:
                # A metrics file was supplied: the training set must also
                # comply with the configured quality thresholds, not just
                # meet the raw tomogram count.
                qualifyingRows = self._get_qualifying_tomograms(metricsFile)
                self.log(f"Quality metrics: {len(qualifyingRows)}/{self.inputLen} "
                          f"tomograms currently pass the configured "
                          f"thresholds (need {self.n_training}).")
                if len(qualifyingRows) >= self.n_training:
                    return qualifyingRows[:self.n_training]
            else:
                return list(itertools.islice(self.inputTomTable, self.n_training))

            time.sleep(30)
            self.inputTomTable = self._getInputTomTable()

    def _initialize_warp_paths(self):
        """Initialize the Warp project paths used by MissAlignment."""
        self.warpTomostarDir = 'warp_tomostar'
        self.warpTiltSeriesDir = 'warp_tiltseries'
        self.warpSettings = os.path.join(
            self.warpTiltSeriesDir,
            'warp_tiltseries.settings',
        )

        self.frameSeries = 'warp_frameseries'
        self.frameSeriesSettings = f'{self.frameSeries}.settings'
        self.mdocsDir = 'mdocs'

        self.mkdir(self.warpTomostarDir)
        self.mkdir(self.warpTiltSeriesDir)
        self.mkdir(self.frameSeries)
        self.mkdir(self.mdocsDir)

    
    def _build_ts_import_argv(self):
        """Build WarpTools ts_import arguments."""

        frameseries_settings = os.path.abspath(
            self.frameSeries
        )
        mdocs_dir = os.path.abspath(
            self.mdocsDir
        )
        output_dir = os.path.abspath(
            self.warpTomostarDir
        )

        argv = [
            'WarpTools',
            'ts_import',
            '--frameseries',
            frameseries_settings,
            '--tilt_exposure',
            str(self.acq['total_dose']),
            '--output',
            output_dir,
            '--mdocs',
            mdocs_dir,
        ]

        # Recommended by the MissAlignment guide. Warp uses image intensity
        # to determine the tilt offset.
        argv.append('--auto_zero')

        return argv
    
    def _importInputs(self, inputRunFolder, keys=None):
        """ Inspect the input run folder and copy or link input folder/files
        if necessary. If gain is present in the acquisition, it will be linked.

        Args:
            inputRunFolder: the input run folder
            keys: input keys to import, if None, all inputs will be imported
        """
        print(f"{self.name}: Import inputs ", self.gain)
        if keys is None:
            keys = [k for k in self.INPUTS if k != self.M]  # all keys except m

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
    
    def _run_ts_import(self):
    """Generate Warp .tomostar files from frame-series and MDOC data."""
        self._importInputs(inputFolder, keys=['fs', 'fss', 'frames', 'mdocs'])
        
        argv = self._build_ts_import_argv()    
        launcher = self._get_launcher()

        self.log(
            "WarpTools ts_import command: "
            f"{launcher} {shlex.join(argv)}"
        )

        self.call(
            launcher,
            argv,
            logfile=self.join('warp_ts_import.log'),
        )


    def _build_create_settings_argv(self):
        """Build WarpTools create_settings arguments."""

        return [
            'WarpTools',
            'create_settings',
            '--folder_data',
            os.path.abspath(self.warpTomostarDir),
            '--extension',
            '*.tomostar',
            '--folder_processing',
            os.path.abspath(self.warpTiltSeriesDir),
            '--output',
            os.path.abspath(self.warpTiltSeriesSettings),
            '--angpix',
            f'{self.pixelSize:.6f}',
            '--exposure',
            str(self.acq['total_dose']),
        ]

    def _create_warp_settings(self):
        """Create warp_tiltseries.settings."""
        argv = self._build_create_settings_argv()
        launcher = self._get_launcher()

        self.log(
            "WarpTools create_settings command: "
            f"{launcher} {shlex.join(argv)}"
        )

        self.call(
            launcher,
            argv,
            logfile=self.join('warp_create_settings.log'),
        )

        if not os.path.isfile(self.warpTiltSeriesSettings):
            raise RuntimeError(
                "WarpTools create_settings did not create the expected "
                f"settings file: {self.warpTiltSeriesSettings}"
            )

    def _build_ts_import_alignments_argv(self):
        """Build WarpTools ts_import_alignments arguments."""

        settings_path = os.path.abspath(
            self.warpTiltSeriesSettings
        )

        alignments_path = os.path.abspath(
            self.imodAlignmentsDir
        )

        return [
            'WarpTools',
            'ts_import_alignments',
            '--settings',
            settings_path,
            '--alignments',
            alignments_path,
            '--alignment_angpix',
            f'{self.pixelSize:.3f}',
        ]
    
    def _import_initial_alignments(self):
        """Import the regenerated IMOD alignments into the Warp project."""
        argv = self._build_ts_import_alignments_argv()
        launcher = self._get_launcher()

        self.log(
            "WarpTools ts_import_alignments command: "
            f"{launcher} {shlex.join(argv)}"
        )

        self.call(
            launcher,
            argv,
            logfile=self.join('warp_import_alignments.log'),
        )
    
    def _regenerate_imod_files_for_tiltseries(self, ts_name, ts_star_path, pixel_size, output_root):
        """Generate IMOD .xf and .tlt files from a RELION 5 tilt-series STAR.
        Parameters
        ----------
        ts_name : str
            Tilt-series name from rlnTomoName.
        ts_star_path : str
            Path from rlnTomoTiltSeriesStarFile.
        pixel_size : float
            Pixel size, in Angstrom/pixel, used for the alignment shifts.
        output_root : str
            Root alignment directory passed later to
            ``WarpTools ts_import_alignments --alignments``.
        """
        ts_star_path = os.path.abspath(ts_star_path)

        imod_dir = os.path.join(output_root, f'{ts_name}_Imod')
        os.makedirs(imod_dir, exist_ok=True)

        xf_path = os.path.join(imod_dir, f'{ts_name}_st.xf')
        tlt_path = os.path.join(imod_dir, f'{ts_name}_st.tlt')

        with StarFile(ts_star_path) as star_file:
            table_names = star_file.getTableNames()
            tilt_table = star_file.getTable(ts_name)

        if not len(tilt_table):
            raise ValueError(f"Tilt-series STAR table is empty: {ts_star_path}")

        xf_rows = []
        tilt_angles = []

        for index, tilt_row in enumerate(tilt_table):
            xf_row = RelionStar.alignment_to_xf(tilt_row, pixel_size)
            tilt_angle = float(tilt_row.rlnTomoYTilt)
            xf_rows.append(xf_row)
            tilt_angles.append(tilt_angle)

        with open(xf_path, 'w', encoding='utf-8') as xf_file:
            for a11, a12, a21, a22, dx, dy in xf_rows:
                xf_file.write(
                    f'{a11: .3f} '
                    f'{a12: .3f} '
                    f'{a21: .3f} '
                    f'{a22: .3f} '
                    f'{dx: .2f} '
                    f'{dy: .2f}\n'
                )

        with open(tlt_path, 'w', encoding='utf-8') as tlt_file:
            for tilt_angle in tilt_angles:
                tlt_file.write(f'{tilt_angle:.6f}\n')

        self.log(
            f"Generated IMOD alignment for {ts_name}: "
            f"{xf_path}, {tlt_path}"
        )

        return xf_path, tlt_path
    
    def _regenerate_imod_files(self):
        """Generate IMOD .xf and .tlt files for every input tilt series.
        The resulting root directory can be passed directly to:
            WarpTools ts_import_alignments --alignments <directory>
        """
        
        imodAlignmentsDir = self.join(self.imodAlignmentsDir)
        os.makedirs(imodAlignmentsDir, exist_ok=True)

        self.log(
            "Regenerating IMOD .xf and .tlt files for all tilt series."
        )

        for row in self.inputTsTable:
            ts_name = row.rlnTomoName
            ts_path = row.rlnTomoTiltSeriesStarFile

            # This must be the same pixel size used when the IMOD shifts were
            # converted to RELION Angstrom shifts.
            pixel_size = float(row.rlnTomoTiltSeriesPixelSize)

            if self.pixelSize is None:
                self.pixelSize = pixel_size

            self.log(
                f"Regenerating IMOD alignment files for "
                f"{ts_name} ({ts_path})"
            )

            self._regenerate_imod_files_for_tiltseries(
                ts_name=ts_name,
                ts_star_path=ts_path,
                pixel_size=pixel_size,
                output_root=imodAlignmentsDir,
            )
    
    
    def prerun(self):
        self.inputTsTable = self._getInputTsTable()
        self.inputTs =  self._args['input_tiltseries']
        print(f"Input tilt-series: {len(self.inputTsTable)}")  

        # if self.registerOnly:
        #     self._register_existing_final_outputs()
        #     return
        
       # Streaming is not supported yet. Start processing after all tilt
        # series are available.

        self._initialize_warp_paths()

        # Import or link the frame-series Warp project, frames and MDOCs.
        self._prepare_warp_inputs()

        # Generate one .tomostar file per tilt series.
        self._run_ts_import()
        self._validate_tomostar_files()

        # Generate warp_tiltseries.settings.
        self._create_warp_settings()

        # Generate the .xf and .tlt alignment files from RELION metadata.
        self._regenerate_imod_files()

        # Import the external alignment into the new Warp tilt-series project.
        self._import_initial_alignments()
        
        
        # self.mkdir(self.outputTsDir)
        # self.mkdir(self.outputTomDir)
        
        # batchMgr = TsStarBatchManager(self.inputTsTable, self.tmpDir)
        # g = self.addGenerator(batchMgr.generate)
        
        # self.addGpuProcessors(g, self.get_aretomo3_proc, self._output)


if __name__ == '__main__':
    MissAlignment.main()