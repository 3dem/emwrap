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
import shutil
import csv
import itertools

from emtools.utils import Color, FolderManager, Timer
from emtools.jobs import BatchManager, Args, Batch
from emtools.metadata import StarFile, Table, StarMonitor

from emwrap.base import ProcessingPipeline


class DenoisET(ProcessingPipeline):
    """ Wrapper specific to DenoisET Noise2Noise algorithm.
 
    Two GUI-driven CLI commands are built and launched by this wrapper:
      - denoise3d (training),  through launch_training()
      - predict3d (inference), through get_denoiset_proc()
 
    GUI parameter naming convention (see emw-denoiset.json):
      - train.<name> / infer.<name>      -> needs conversion logic before
                                             it can be used on the command
                                             line (handled explicitly below)
      - train.dn3.<name> / infer.dn3.<name> -> maps 1:1 to a denoise3d /
                                             predict3d CLI flag
    """
    
    name = 'emw-denoiset'

    # Quality-metric flags that only make sense when a metrics file is
    # supplied; stripped from the command otherwise.
    QUALITY_METRIC_KEYS = (
        '--tilt_axis', '--thickness', '--global_shift',
        '--bad_patch_low', '--bad_patch_all', '--ctf_res', '--ctf_score'
    )
 
    # Quality-metric column candidates in TiltSeries_Metrics.csv, and the
    # comparison mode against the configured GUI threshold ('max' = value
    # must be <= threshold, 'min' = value must be >= threshold). These four
    # can be compared directly against the CSV value.
    QUALITY_METRIC_COLUMNS = {
        'bad_patch_low': (['Bad_Patch_Low'], 'max'),
        'bad_patch_all': (['Bad_Patch_All'], 'max'),
        'ctf_res':       (['CTF_Res(A)'], 'max'),
        'ctf_score':     (['CTF_Score'], 'min'),
    }

    # Tilt_Series identifies the tilt series in the metrics CSV, but with
    # a trailing '.mrc' (e.g. 'TS_23.mrc'), unlike rlnTomoName in the STAR
    # table ('TS_23') -- matching needs the extension stripped.
    TIlT_SERIES_COLUMN = 'Tilt_Series'

    # tilt_axis: the CSV reports the raw tilt-axis angle, not a deviation.
    # --tilt_axis is "maximum deviation from the median tilt axis", so the
    # median across the metrics file must be computed first.
    TILT_AXIS_COLUMN = 'Tilt_Axis'

    # thickness / global_shift: reported in pixels in the CSV, but the GUI
    # thresholds are in Angstrom, so they need converting using the
    # tilt series actual pixel size -- see _get_ts_pixel_size.
    THICKNESS_PIX_COLUMN = 'Thickness(Pix)'
    GLOBAL_SHIFT_PIX_COLUMN = 'Global_Shift(Pix)'

    def __init__(self, args, output):
        ProcessingPipeline.__init__(self, args, output) # self._args is defined here
        gpus = self._args.get('gpus', '')
        self.gpuList = self.get_gpu_list(str(gpus).strip()) if gpus else []
    
        self.outputTomDir = 'tomograms'
        self.trainingDir = 'training'

        self.inputLen = 0
        self.inputToms = None
        self.inputTomTable = None      # set in prerun via _getInputTomTable
        self.n_training = 0            # set in prerun

        self.trainingBestModel = None  # best epoch*.pth found after training
        self.modelPath = None          # model actually used for inference

        self._allResults = {}  # tsName -> result dict, accumulated by _output
        self.registerOnly = self._register_output_only() # DEBUG flag
        self.launcher_denoiset = self._args.get('launcher_denoiset', None)

    
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
    # Command-building helpers
    # ------------------------------------------------------------------
    def _get_launcher(self):
        return self.launcher_denoiset or ProcessingPipeline.get_launcher('DENOISET')

    def _build_training_args(self, inputDir, outputDir, metricsFile=None):
        """ Build the full denoise3d argument dict: GUI params directly
        usable on the command line (train.dn3.*) plus the internally
        managed ones (--input, --output, --train_only, --min_selected,
        --max_selected). If metricsFile it is used to point denoise3d
        at the frozen copy of the metrics file inside the training folder,
        instead of the original path which may still be growing under
        AreTomo3's live streaming). """
        cmdArgs = self._args.subset(
            'train.dn3', new_prefix="--", filters=['remove_empty', 'remove_false'])

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
        cmdArgs = self._args.subset(
            'infer.dn3', new_prefix="--", filters=['remove_empty', 'remove_false'])
 
        cmdArgs['--input'] = inputDir
        cmdArgs['--output'] = outputDir
        cmdArgs['--model'] = modelPath
 
        return cmdArgs
    

    # ------------------------------------------------------------------
    # Input/output folder helpers
    # ------------------------------------------------------------------
    def _getInputTomTable(self):
        """ Read input star file and return the 'global' table. """
        inputStar = self._args['input_tomograms']
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
    def _get_best_training_model(self, trainingOutputDir):
        """ Parse training_stats.csv and return the path to the model from
        the epoch immediately BEFORE ch_mean (the checkerboard-artifact
        metric) first reaches/exceeds --ch_threshold. Training itself
        stops once that threshold is crossed specifically to avoid
        checkerboard/overdenoising artifacts, so the last epoch below
        threshold -- not the epoch with the single lowest ch_mean -- is
        the safe model to use. The epoch number is read from the first
        column of the CSV. """
        statsCsv = os.path.join(trainingOutputDir, 'training_stats.csv')
        self.__expect(statsCsv)
 
        fieldnames, rows = self._read_csv_rows(statsCsv)
        epochCol = fieldnames[0]
        rows = sorted(rows, key=lambda r: int(r[epochCol]))
 
        chThreshold = float(self._args.get('train.dn3.ch_threshold', 0.034))
 
        bestRow = rows[0]
        for i, row in enumerate(rows):
            if float(row['ch_mean']) >= chThreshold:
                # Use the epoch right before the threshold was crossed; if
                # the very first epoch already crosses it, fall back to
                # that first epoch since there is no earlier one to use.
                bestRow = rows[i - 1] if i > 0 else row
                break
            bestRow = row  # keep advancing in case threshold is never hit
 
        epoch = bestRow[epochCol]
        modelPath = os.path.join(trainingOutputDir, f'epoch{epoch}.pth')
        self.__expect(modelPath)
        return modelPath  

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
        metricsFile = self._args.get('train.dn3.metrics_file', '')
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

        trainingBatch = Batch(id='training', path=self.path)
        trainingBatch.log(f"DenoisET denoise3d argv: {launcher} {' '.join(argv)}")
        trainingBatch.call(launcher, argv)
 
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

    def __expect(self, fileName):
        if not os.path.exists(fileName):
            raise Exception(f"Missing expected output: {fileName}")

    # ------------------------------------------------------------------
    # Pipeline lifecycle
    # ------------------------------------------------------------------
    def _wait_for_input_table(self):
        table = self._getInputTomTable()
        while table is None:
            self.log('No input found yet, sleeping 30s')
            time.sleep(30)
            table = self._getInputTomTable()
        return table
 
    @staticmethod
    def _find_csv_column(fieldnames, candidates):
        lowerFields = {f.lower(): f for f in fieldnames}
        for cand in candidates:
            if cand in fieldnames:
                return cand
            if cand.lower() in lowerFields:
                return lowerFields[cand.lower()]
        return None

    @staticmethod
    def _strip_metrics_name(name):
        """ TiltSeries_Metrics.csv identifies tilt series with a trailing
        '.mrc' (e.g. 'TS_23.mrc'); STAR tables use the bare name
        ('TS_23'). Strip the extension so the two can be matched. """
        return os.path.splitext(name)[0]

    @staticmethod
    def _compute_median(values):
        values = sorted(values)
        n = len(values)
        if n == 0:
            return None
        mid = n // 2
        return values[mid] if n % 2 else (values[mid - 1] + values[mid]) / 2

    def _get_ts_pixel_size(self):
        """ Effective tilt series pixel size in Angstrom: the tilt series
        pixel size is read from the input tomograms.star. It should be 
        the same as the per-tomogram pixel size reported in TiltSeries_Metrics.csv, 
        since that CSV and thresholds does not account for tomogram binning.
        Assumes a uniform pixel size/binning across the dataset (read from the first row). """
        if not self.inputTomTable or len(self.inputTomTable) == 0:
            return None
        row = next(iter(self.inputTomTable))
        try:
            tsPixelSize = float(row.rlnTomoTiltSeriesPixelSize)
        except (AttributeError, TypeError, ValueError):
            return None
        return tsPixelSize

    def _passes_quality_thresholds(self, metricsRow, fieldnames, thresholdArgs,
                                    medianTiltAxis, pixelSize):
        # -- tilt_axis: maximum deviation from the median tilt axis (deg) --
        thresholdValue = thresholdArgs.get('tilt_axis', None)
        if thresholdValue not in (None, '') and medianTiltAxis is not None:
            col = self._find_csv_column(fieldnames, [self.TILT_AXIS_COLUMN])
            if col is not None:
                try:
                    deviation = abs(float(metricsRow[col]) - medianTiltAxis)
                    if deviation > float(thresholdValue):
                        return False
                except (TypeError, ValueError):
                    pass

        # -- thickness / global_shift: reported in pixels, GUI threshold
        # is in Angstrom, so convert using the tomogram's actual pixel size
        for guiKey, csvCol, mode in (
            ('thickness', self.THICKNESS_PIX_COLUMN, 'min'),
            ('global_shift', self.GLOBAL_SHIFT_PIX_COLUMN, 'max'),
        ):
            thresholdValue = thresholdArgs.get(guiKey, None)
            if thresholdValue in (None, '') or pixelSize is None:
                continue
            col = self._find_csv_column(fieldnames, [csvCol])
            if col is None:
                continue
            try:
                valueA = float(metricsRow[col]) * pixelSize
                thresholdValue = float(thresholdValue)
            except (TypeError, ValueError):
                continue
            if mode == 'max' and valueA > thresholdValue:
                return False
            if mode == 'min' and valueA < thresholdValue:
                return False

        # -- remaining metrics: direct comparison, no conversion needed --
        for guiKey, (candidates, mode) in self.QUALITY_METRIC_COLUMNS.items():
            thresholdValue = thresholdArgs.get(guiKey, None)
            if thresholdValue in (None, ''):
                continue  # no threshold configured for this metric

            col = self._find_csv_column(fieldnames, candidates)
            if col is None:
                continue  # column not found, can't verify, don't block on it

            try:
                metricValue = float(metricsRow[col])
                thresholdValue = float(thresholdValue)
            except (TypeError, ValueError):
                continue

            if mode == 'max' and metricValue > thresholdValue:
                return False
            if mode == 'min' and metricValue < thresholdValue:
                return False

        return True

    def _get_qualifying_tomograms(self, metricsFile):
        """ Return the list of currently-known input tomogram rows that
        have a matching entry in metricsFile and pass the configured
        quality thresholds. """
        if not os.path.exists(metricsFile):
            self.log(f"WARNING: metrics file not found, skipping quality "
                      f"filtering: {metricsFile}")
            return list(self.inputTomTable)

        fieldnames, rows = self._read_csv_rows(metricsFile)
        nameCol = self._find_csv_column(fieldnames, [self.TIlT_SERIES_COLUMN])
        if nameCol is None:
            self.log("WARNING: could not identify the tilt-series name "
                      "column in the metrics file, skipping quality "
                      "filtering.")
            return list(self.inputTomTable)

        metricsByName = {self._strip_metrics_name(row[nameCol]): row for row in rows}
        thresholdArgs = self._args.subset('train.dn3', new_prefix="")

        tiltAxisCol = self._find_csv_column(fieldnames, [self.TILT_AXIS_COLUMN])
        medianTiltAxis = None
        if tiltAxisCol is not None:
            try:
                medianTiltAxis = self._compute_median(
                    [float(r[tiltAxisCol]) for r in rows])
            except (TypeError, ValueError):
                medianTiltAxis = None

        pixelSize = self._get_ts_pixel_size()
        if pixelSize is None:
            self.log("WARNING: could not determine tilt series pixel size "
                      "from the input STAR file (rlnTomoTiltSeriesPixelSize) "
                      "thickness/global_shift quality checks will be skipped.")

        qualifying = []
        for row in self.inputTomTable:
            metricsRow = metricsByName.get(row.rlnTomoName)
            if metricsRow is None:
                continue  # no metrics entry yet for this tomogram
            if self._passes_quality_thresholds(metricsRow, fieldnames, thresholdArgs,
                                                medianTiltAxis, pixelSize):
                qualifying.append(row)

        return qualifying

    def _wait_for_training_set(self):
        """ Wait until enough tomograms are available for training, and
        return the actual list of rows to train on. """
        metricsFile = self._args.get('train.dn3.metrics_file', '')

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
    
    def prerun(self):
        self.inputToms = self._args['input_tomograms']
        self.n_training = int(self._args['train.n_training'])
 
        if self.registerOnly:
            self._register_existing_final_outputs()
            return
 
        self.inputTomTable = self._wait_for_input_table()
        self.log(f"Found input tomograms: {len(self.inputTomTable)}")

        # infer.dn3.model, if set and pointing to an existing file, means the
        # user wants to run inference only with that model, skipping
        # training entirely.
        if userModel := self._args.get('infer.dn3.model', ''):
            if not os.path.exists(userModel):
                raise Exception(f"Selected model not found: {userModel}")

            self.log(f"Pre-trained model selected, skipping training: {userModel}")
            self.modelPath = userModel
        
        else:
            trainingSubset = self._wait_for_training_set()

            self.log(f"Starting training with {len(trainingSubset)} tomograms")
            self.launch_training(trainingSubset)

            self.modelPath = self.trainingBestModel
            
        self.log(f"Using model for inference: {self.modelPath}")
 
        self.mkdir(self.outputTomDir)
 
        monitor = StarMonitor(self.inputToms, 'global',
                               lambda row: row.rlnTomoName, timeout=30)
        batchMgr = BatchManager(1, monitor.newItems(), self.tmpDir,
                                 itemFileNameFunc=self._filename)
        g = self.addGenerator(batchMgr.generate)
        self.addGpuProcessors(g, self.get_denoiset_proc, self._output)


if __name__ == '__main__':
    DenoisET.main()