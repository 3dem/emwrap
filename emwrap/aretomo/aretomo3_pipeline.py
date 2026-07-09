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
import re
import ntpath
import posixpath
import shutil
import csv

#Plots
# import mrcfile
# import numpy as np

from emtools.utils import Color, FolderManager
from emtools.image import Image
from emtools.jobs import Args, TsStarBatchManager
from emtools.metadata import StarFile, RelionStar, Acquisition, Table, Mdoc

from emwrap.base import ProcessingPipeline
from .aretomo3 import AreTomo3

# TODO: 
# - Fix sampling size
# - 'rlnTiltSeriesAligned' is not aligned is the normal tilt series unaligned

def _fix_mdoc_subframe_paths(mdocPath, destPath, relDir='.'):
    """
    Read mdocPath, rewrite every SubFramePath line so it points to the
    basename of the original movie file prefixed with relDir (mirroring
    the working `sed` approach: 'SubFramePath = ./tmp_mdoc/<basename>'),
    regardless of whether the original path used / or \\ or a UNC
    \\server\share\ prefix. Write the result to destPath.

    relDir: relative path (from AreTomo3's working directory) where the
            movie files actually live, e.g. '.' if mdoc and movies are
            siblings in the same batch folder, or 'tmp_mdoc' if movies
            live in a subfolder relative to where AreTomo3 is invoked.
    """
    
    mdoc = Mdoc.parse(mdocPath)
    # Let's write a local MDOC file with fixed filenames
    for _, section in mdoc.zvalues:
        section['SubFramePath'] = f'./{Mdoc.getSubFrameBase(section)}' 

    mdoc.write(destPath)


class AreTomo3Pipeline(ProcessingPipeline):
    """ Pipeline specific to AreTomo3 preprocessing. """
    name = 'emw-aretomo3'

    def __init__(self, args, output):
        ProcessingPipeline.__init__(self, args, output) # self._args is defined here
        gpus = self._args.get('gpus', '')
        if gpus is not None and gpus != '':
            gpus = str(gpus).strip()
        else:
            gpus = ''
        self.gpuList = self.get_gpu_list(gpus) if gpus else []
        self.outputTomDir = 'tomograms'
        self.outputTsDir = 'tiltseries'
        self.acq = self.loadAcquisition()
        self.inputLen = 0
        self.inputGain = self.acq.get('gain', None)
        self._allResults = {}  # tsName -> result dict, accumulated by _output
        self.inputTs = None  # set in prerun via _getInputTsTable
        self.registerOnly = self._is_register_only() # DEBUG flag

    def get_aretomo3_proc(self, gpu):
        def _aretomo3(batch):
            # In this pipeline, batch are not created until now, when we are
            # processing each one.
            # We also need to create the links to movies and mdocs files
            items = batch['items']
            mdoc = batch['tsMdoc']
            
            batch.create()
            
            def _absfn(item):
                return os.path.abspath(item['rlnMicrographMovieName'])

            mdocAbs = os.path.abspath(mdoc)
            print(f"mdoc: {mdocAbs}")

            # --- Full folder link, side location only, for browsing/debugging.
            framesFolder = os.path.dirname(_absfn(items[0]))
            framesLink = batch.join('frames')
            if not os.path.exists(framesLink):
                os.symlink(framesFolder, framesLink)

            # --- Batch-only movie links, directly in batch root.
            for item in items:
                srcPath = _absfn(item)
                baseName = os.path.basename(srcPath)
                destPath = batch.join(baseName)
                if not os.path.lexists(destPath):
                    os.symlink(srcPath, destPath)

            # --- Rewrite the mdoc's SubFramePath entries to basenames only,
            # and write the corrected copy directly into batch root (not a link,
            # since the content itself is being modified).
            mdocBase = os.path.basename(mdocAbs)
            mdocDest = batch.join(mdocBase)
            _fix_mdoc_subframe_paths(mdocAbs, mdocDest, relDir='.')
            # Link the gain reference
            acq = Acquisition(self.acq) 

            if self.inputGain:
                acq['gain'] = batch.link(self.inputGain)

            at3 = AreTomo3(acq, **self._args)
            at3.process_batch(batch, gpu=gpu)
            
            return batch

        return _aretomo3

    def _is_register_only(self):
        """ DEBUG: if True, don't run AreTomo3, just rebuild outputs from the final job folders. """
        return self._args.get('register_output_only', False)

    def _collect_existing_final_result(self, tsName):
        tsFolder = self._getOutputTsFolder(tsName)
        tomFolder = self._getOutputTomFolder(tsName)

        result = {'rlnTomoName': tsName}

        def _add_if_exists(key, folder, filename):
            path = folder.join(filename)
            if os.path.exists(path):
                result[key] = path
                return path
            return None

        # Files expected in jobX/tiltseries/<tsName>/
        _add_if_exists('rlnTiltSeriesAligned', tsFolder, f'{tsName}.mrc')
        _add_if_exists('rlnTiltSeriesOdd', tsFolder, f'{tsName}_ODD.mrc')
        _add_if_exists('rlnTiltSeriesEvn', tsFolder, f'{tsName}_EVN.mrc')
        _add_if_exists('rlnTomoAlignmentFile', tsFolder, f'{tsName}.aln')
        _add_if_exists('rlnTomoCtfFile', tsFolder, f'{tsName}_CTF.txt')
        _add_if_exists('rlnTomoCtfMrc', tsFolder, f'{tsName}_CTF.mrc')
        _add_if_exists('rlnTomoMetadata', tsFolder, f'{tsName}.star')

        _add_if_exists('aretomo3MetricsCsv', tsFolder, 'TiltSeries_Metrics.csv')
        _add_if_exists('aretomo3TimeStampCsv', tsFolder, 'TiltSeries_TimeStamp.csv')

        # Files expected in jobX/tomograms/<tsName>/
        _add_if_exists('rlnTomogram', tomFolder, f'{tsName}_Vol.mrc')
        _add_if_exists('rlnTomoNameOdd', tomFolder, f'{tsName}_ODD_Vol.mrc')
        _add_if_exists('rlnTomoNameEvn', tomFolder, f'{tsName}_EVN_Vol.mrc')
        _add_if_exists('aretomo3ThicknessMrc', tomFolder, f'{tsName}_Thick.mrc')
        _add_if_exists('aretomo3ThicknessCsv', tomFolder, f'{tsName}_Thick_CC.csv')

        if 'rlnTiltSeriesAligned' not in result:
            result['error'] = (
                f'Missing expected final aligned tilt-series: '
                f'{tsFolder.join(f"{tsName}.mrc")}'
            )

        return result
    
    def _register_existing_final_outputs(self):
        self.log(
            'DEBUG register-only mode: rebuilding outputs from final job folders.',
            flush=True
        )

        self._allResults = {}

        for row in self.inputTs:
            tsName = row.rlnTomoName

            result = self._collect_existing_final_result(tsName)
            self._allResults[tsName] = result

            if 'error' in result:
                self.log(f"DEBUG register-only: {tsName}: {result['error']}")
            else:
                self.log(f"DEBUG register-only: found final outputs for {tsName}")

        self._registerOutputs()

        self.info['register_only'] = True
        self.info['aretomo_output'] = len([
            r for r in self._allResults.values()
            if 'error' not in r
        ])
    
    def _getOutputTsFolder(self, tsName):
        return FolderManager(self.join(self.outputTsDir, tsName))

    def _getOutputTomFolder(self, tsName):
        return FolderManager(self.join(self.outputTomDir, tsName))
    
    def _output(self, batch):
        """ Register output STAR files. Runs per-batch (streaming): each
        call rewrites the aggregate tables to include this batch's result,
        so outputs are available incrementally as each tilt series finishes,
        without waiting for the whole input to be processed. """
        tsName = batch['tsName']
        batch.log(f"Storing output for batch '{tsName}'", flush=True)

        if batch.error:
            batch.log(f"ERROR: {batch.error}")
            self._allResults[tsName] = {'error': batch.error}
        else:
            result = batch['results'][0] if batch['results'] else {}

            tsFolder = self._getOutputTsFolder(tsName)
            tsFolder.create()
            tomFolder = None  # only created if a tomogram was produced

            def _copy(srcKey, destFolder):
                """ Copy the file at result[srcKey] into destFolder, update
                result[srcKey] to the copied file. Returns the new path, or
                None if srcKey wasn't populated for this batch. """
                src = result.get(srcKey, None)
                if src is None or not os.path.exists(src):
                    return None

                dst = destFolder.join(os.path.basename(src))
                shutil.copy2(src, dst)
                result[srcKey] = dst
                return dst

            # --- Aligned tilt series outputs -> outputTsDir
            _copy('rlnTiltSeriesAligned', tsFolder)
            _copy('rlnTiltSeriesOdd', tsFolder)
            _copy('rlnTiltSeriesEvn', tsFolder)
            _copy('rlnTomoAlignmentFile', tsFolder)
            _copy('rlnTomoCtfFile', tsFolder)
            _copy('rlnTomoCtfMrc', tsFolder)
            _copy('aretomo3MetricsCsv', tsFolder)
            _copy('aretomo3TimeStampCsv', tsFolder)
            metadataDest = _copy('rlnTomoMetadata', tsFolder)

            # --- Tomogram outputs (only present if reconstruction was enabled) -> outputTomDir
            if result.get('rlnTomogram', None) is not None:
                tomFolder = self._getOutputTomFolder(tsName)
                tomFolder.create()
                _copy('rlnTomogram', tomFolder)
                _copy('rlnTomoNameOdd', tomFolder)
                _copy('rlnTomoNameEvn', tomFolder)
                _copy('aretomo3ThicknessMrc', tomFolder)
                _copy('aretomo3ThicknessCsv', tomFolder)

            # The metadata star file was written while everything still
            # lived in the batch temp folder, so its internal path
            # references are now stale. Rewrite them in place to point at
            # the final, post-move locations.
            if metadataDest is not None:
                self._fixMetadataStarPaths(metadataDest, result)

            batch.info['result'] = {k: v for k, v in result.items()
                                    if k != 'error'}
            self._allResults[tsName] = result

        batch.info['tsName'] = tsName  # Store tsName in the info.json

        # Rebuild and re-register the aggregate outputs now, so they reflect
        # everything completed so far -- this is what makes results visible
        # incrementally rather than only once the whole input is processed.
        self._registerOutputs()

        self.updateBatchInfo(batch)

        if self.inputLen:
            totalOutput = len(self.info['batches'])
            percent = totalOutput * 100 / self.inputLen
            batch.log(f">>> Processed {Color.green(totalOutput)} out of "
                    f"{Color.red(self.inputLen)} "
                    f"({Color.bold('%0.2f' % percent)} %)", flush=True)
        
        return batch
    
    def _fixMetadataStarPaths(self, metadataStarPath, result):
        """ Rewrite the metadata star file's 'general' table in place with
        the post-move (final) paths, since it was originally written while
        the files still lived in the batch temp folder. """
        with StarFile(metadataStarPath) as sf:
            tGeneral = sf.getTable('general')
            row = tGeneral[0]._asdict()

        pathFields = ['rlnTiltSeriesAligned', 'rlnTiltSeriesOdd', 'rlnTiltSeriesEvn', 'rlnTomoAlignmentFile',
                    'rlnTomogram', 'rlnTomoCtfFile', 'rlnTomoCtfMrc',
                    'rlnTomoNameOdd', 'rlnTomoNameEvn']
        for field in pathFields:
            if field in row and field in result:
                row[field] = result[field]

        tGeneral = Table(list(row.keys()))
        tGeneral.addRowValues(**row)

        with StarFile(metadataStarPath, 'w') as sfOut:
            sfOut.writeTimeStamp()
            sfOut.writeTable('general', tGeneral, singleRow=True)
    
    def write_ts_table(self, tableName, table, starFile):
        self.log(f"Writing: {starFile}")
        with StarFile(starFile, 'w') as sfOut:
            sfOut.writeTable(tableName, table, computeFormat='left', timeStamp=True)

    def _read_csv_rows(self, csvPath):
        with open(csvPath, newline='') as f:
            reader = csv.DictReader(f)
            return reader.fieldnames, list(reader)

    def _write_csv_rows(self, csvPath, fieldnames, rows):
        if not fieldnames or not rows:
            return False

        with open(csvPath, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

        return True

    def _registerAretomo3CsvOutputs(self):
        metricsRows = []
        metricsHeader = None

        timestampRows = []
        timestampHeader = None

        for tsName, result in self._allResults.items():
            if 'error' in result:
                continue

            metricsCsv = result.get('aretomo3MetricsCsv')
            if metricsCsv and os.path.exists(metricsCsv):
                header, rows = self._read_csv_rows(metricsCsv)
                if metricsHeader is None:
                    metricsHeader = header
                metricsRows.extend(rows)

            timestampCsv = result.get('aretomo3TimeStampCsv')
            if timestampCsv and os.path.exists(timestampCsv):
                header, rows = self._read_csv_rows(timestampCsv)
                if timestampHeader is None:
                    timestampHeader = header
                timestampRows.extend(rows)

        metricsOut = self.join('TiltSeries_Metrics.csv')
        timestampOut = self.join('TiltSeries_TimeStamp.csv')

        if self._write_csv_rows(metricsOut, metricsHeader, metricsRows):
            self.outputs['AreTomo3Metrics'] = {
                'label': 'AreTomo3 Metrics',
                'type': 'File',
                'info': f'{len(metricsRows)} rows',
                'files': [[metricsOut, 'text/csv']]
            }

        if self._write_csv_rows(timestampOut, timestampHeader, timestampRows):
            self.outputs['AreTomo3TimeStamp'] = {
                'label': 'AreTomo3 TimeStamp',
                'type': 'File',
                'info': f'{len(timestampRows)} rows',
                'files': [[timestampOut, 'text/csv']]
            }
    
    def _registerOutputs(self):
        """ Rebuild the two aggregate output tables (TiltSeriesAligned,
        Tomograms) from everything in self._allResults so far, and update
        self.outputs. Called after every batch completes, so the pipeline
        can stream partial results before the full input is processed. """
        tsExtraCols = ['rlnTiltSeriesAligned', 'rlnTiltSeriesOdd', 'rlnTiltSeriesEvn','rlnTomoAlignmentFile',
                    'rlnTomoCtfFile', 'rlnTomoMetadata']
        tomExtraCols = ['rlnTomogram', 'rlnTomoNameOdd', 'rlnTomoNameEvn',
                        'rlnTomoTiltSeriesPixelSize']

        inputCols = self.inputTs.getColumnNames()
        inputByName = {row.rlnTomoName: row for row in self.inputTs}

        newTsTable = Table(inputCols + tsExtraCols)
        failedTable = Table(newTsTable.getColumnNames())
        newTomTable = Table(inputCols + tomExtraCols)

        tsDims = None
        tomDims = None
        haveTomograms = False

        # Only iterate tilt series 
        for tsName, result in self._allResults.items():
            tsRow = inputByName.get(tsName, None)
            if tsRow is None:
                continue
            tsDict = tsRow._asdict()

            if 'error' in result:
                tsDict.update({
                    'rlnTiltSeriesAligned': 'None',
                    'rlnTiltSeriesOdd': 'None',
                    'rlnTiltSeriesEvn': 'None',
                    'rlnTomoAlignmentFile': 'None',
                    'rlnTomoCtfFile': 'None',
                    'rlnTomoMetadata': 'None',
                })
                failedTable.addRowValues(**tsDict)
                continue

            tsAligned = result.get('rlnTiltSeriesAligned', None)
            if tsAligned is None or not os.path.exists(tsAligned):
                tsDict.update({
                    'rlnTiltSeriesAligned': 'None',
                    'rlnTiltSeriesOdd': 'None',
                    'rlnTiltSeriesEvn': 'None',
                    'rlnTomoAlignmentFile': 'None',
                    'rlnTomoCtfFile': 'None',
                    'rlnTomoMetadata': 'None',
                })
                failedTable.addRowValues(**tsDict)
                continue

            if tsDims is None:
                tsDims = Image.get_dimensions(tsAligned)

            tsDict.update({
                'rlnTiltSeriesAligned': tsAligned,
                'rlnTiltSeriesOdd': result.get('rlnTiltSeriesOdd', ''),
                'rlnTiltSeriesEvn': result.get('rlnTiltSeriesEvn', ''),
                'rlnTomoAlignmentFile': result.get('rlnTomoAlignmentFile', ''),
                'rlnTomoCtfFile': result.get('rlnTomoCtfFile', ''),
                'rlnTomoMetadata': result.get('rlnTomoMetadata', ''),
            })
            newTsTable.addRowValues(**tsDict)

            tomogram = result.get('rlnTomogram', None)
            if tomogram is not None and os.path.exists(tomogram):
                haveTomograms = True
                if tomDims is None:
                    tomDims = Image.get_dimensions(tomogram)
                tomDict = tsRow._asdict()
                tomDict.update({
                    'rlnTomogram': tomogram,
                    'rlnTomoNameOdd': result.get('rlnTomoNameOdd', ''),
                    'rlnTomoNameEvn': result.get('rlnTomoNameEvn', ''),
                    'rlnTomoTiltSeriesPixelSize': self.acq.pixel_size,
                })
                newTomTable.addRowValues(**tomDict)

        tsStarFile = self.join('aligned_tilt_series.star')
        failedStarFile = self.join('tilt_series_failed.star')
        tomStarFile = self.join('tomograms.star')

        self.write_ts_table('global', newTsTable, tsStarFile)

        N = len(newTsTable)
        x, y, n = tsDims if tsDims else (0, 0, 0)
        self.outputs = {
            'TiltSeriesAligned': {
                'label': 'Tilt Series Aligned',
                'type': 'TiltSeriesAligned',
                'info': f"{N} items, {x} x {y} x {n}, {self.acq.pixel_size:0.3f} Å/px",
                'files': [
                    [tsStarFile, 'TomogramGroupMetadata.star.relion.tomo.aligntiltseries']
                ]
            }
        }

        if len(failedTable) > 0:
            self.write_ts_table('global', failedTable, failedStarFile)
            self.outputs['TiltSeriesFailed'] = {
                'label': 'Tilt Series Failed',
                'type': 'TiltSeriesFailed',
                'info': f"{len(failedTable)} items",
                'files': [
                    [failedStarFile, 'TomogramGroupMetadata.star.relion.tomo.failed']
                ]
            }

        if haveTomograms:
            self.write_ts_table('global', newTomTable, tomStarFile)
            M = len(newTomTable)
            tx, ty, tn = tomDims if tomDims else (0, 0, 0)
            self.outputs['Tomograms'] = {
                'label': 'Tomograms',
                'type': 'Tomograms',
                'info': f"{M} items, {tx} x {ty} x {tn}, {self.acq.pixel_size:0.3f} Å/px",
                'files': [
                    [tomStarFile, 'TomogramGroupMetadata.star.relion.tomo.tomograms']
                ]
            }
        
        self._registerAretomo3CsvOutputs()
    
    def _getInputTsTable(self):
        """ Read input star file and return the 'global' table. """
        inputStar = self._args['input_tiltseries']
        with StarFile(inputStar) as sf:
            t = sf.getTable('global')
            self.inputLen = len(t)  # Let's update the inputLen property
            return t
        return None
    
    # TODO: check if using 
    def get_float(self, key, defaultValue):
        if v := self._args.get(key, None):
            return float(v)
        return defaultValue

    def _globalStarColumns(self):
        return ['rlnTomoName', 'rlnTiltSeriesAligned', 'rlnTiltSeriesOdd', 'rlnTiltSeriesEvn',
                'rlnTomoAlignmentFile', 'rlnTomoCtfFile',
                'rlnTomogram', 'rlnTomoNameOdd', 'rlnTomoNameEvn',
                'rlnTomoMetadata', 'rlnTomoTiltSeriesPixelSize']

    def _appendToGlobalStar(self, tsName, result):
        """ Append one row for this tilt series into the running global
        aggregate star file, creating it (with header) on first write. """
        globalStarPath = self.join('tilt_series.star')
        cols = self._globalStarColumns()
        newPixelSize = self.acq.pixel_size  # AreTomo3 itself handles binning

        fileExists = os.path.exists(globalStarPath)
        existingRows = []
        if fileExists:
            with StarFile(globalStarPath) as sf:
                existingRows = list(sf.getTable('global'))

        outTable = Table(cols)
        for row in existingRows:
            outTable.addRowValues(*[getattr(row, c, '') for c in cols])

        values = {c: result.get(c, '') for c in cols}
        values['rlnTomoName'] = tsName
        values['rlnTomoTiltSeriesPixelSize'] = newPixelSize
        outTable.addRowValues(*[values[c] for c in cols])

        with StarFile(globalStarPath, 'w') as sfOut:
            sfOut.writeTimeStamp()
            sfOut.writeHeader('global', outTable)
            for row in outTable:
                sfOut.writeRowValues(row._asdict())
    
    def prerun(self):
        self.inputTs = self._getInputTsTable()
        print(f"Input tilt-series: {len(self.inputTs)}")  
        
        self.mkdir(self.outputTsDir)
        self.mkdir(self.outputTomDir)


        if self.registerOnly:
            self._register_existing_final_outputs()
            return
        
        batchMgr = TsStarBatchManager(self.inputTs, self.tmpDir)
        g = self.addGenerator(batchMgr.generate)
        outputQueue = None
        
        print(f"Creating {len(self.gpuList)} processing threads.")
        for gpu in self.gpuList:
            p = self.addProcessor(g.outputQueue,
                                  self.get_aretomo3_proc(gpu),
                                  outputQueue=outputQueue)
            outputQueue = p.outputQueue

        self.addProcessor(outputQueue, self._output)


if __name__ == '__main__':
    AreTomo3Pipeline.main()