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

#Plots and reading mrc
# import mrcfile
# import numpy as np

from emtools.utils import Color, FolderManager
from emtools.image import Image
from emtools.jobs import TsStarBatchManager # Maybe we need a new BatchManager
from emtools.metadata import StarFile, Acquisition, Table, Mdoc

from emwrap.base import ProcessingPipeline

class DenoisET(ProcessingPipeline):
    """ Wrapper specific to DenoisET Noise2Noise algorithm."""

    name = 'emw-denoiset'

    def __init__(self, args, output):
        ProcessingPipeline.__init__(self, args, output) # self._args is defined here
        gpus = self._args.get('gpus', '')
        if gpus is not None and gpus != '':
            gpus = str(gpus).strip()
        else:
            gpus = ''
        self.gpuList = self.get_gpu_list(gpus) if gpus else []
        self.outputTomDir = 'tomograms'
        self.trainingDir = 'training'
        self.inputLen = 0
        self._allResults = {}  # tsName -> result dict, accumulated by _output
        self.inputTomTable = None  # set in prerun via _getInputTomTable
        self.inputToms = None
        self.registerOnly = self._register_output_only() # DEBUG flag
        self.launcher_denoiset = self._args.get('launcher_denoiset', None)

    
    # -------- Simple configuration / column definitions --------
    def _tomogram_extra_cols(self):
        return [
            # Tomo specific columns
            'rlnTomoReconstructedTomogram',
            'rlnTomoReconstructedTomogramHalf1',
            'rlnTomoReconstructedTomogramHalf2',
        ]

    # -------- Small generic helpers -------------
    def _get_launcher(self):
        return self.launcher_denoiset or ProcessingPipeline.get_launcher('DENOISET')

    @classmethod
    def _serialize_form_args(cls, formArgs):
        args = Args(formArgs)
        subargs = args.subset('aretomo3', new_prefix="-", 
                                 filters=['remove_empty', 'binary_boolean', 'multiple_values'], 
                                 multiple_values=cls._MULTIPLE_VALUE_FLAGS)
        
        subargs.update(Args.fromString(subargs.pop('-ExtraArgs', '')))
        
        return subargs
    
    # ----- Input/output folder helpers --------
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

    def write_ts_table(self, tableName, table, starFile):
        self.log(f"Writing: {starFile}")
        with StarFile(starFile, 'w') as sfOut:
            sfOut.writeTable(tableName, table, computeFormat='left', timeStamp=True)
    
    # -------- Parsers for Denoiset output files --------------
    
    # ----- Relion metadata conversion helpers -----------
    def _build_tomogram_row(self, tsRow, result):
        tomDict = tsRow._asdict()

        tomDict.update({
            'rlnTomoReconstructedTomogram': result.get('rlnTomoReconstructedTomogram', ''), # We need to decide whether this we update or create a new label for denoised tomograms
        })

        return tomDict


    # ------- Output registration ------------
    def _read_csv_rows(self, csvPath):
        with open(csvPath, newline='') as f:
            reader = csv.DictReader(f)
            return reader.fieldnames, list(reader)
    
    def _collect_existing_final_result(self, tsName):
        tomFolder = self._getOutputTomFolder(tsName)

        result = {'rlnTomoName': tsName}

        def _add_if_exists(key, folder, filename):
            path = folder.join(filename)
            if os.path.exists(path):
                result[key] = path
                return path
            return None

        def _add_folder_if_exists(key, folder, dirname):
            path = folder.join(dirname)
            if os.path.isdir(path):
                result[key] = path
                return path
            return None
        
        # _add_folder_if_exists('at3ImodFolder', tsFolder, f'{tsName}_Imod')
       
        # Files expected in jobX/tomograms/<tsName>/
        # _add_if_exists('rlnTomoReconstructedTomogram', tomFolder, f'{tsName}_Vol.mrc')
        # _add_if_exists('at3ThicknessCsv', tomFolder, f'{tsName}_Thick_CC.csv')

        return result
    
    def _register_existing_final_outputs(self):
        self.log(
            'DEBUG register-only mode: rebuilding outputs from final job folders.',
            flush=True
        )

        self._allResults = {}

        for row in self.inputTomTable:
            tsName = row.rlnTomoName
            result = self._collect_existing_final_result(tsName)
            self._allResults[tsName] = result

            if 'error' in result:
                self.log(f"DEBUG register-only: {tsName}: {result['error']}")
            else:
                self.log(f"DEBUG register-only: found final outputs for {tsName}")

        self._registerOutputs()

        self.info['register_only'] = True
        self.info['denoiset_output'] = len([
            r for r in self._allResults.values()
            if 'error' not in r
        ])

    def _registerOutputs(self):
        """Rebuild Relion-style AreTomo3 outputs.
        Outputs:
            tomograms.star
                Global table containing denoised tomograms, only if
                denoising was produced.

            failed_tomograms.star
                Global table containing tomograms that failed registration.
        """

        self.log("Registering output STAR files.")

        failedStarFile = self.join('failed_tomograms.star')
        tomogramsStarFile = self.join('tomograms.star')

        inputCols = self.inputTomTable.getColumnNames()

        tomExtraCols = [
            c for c in self._tomogram_extra_cols()
            if c not in inputCols
        ]

        failedTable = Table(inputCols)
        tomogramsTable = Table(inputCols + tomExtraCols)

        inputByName = {row.rlnTomoName: row for row in self.inputTomTable}

        tomDims = None

        for tsName, result in self._allResults.items():
            tsRow = inputByName.get(tsName, None)
            if tsRow is None:
                self.log(f"WARNING: Result for unknown tomogram {tsName}, skipping.")
                continue

            if 'error' in result:
                failedTable.addRowValues(**tsRow._asdict())
                continue

            tomogram = result.get('rlnTomoReconstructedTomogram', None)
            if tomogram and os.path.exists(tomogram):   
                tomRow = self._build_tomogram_row(tsRow, result)
                tomogramsTable.addRowValues(**tomRow)

        outputNodes = []

        if len(failedTable) > 0:
            self.write_ts_table('global', failedTable, failedStarFile)
            outputNodes.append(
                [failedStarFile, 'TomogramGroupMetadata.star.relion.tomo.tomograms-failed'])

        # tomograms.star
        self.write_ts_table('global', tomogramsTable, tomogramsStarFile)
        outputNodes.append([tomogramsStarFile, 'TomogramGroupMetadata.star.relion.tomo.tomograms'])

        self.writeRelionOutputNodes(outputNodes)    
    
    # ---------- Batch execution --------------
    def get_denoiset_proc(self, gpu):
        def _denoiset(batch):
            # In this pipeline, batch are not created until now, when we are
            # processing each one.
            # We also need to create the links to movies and mdocs files
            items = batch['items']            
            batch.create()
            
            def _absfns(item):
                cols_to_copy = ['rlnTomoReconstructedTomogram', 'rlnTomoReconstructedTomogramHalf1', 'rlnTomoReconstructedTomogramHalf2']
                true_paths = []                
                for col in cols_to_copy:
                    true_paths.append(os.path.abspath(item[col]))

                return true_paths

        
            # --- TODO: link the following columns containing the TS_NAME_Vol.mrc, TS_NAME_ODD_Vol.mrc and TS_NAME_EVN_Vol.mrc into the same directory. All tomograms need to be linked into the same directory. 
            for item in items:
                srcPath = _absfns(item)
                for srcPath in _absfns(item):
                    baseName = os.path.basename(srcPath)
                    destPath = batch.join(baseName)
                    if not os.path.lexists(destPath):
                        os.symlink(srcPath, destPath)

            # at3 = AreTomo3(acq, **self._args) # TODO: DenoisET
            # at3.process_batch(batch, gpu=gpu)
            
            return batch

        return _denoiset
    
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

            tsFolder = self._getOutputTomFolder(tsName)
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
                
                if os.path.abspath(src) != os.path.abspath(dst):
                    shutil.copy2(src, dst)
                
                result[srcKey] = dst
                return dst

            def _copy_folder(srcKey, destFolder):
                """Copy the folder at result[srcKey] into destFolder, update result[srcKey].
                Returns the copied folder path, or None if srcKey is missing.
                """
                src = result.get(srcKey, None)
                if src is None or not os.path.isdir(src):
                    return None

                dst = destFolder.join(os.path.basename(src))

                if os.path.abspath(src) != os.path.abspath(dst):
                    if os.path.exists(dst):
                        shutil.rmtree(dst)
                    shutil.copytree(src, dst)

                result[srcKey] = dst
                return dst

            # --- Tomogram outputs (only present if reconstruction was enabled) -> outputTomDir
            tomFolder = self._getOutputTomFolder(tsName)
            tomFolder.create()
            _copy('rlnTomoReconstructedTomogram', tomFolder) # TODO: this should be the denoised tomogram
            # --- Denoiset folder -> outputTomDir/
            # _copy_folder('at3ImodFolder', tomFolder)# TODO: copy everything that is generated from DENOISET

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

    def launch_training(tom_table, n_train):

        self.mkdir(self.trainingDir)
        
        def _absfns(tomDict):
            cols_to_copy = ['rlnTomoReconstructedTomogram', 'rlnTomoReconstructedTomogramHalf1', 'rlnTomoReconstructedTomogramHalf2']
            true_paths = []                
            for col in cols_to_copy:
                true_paths.append(os.path.abspath(tomDict[col]))

            return true_paths

        # --- TODO: link the following columns containing the TS_NAME_Vol.mrc, TS_NAME_ODD_Vol.mrc and TS_NAME_EVN_Vol.mrc into the same directory. All tomograms need to be linked into the same directory. 
        n = 1 
        for row in tom_table:
            if n >= n_train:
                break 

            tomDict = row._asdict()
            srcPath = _absfns(tomDict)
            for srcPath in _absfns(item):
                baseName = os.path.basename(srcPath)
                destPath = self.join(self.trainingDir, baseName)
                if not os.path.lexists(destPath):
                    os.symlink(srcPath, destPath)

            n += 1

        # at3 = AreTomo3(acq, **self._args) # TODO: DenoisET
        # at3.process_batch(batch, gpu=gpu)
    
    # -------- Pipeline lifecycle ---------- 
    def prerun(self):
        self.inputToms =  self._args['input_tomograms']
        # Get training params at some point
        self.n_training = int(self._args['train.n_training'])

        if self.registerOnly:
            self._register_existing_final_outputs()
            return

        while not self._getInputTomTable():
            time.sleep(30)
            print('No input found: sleeping for 30 seconds')

        self.inputTomTable = self._getInputTomTable()  # self.inputLen is set during this function 
        print(f"Found Input tomograms: {len(self.inputTomTable)}")  
        
        while self.inputLen < self.n_training:
            training_table = self._getInputTomTable() # self.inputLen is set during this function 
            # launcher = self._get_launcher()
            # Now how are we going to execute this 
            # if we dont have a batch.call?, what is it?
            time.sleep(30)
            print('sleeping for 30 seconds')

        print(f"Len training set: {len(training_table)}")
        print("Start training")

        # TODO: function to launch training
        self.launch_training(training_table, self.n_training)
        
        # self.mkdir(self.outputTomDir) # Here we should have the denoised tomograms

        # For the training we will make al loop that looks into the self.inputTomTable
        
        # We will follow this approach for the denoising tomograms
        # batchMgr = TsStarBatchManager(self.inputTomTable, self.tmpDir)
        # g = self.addGenerator(batchMgr.generate)
        # self.addGpuProcessors(g, self.get_denoiset_proc, self._output)


if __name__ == '__main__':
    DenoisET.main()