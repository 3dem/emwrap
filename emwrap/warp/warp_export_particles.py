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
import json
import argparse
import time
import sys
from glob import glob
from datetime import datetime

from emtools.utils import Color, FolderManager, Path, Process
from emtools.metadata import StarFile, Acquisition, Table
from emtools.jobs import Batch, Args
from emtools.image import Image


from .warp import WarpBasePipeline


class WarpExportParticles(WarpBasePipeline):
    """ Script to run warp_ts_aretomo. """
    name = 'emw-warp-export'

    def prerun(self):
        inTomoStar = self._args['input_tomograms']

        inTable = StarFile.getTableFromFile('global', inTomoStar)
        firstRow = inTable[0]
        columns = inTable.getColumnNames()

        if 'rlnCoordinatesMetadata' not in columns:
            raise Exception("Missing 'rlnCoordinatesMetadata' "
                            "column from input STAR file. ")
        if 'wrpTomostar' not in columns:
            raise Exception("Missing 'wrpTomostar' column from input STAR file. "
                            "For Warp export-particles, picking needs to be "
                            "done after Warp ctfrec.")

        self.log(f"Input star file: {Color.bold(inTomoStar)}")
        N = len(inTable)
        n = sum(row.rlnCoordinatesCount for row in inTable)
        self.log(f"Total input tomograms: {Color.green(N)}")

        self.log(f"Input number of particles: {Color.green(n)}")
        # Register input in the info.json file
        self.inputs = {
            'TomogramCoordinates': {
                'label': 'Tomogram Coordinates',
                'type': 'TomogramCoordinates',
                'info': f"{n} particles from {N} tomograms",
                'files': [
                    [inTomoStar, 'TomogramGroupMetadata.star.relion.tomo.tomocoordinates']
                ]
            }
        }
        self.writeInfo()
        warpPath = self.project.join(firstRow.wrpTomostar)
        warpFolder = os.path.dirname(os.path.dirname(warpPath))

        self._joinStarFiles(inTable)
        # Import inputs except tomostar, that might come from a different folder
        self._importInputs(warpFolder, keys=['fs', 'fss', 'ts', 'tss', 'tm'])
        self.mkdir('Particles')

        batch = Batch(id=self.name, path=self.path)
        subargs = self.get_subargs("ts_export_particles")

        # Run ts_ctf
        box = subargs['box']
        ps = subargs['output_angpix']
        outStar = "warp_particles.star"
        args = Args({
            'WarpTools': "ts_export_particles",
            "--settings": self.TSS,
            "--input_star": "all_coordinates.star",
            "--box": box,
            "--diameter": subargs['diameter'],
            "--coords_angpix": firstRow.rlnTomogramPixelSize,
            "--output_angpix": ps,
            "--output_star": outStar,
            "--output_processing": "Particles",
            f"--{self._args['ts_export_type']}": ""  # 2d or 3d
        })
        if self.gpuList:
            args['--device_list'] = self.gpuList

        self.batch_execute('ts_export_particles', batch, args)

        iosFn = self.join('warp_particles_optimisation_set.star')
        ptsFn = self.join('warp_particles.star')
        tomoPtsFn = self.join('warp_particles_tomograms.star')

        outFn = ptsFn

        ptsTableName = ''

        if os.path.exists(iosFn):
            outFn = iosFn
            ptsTableName = 'particles'
            self._fixPaths(iosFn, '', ['rlnTomoParticlesFile', 'rlnTomoTomogramsFile'])

        if os.path.exists(ptsFn):
            self._fixPaths(ptsFn, ptsTableName, ['rlnImageName', 'rlnCtfImage'])

        if os.path.exists(tomoPtsFn):
            self._fixPaths(tomoPtsFn, 'global', ['rlnTomoTiltSeriesName'])

        self.outputs = {
            'TomogramParticles': {
                'label': 'Tomogram Particles',
                'type': 'TomogramParticles',
                'info': f"{n} items (box size: {box} px, {ps} Å/px)",
                'files': [
                    [outFn, 'TomogramGroupMetadata.star.relion.tomo.particles']
                ]
            }
        }
        self.updateBatchInfo(batch)

    def _joinStarFiles(self, inTable):
        """ Join all input coordinates star files into a single one,
        and correct the rlnMicrographName to use the .tomostar suffix
        """
        outStarFile = self.join('all_coordinates.star')
        self.log(f"Writing output star file: {Color.bold(outStarFile)}")
        with StarFile(outStarFile, 'w') as sfOut:
            newTable = None
            for tomoRow in inTable:
                starFn = tomoRow.rlnCoordinatesMetadata
                if self.project.exists(starFn):
                    self.log(f"Parsing file: {starFn}")
                    # Update micrographs.star
                    with StarFile(self.project.join(starFn)) as sf:
                        if t := sf.getTable('particles'):
                            if newTable is None:
                                # Replace column rlnMicrographName by rlnTomoName
                                newCols = ['rlnTomoName' if c == 'rlnMicrographName' else c
                                           for c in t.getColumnNames()]
                                newTable = Table(newCols)
                                sfOut.writeTimeStamp()
                                sfOut.writeHeader('particles', newTable)
                            for row in t:
                                rowDict = row._asdict()
                                del rowDict['rlnMicrographName']
                                rowDict['rlnTomoName'] = os.path.basename(tomoRow.wrpTomostar)
                                sfOut.writeRowValues(rowDict)

    def _fixPaths(self, starFn, tableName, labels):
        """ Add the run folder to the star file paths. """
        starFnOut = starFn.replace('.star', '_fixed.star')
        # Iterate over all tables and fix paths for the selected one
        with StarFile(starFn) as sf:
            with StarFile(starFnOut, 'w') as sfOut:
                sfOut.writeTimeStamp()
                tableNames = sf.getTableNames()
                for tn in tableNames:
                    table = sf.getTable(tn, guessType=False)

                    if tn == tableName:
                        newTable = table.cloneColumns()
                        for row in table:
                            rowDict = row._asdict()
                            for label in labels:
                                if label in rowDict:
                                    rowDict[label] = self.join(rowDict[label])
                            newTable.addRowValues(**rowDict)
                    else:
                        newTable = table

                    singleRow = len(newTable) == 1
                    sfOut.writeTable(tn, newTable, computeFormat='right', singleRow=singleRow)
        # Override input star file
        shutil.move(starFnOut, starFn)


if __name__ == '__main__':
    WarpExportParticles.main()
