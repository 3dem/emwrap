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
from emtools.jobs import Args
from emtools.metadata import StarFile, Table

from .warp import WarpBasePipeline


class WarpCtfReconstruct(WarpBasePipeline):

    """ Script to run warp_ts_aretomo. """
    name = 'emw-warp-ctfrec'

    def runBatch(self, batch, **kwargs):
        inputTs = kwargs['inputTs']
        tsAllTable = StarFile.getTableFromFile('global', inputTs)
        # N = len(tsAllTable)
        # ps = tsAllTable[0].rlnTomoTiltSeriesPixelSize
        # # FIXME: Avoid use of rlnTiltSeriesAligned, since it might not be availabe and it is not part of the data model
        # x, y, n = Image.get_dimensions(tsAllTable[0].rlnTiltSeriesAligned)

        if kwargs.get('importInputs', True):
            inputFolder = FolderManager(os.path.dirname(inputTs))
            self._importInputs(inputFolder)
            self._removeImportedTsReconstruction()

        # Run ts_ctf
        args = Args({
            'WarpTools': 'ts_ctf',
            '--settings': self.TSS,
            '--voltage': int(self.acq.voltage),
            '--cs': self.acq.cs,
            '--amplitude': self.acq.amplitude_contrast,
            '--auto_hand': 8
        })
        if self.gpuList:
            args['--device_list'] = self.gpuList

        args.update(self.get_subargs('ts_ctf', '--'))
        self.batch_execute('ts_ctf', batch, args)

        # Run ts_reconstruct
        args = Args({
            'WarpTools': 'ts_reconstruct',
            '--settings': self.TSS
        })
        if self.gpuList:
            args['--device_list'] = self.gpuList
        args.update(self.get_subargs('ts_reconstruct', '--'))
        self.batch_execute('ts_reconstruct', batch, args)
        self.updateBatchInfo(batch)

    def _output(self, batch):
        """ Register output STAR files. """
        self.log("Registering output STAR files.")
        tsAllTable = StarFile.getTableFromFile('global', self.inputTs)

        newTsStarFile = batch.join('tomograms.star')

        extraLabels = [
            'rlnTomoReconstructedTomogram',
            'rlnTomoTomogramBinning',
            'rlnDefocus',
            'rlnTomoSizeX',
            'rlnTomoSizeY',
            'rlnTomoSizeZ',
            'rlnTomoReconstructedTomogramHalf1',
            'rlnTomoReconstructedTomogramHalf2',
            'wrpTomostar'
        ]
        newPs = self.reconstructPs()
        newTsAllTable = Table(tsAllTable.getColumnNames() + extraLabels)
        dims = None
        for tsRow in tsAllTable:
            tsDict = tsRow._asdict()
            ok, tsDims = self.updateCtfRecTsDict(tsDict, newPs)
            if tsDims is not None:
                dims = tsDims
            if not ok:
                self.log(f"WARNING: Missing reconstructed tomogram for TS {tsDict['rlnTomoName']}")
            newTsAllTable.addRowValues(**tsDict)

        # Write the corrected_tilt_series.star
        self.write_ts_table('global', newTsAllTable, newTsStarFile)

        N = len(newTsAllTable)
        x, y, n = dims
        outputNodes = [[newTsStarFile, 'TomogramGroupMetadata.star.relion.tomo.Tomograms']]
        self.writeRelionOutputNodes(outputNodes)
        self.updateBatchInfo(batch)

    def _removeImportedTsReconstruction(self):
        """Drop a previous reconstruction folder linked during input import."""
        recpath = self.join(self.TS, 'reconstruction')
        if os.path.lexists(recpath):
            self.log(f"Removing previous reconstruction imported from input: {recpath}")
            if os.path.islink(recpath):
                os.unlink(recpath)
            else:
                raise ValueError(f"Reconstruction path {recpath} is not a link")

    def prerun(self):
        self.prerunTs()


if __name__ == '__main__':
    WarpCtfReconstruct.main()
