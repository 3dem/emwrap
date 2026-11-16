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
import argparse
import shutil
import time
from glob import glob
import threading
from datetime import datetime

from emtools.utils import Color, Pretty, Path, FolderManager
from emtools.metadata import Acquisition, StarFile, RelionStar, Table
from emtools.jobs import MdocBatchManager
from emtools.image import Image

from emwrap.base import ProcessingPipeline


class ImportTsPipeline(ProcessingPipeline):
    name = 'emw-import-ts'

    def __init__(self, args, output):
        ProcessingPipeline.__init__(self, args, output)
        self.acq = Acquisition({k.replace('acq.', ''): v
                                for k, v in args.items() if k.startswith('acq.')})

        self.wait = {
            'timeout': int(args.get('wait.timeout', 60)),
            'file_change': int(args.get('wait.file_change', 30)),
            'sleep': int(args.get('wait.sleep', 30)),
        }
        self.outputStar = self.join('tilt_series.star')
        self.badTiltsStar = self.join('bad_tilts.star')
        self.tsFolder = args["tilt_images"]
        self.mdocPattern = args['mdoc_files']
        self.tiltAxisAngle = args['tilt_axis_angle']
        self.minTilts = max(1, int(args.get('min_tilts', 1) or 1))

    @staticmethod
    def _checkTiltImage(framesPath, refDims):
        """ Check that the tilt image exists and can be read.

        Args:
            framesPath: path to the tilt movie (EER, TIFF or MRC).
            refDims: (x, y) of the first good tilt of the series, or None.

        Returns:
            (dims, reason, error): (x, y, n) for a good image (reason None),
            or None, the reason ('missing' or 'corrupted') and an error
            message for a bad one.
        """
        if not os.path.exists(framesPath):
            return None, 'missing', 'File not found'
        if os.path.getsize(framesPath) == 0:
            return None, 'corrupted', 'Empty file'
        try:
            dims = Image.get_dimensions(framesPath)
        except Exception as e:
            return None, 'corrupted', f"{type(e).__name__}: {e}"
        if not dims:
            return None, 'corrupted', 'Could not read the image dimensions'
        x, y = dims[0], dims[1]
        n = dims[2] if len(dims) > 2 else 1
        if refDims and (x, y) != refDims:
            return None, 'corrupted', (f"Dimensions {x}x{y} differ from "
                                       f"{refDims[0]}x{refDims[1]}")
        return (x, y, n), None, None

    @staticmethod
    def _cleanError(msg):
        """ Make the error message a valid single STAR value. """
        return ' '.join(str(msg).replace('"', "'").split())[:200] or 'Unknown'

    def _writeBadTilts(self):
        """ Write all bad-tilts tables, one per tilt series (tomoName). """
        tmpStar = self.badTiltsStar + '.tmp'
        with StarFile(tmpStar, 'w') as sfOut:
            for tsName, badTable in self.badTilts.items():
                sfOut.writeTable(tsName, badTable,
                                 computeFormat="left",
                                 timeStamp=True)
        os.replace(tmpStar, self.badTiltsStar)

    def _output(self, batch):
        tsName = batch['tsName']
        mdoc = batch['mdoc']
        rows = []
        badRows = []
        preExposure = 0
        refDims = None
        N = 0
        # There are mdocs where the ZValue index does not match the acquisition order
        # We will sort the zvalues by date and assign the index accordingly
        for i, (z, s) in enumerate(mdoc.zsections(sort='date')):
            movieName = mdoc.getSubFrameBase(s)
            framesPath = os.path.join(self.tsFolder, movieName)
            dims, reason, error = self._checkTiltImage(framesPath, refDims)
            if reason:
                badRows.append(dict(
                    rlnTomoTiltMovieIndex=i + 1,  # acquisition order
                    rlnMicrographMovieName=framesPath,
                    rlnTomoNominalStageTiltAngle=s['TiltAngle'],
                    rlnMicrographPreExposure='%0.3f' % preExposure,
                    emwMdocZValue=str(z).split('=')[-1].strip(),
                    emwBadTiltReason=reason,
                    emwBadTiltError=self._cleanError(error)
                ))
                self.log(f"{tsName}: {Color.warn(reason.upper())} tilt "
                         f"{s['TiltAngle']}: {framesPath} ({error})")
            else:
                refDims = refDims or dims[:2]
                N += 1
                rows.append(dict(
                    # Acquisition order index, so skipped tilts leave a gap
                    rlnTomoTiltMovieIndex=i + 1,
                    rlnMicrographMovieName=framesPath,
                    rlnTomoTiltMovieFrameCount=dims[2],
                    rlnTomoNominalStageTiltAngle=s['TiltAngle'],
                    rlnTomoNominalTiltAxisAngle=self.tiltAxisAngle,
                    rlnMicrographPreExposure='%0.3f' % preExposure,
                    rlnTomoNominalDefocus=s['TargetDefocus'],
                    #emwMdocPriorRecordDose=s['PriorRecordDose'],
                    #emwMdocDateTime=s['DateTime']
                ))
            # The specimen received the dose even if the image is bad
            preExposure += self.acq.total_dose

        if badRows:
            self.badTilts[tsName] = Table.fromDict(badRows)
        else:
            self.badTilts.pop(tsName, None)  # it may be fixed after a rerun
        if badRows or tsName in self._badTiltsLoaded:
            self._writeBadTilts()

        if N < self.minTilts:
            self.log(f"{tsName}: {Color.warn('EXCLUDED')}, only {N} good tilts "
                     f"of {N + len(badRows)} (min_tilts = {self.minTilts})")
            return

        if badRows:
            self.log(f"{tsName}: imported {N} good tilts, "
                     f"{len(badRows)} bad tilts skipped")

        mdocFile = self.join('mdocs', f'{tsName}.mdoc')
        # Write a copy of the mdoc file without the bad tilts: WarpTools
        # ts_import fails for the whole series if a frame series is missing
        for row in badRows:
            mdoc.pop(f"ZValue = {row['emwMdocZValue']}", None)
        mdoc.write(mdocFile)
        # Write the TS star file
        tsStarFile = self.join('tilt_series', f"{tsName}.star")

        with StarFile(tsStarFile, 'w') as sfOut:
            tsTable = Table.fromDict(rows)
            sfOut.writeTable(tsName, tsTable,
                             computeFormat="left",
                             timeStamp=True)

        ps = self.acq.pixel_size
        rowValues = dict(
            rlnTomoName=tsName,
            rlnTomoTiltSeriesStarFile=tsStarFile,
            rlnVoltage=self.acq.voltage,
            rlnSphericalAberration=self.acq.cs,
            rlnAmplitudeContrast=self.acq.amplitude_contrast,
            rlnMicrographOriginalPixelSize=ps,
            rlnTomoHand=-1,
            rlnOpticsGroupName='optics_group1',
            rlnTomoMdocFile=mdocFile
        )
        if gain := self.acq.get('gain'):
            rowValues['rlnMicrographGainName'] = gain
        if total_dose := self.acq.get('total_dose'):
            rowValues['rlnMicrographDoseRate'] = total_dose

        if self.allTsTable is None:
            self.allTsTable = Table.fromDict([rowValues])
        else:
            self.allTsTable.addRowValues(**rowValues)
        
        # Write to a temporary file first, the output is read on-the-fly
        # (e.g. by WarpOTF) and should never be seen partially written
        tmpStar = self.outputStar + '.tmp'
        with StarFile(tmpStar, 'w') as sfOut:
            sfOut.writeTable('global', self.allTsTable,
                             computeFormat="left",
                             timeStamp=True)
        os.replace(tmpStar, self.outputStar)
        self.writeRelionOutputNodes([[self.outputStar, 'TomogramGroupMetadata.star.emwrap.frameseries']])          
        self.writeInfo()

    def prerun(self):
        previousTs = set()
        self.allTsTable = None
        # Bad (missing or corrupted) tilts, one table per tilt series
        self.badTilts = {}

        # Load already seen movies if we are continuing the job
        if os.path.exists(self.outputStar):
            # Read values as strings, guessed float columns would not format
            # the str values of the rows added in this run
            self.allTsTable = StarFile.getTableFromFile('global', self.outputStar,
                                                        guessType=False)
            previousTs.update(row.rlnTomoName for row in self.allTsTable)
        else:
            self.mkdir('tilt_series')
            self.mkdir('mdocs')

        # Imported TS keep their bad tilts; excluded ones (below min_tilts)
        # are not in previousTs, so they are checked again in this run
        if os.path.exists(self.badTiltsStar):
            self.badTilts = StarFile.getTablesDict(self.badTiltsStar,
                                                   guessType=False)
        self._badTiltsLoaded = set(self.badTilts)

        self.log(f">>>> STARTING RUN: ")
        self.log(f"  - Mdocs pattern: {Color.cyan(self.mdocPattern)}")
        self.log(f"  - Input TS folder:  {Color.cyan(self.tsFolder)}")
        self.log(f"  - Min tilts per TS: {Color.cyan(self.minTilts)}")
        self.log(f"  - TS from previous run: {Color.cyan(len(previousTs))}")

        batchMgr = MdocBatchManager(self.mdocPattern, self.tmpDir,
                                    moviesPath=self.tsFolder,
                                    blacklist=previousTs,
                                    createBatch=False,
                                    timeout=self.wait['timeout'],
                                    wait=self.wait['sleep'],
                                    fileChange=self.wait['file_change'],
                                    checkMovies=True)
        g = self.addGenerator(batchMgr.generate)
        self.addProcessor(g.outputQueue, self._output)


if __name__ == '__main__':
    ImportTsPipeline.main()



