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
from glob import glob
from datetime import datetime
from collections import defaultdict

from emtools.utils import FolderManager, Path
from emtools.jobs import Args
from emtools.metadata import StarFile, Table, WarpXml
from emtools.image import Image

from .warp import WarpBasePipeline


class WarpMotionCtf(WarpBasePipeline):
    """ Warp wrapper to run Motion correction and CTF estimation.
    It will run:
        - create_settings -> frame_series.setting
        - fs_motion_and_ctf
    """
    name = 'emw-warp-mctf'

    def get_float(self, key, defaultValue):
        if v := self._args.get(key, None):
            return float(v)
        return defaultValue

    def targetPs(self, inputPs):
        v = self._args.get('create_settings.bin_angpix', '') or 0
        return float(v) or inputPs

    def _create_settings(self, batch, kwargs):
        """ This method should only be called the first time the pipeline is run. 
        It will make the import from previous WARP run and create the settings file.
        If it is a continue mode (frameseries.settings exists), it is not needed to run this method.
        """
        framesFm = FolderManager(batch.join('frames'))
        framesFm.create()

        mdocsFm = FolderManager(batch.join('mdocs'))
        mdocsFm.create()

        batch.mkdir(self.FS)

        ext = None
        ps = None
        dims = None
        N = None
        eer = False

        # Input movies pattern for the frame series
        inputTsStar = kwargs['inputTs']
        tsAllTable = StarFile.getTableFromFile('global', inputTsStar)

        for tsRow in tsAllTable:
            tsName = tsRow.rlnTomoName
            ps = tsRow.rlnMicrographOriginalPixelSize
            tsTable = StarFile.getTableFromFile(tsName, tsRow.rlnTomoTiltSeriesStarFile)
            mdocsFm.link(tsRow.rlnMdocFile)
            N = len(tsTable)
            for frameRow in tsTable:
                frameBase = framesFm.link(frameRow.rlnMicrographMovieName)
                # Calculate extension only once
                if ext is None:
                    ext = Path.getExt(frameBase)
                    dims = Image.get_dimensions(frameRow.rlnMicrographMovieName)

        x, y, n = dims
        self.inputs = {
            'FrameSeries': {
                'label': 'Frame Series',
                'type': 'FrameSeries',
                'info': f"{len(tsAllTable)} items, {x} x {y} x {n} x {N}, {ps:0.3f} Å/px",
                'files': [
                    [inputTsStar, 'TomogramGroupMetadata.star.relion.tomo.import']
                ]
            }
        }

        if gain := self.acq.get('gain', None):
            self.log(f"{self.name}: Linking gain file: {gain}")
            self.link(gain)

        cs = 'create_settings'  # shortcut

        if ext == '.eer':
            ngroups = int(self.get_float(f'{cs}.eer_ngroups', 0))
            if not ngroups:
                # TODO: Maybe provide a wizard to calculate the number of eer_groups
                raise Exception("Input frames are in eer and you must provide "
                                "the number of eer_groups")
            eer = True
        else:
            ngroups = n

        # Run create_settings
        args = Args({
            'WarpTools': cs,
            '--folder_data': 'frames',
            '--extension': f"*{ext}",
            '--folder_processing': self.FS,
            '--output': self.FSS,
            '--angpix': ps,
            '--exposure': self.acq['total_dose']
        })  
        tPs = self.targetPs(ps)

        if tPs > ps:
            args['--bin_angpix'] = tPs

        if self.gain:
            args['--gain_path'] = self.gain

        if eer:
            args['--eer_ngroups'] = ngroups

        # TODO: Allow for some extra args

        self.batch_execute('create_settings', batch, args)
        return ngroups

    def runBatch(self, batch, **kwargs):
        """ This method can be run for only the Mctf pipeline
         or for the preprocessing one, where import inputs is not needed.
        """
        if not self.exists(self.FSS):
            self.log("There are no settings, importing files from previous run and creating settings file...")
            ngroups = self._create_settings(batch, kwargs)
        else:
            self.log("There are settings, reading from file...")
            warpXml = WarpXml(self.join(self.FSS))
            d = warpXml.getDict('Settings', 'Import', 'Param')
            ngroups = -1 * int(d['EERGroupFrames'])

        # Run fs_motion_and_ctf
        args = Args({
            'WarpTools': 'fs_motion_and_ctf',
            '--settings': self.FSS,
            '--m_grid': f'1x1x{ngroups}',  # FIXME: Read m_grid from params
            '--c_grid': '2x2x1',  # FIXME: Read c_grid option
            '--c_voltage': int(self.acq.voltage),
            '--c_cs': self.acq.cs,
            '--c_amplitude': self.acq.amplitude_contrast,
            '--out_averages': "",  # We always generate averages, if not the job will fail
        })
        if self.gpuList:
            args['--device_list'] = self.gpuList
        # if pd := subargs['perdevice']:
        #     args['--perdevice'] = int(pd)

        args.update(self.get_subargs('fs_motion_and_ctf'))

        self.batch_execute('fs_motion_and_ctf', batch, args)
        self.updateBatchInfo(batch)

    def _output(self, batch):
        """ Register output STAR files. """

        def _float(v):
            return round(float(v), 2)

        batch.mkdir('tilt_series')
        self.log("Registering output STAR files.")
        tsAllTable = StarFile.getTableFromFile('global', self.inputTs)

        newTsStarFile = batch.join('tilt_series_ctf.star')
        failedStarFile = batch.join('tilt_series_failed.star')
        newPs = None
        n = None
        dims = None

        newPsLabel = 'rlnTomoTiltSeriesPixelSize'
        newTsAllTable = Table(tsAllTable.getColumnNames() + [newPsLabel])
        failedTable = Table(newTsAllTable.getColumnNames())

        for tsRow in tsAllTable:
            tsName = tsRow.rlnTomoName
            tsStarFile = self.join('tilt_series', tsName + '.star')
            ps = tsRow.rlnMicrographOriginalPixelSize
            if newPs is None:
                newPs = self.targetPs(ps)

            tsTable = StarFile.getTableFromFile(tsName, tsRow.rlnTomoTiltSeriesStarFile)
            n = len(tsTable)

            # Each input movie must have xml + average mrc (same idea as WarpAreTomo
            # requiring aligned stack per TS). Collect missing before building output.
            missing = []
            for frameRow in tsTable:
                moviePrefix = Path.removeBaseExt(frameRow.rlnMicrographMovieName)
                movieMrc = moviePrefix + '.mrc'
                movieXml = batch.join(self.FS, moviePrefix + '.xml')
                movieAvgMrc = batch.join(self.FS, 'average', movieMrc)
                if not os.path.exists(movieXml):
                    missing.append((moviePrefix, 'xml', movieXml))
                if not os.path.exists(movieAvgMrc):
                    missing.append((moviePrefix, 'average mrc', movieAvgMrc))

            tsDict = tsRow._asdict()
            tsDict.update({
                newPsLabel: newPs,
                'rlnTomoTiltSeriesStarFile': tsStarFile
            })

            if missing:
                for moviePrefix, reason, path in missing:
                    self.log(f"ERROR: Missing {reason} for movie {moviePrefix}: {path}")
                tsDict['rlnTomoTiltSeriesStarFile'] = "None"
                failedTable.addRowValues(**tsDict)
                continue
            # FIXME: Do not add even/odd when this option is not selected
            extra_cols = [
                'rlnCtfPowerSpectrum', 'rlnMicrographName', 'rlnMicrographMetadata',
                'rlnAccumMotionTotal', 'rlnAccumMotionEarly', 'rlnAccumMotionLate',
                'rlnMicrographNameEven', 'rlnMicrographNameOdd', 'rlnCtfImage',
                'rlnDefocusU', 'rlnDefocusV', 'rlnCtfAstigmatism', 'rlnDefocusAngle',
                'rlnCtfFigureOfMerit', 'rlnCtfMaxResolution', 'rlnCtfIceRingDensity',
            ]

            filesMap = {
                'rlnMicrographName': 'average',
                'rlnCtfPowerSpectrum': 'powerspectrum',
                'rlnCtfImage': 'powerspectrum',
                'rlnMicrographNameEven': 'average/even',
                'rlnMicrographNameOdd': 'average/odd'
            }
            newTsTable = Table(tsTable.getColumnNames() + extra_cols)
            for frameRow in tsTable:
                moviePrefix = Path.removeBaseExt(frameRow.rlnMicrographMovieName)
                movieMrc = moviePrefix + '.mrc'
                frameDict = frameRow._asdict()
                for k, v in filesMap.items():
                    frameDict[k] = batch.join(self.FS, v, movieMrc)
                frameDict['rlnMicrographMetadata'] = "None"

                avgMrcPath = frameDict['rlnMicrographName']
                if dims is None and os.path.exists(avgMrcPath):
                    dims = Image.get_dimensions(avgMrcPath)

                movieXml = batch.join(self.FS, moviePrefix + '.xml')
                defocusDict = defaultdict(lambda: 0)

                # xml and average mrc already validated for whole TS above
                ctf = WarpXml(movieXml).getDict('Movie', 'CTF', 'Param')
                defocusDict['rlnDefocusU'] = _float(ctf['Defocus'])
                defocusDict['rlnCtfAstigmatism'] = _float(ctf['DefocusDelta'])
                defocusDict['rlnDefocusV'] = _float(defocusDict['rlnDefocusU'] + defocusDict['rlnCtfAstigmatism'])
                defocusDict['rlnDefocusAngle'] = _float(ctf['DefocusAngle'])

                for k in extra_cols:
                    if k.startswith('rlnAccumMotion'):
                        # FIXME: Parse the movie values
                        frameDict[k] = 0
                    elif k.startswith('rlnDefocus') or k.startswith('rlnCtf') and k not in frameDict:
                        frameDict[k] = defocusDict[k]

                newTsTable.addRowValues(**frameDict)
            # Write the new ts.star file
            self.write_ts_table(tsName, newTsTable, tsStarFile)
            newTsAllTable.addRowValues(**tsDict)

        # Write the corrected_tilt_series.star
        self.write_ts_table('global', newTsAllTable, newTsStarFile)
        if dims is None:
            x, y = 0, 0
        else:
            x, y = dims[0], dims[1]
        self.outputs = {
            'TiltSeries': {
                'label': 'Tilt Series',
                'type': 'TiltSeries',
                'info': f"{len(newTsAllTable)} items, {x} x {y} x {n}, {newPs:0.3f} Å/px",
                'files': [
                    [newTsStarFile, 'TomogramGroupMetadata.star.relion.tomo.import']
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

        self.updateBatchInfo(batch)

    def prerun(self):
        self.prerunTs()


if __name__ == '__main__':
    WarpMotionCtf.main()
