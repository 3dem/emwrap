# **************************************************************************
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
import threading
from glob import glob

from emtools.utils import Color, Path
from emtools.metadata import StarFile, StarMonitor, Table, WarpXml
from emtools.image import Image

from emwrap.base import ProcessingPipeline
from emwrap.base.tilt_thumbnails import TiltThumbnailWriter
from emwrap.base.tilt_quality_curation import TiltQualityCuration as tqc


class TiltAnalyzePipeline(ProcessingPipeline):
    """ Analyze the tilts of motion corrected tilt series, on the fly: the
    tilt jpegs, the CTF fit views and the image content measures that the
    OTF dashboard shows and the tilt curation job (emw-tilt-curation)
    applies its rule to.

    Input: TiltSeries (or TiltSeriesAligned), followed as the job that
    writes it adds tilt series.  For each tilt series, once it is in the
    input, its jpegs and CTF fit views are completed and its image measures
    written (<tomoName>_imageqc.json).  When the input job is Warp's, a
    background thread also follows its tilt averages and power spectra, so
    the jpegs and CTF views of a tilt are made as soon as Warp writes it,
    before its tilt series is complete.

    Everything goes into <job>/thumbnails (see TiltThumbnailWriter).  The
    output, tilt_analysis.star (TiltSeriesAnalysis), points to it:

        data_analysis   one row: the job folder, the thumbnails folder and
                        the input tilt series
        data_global     one row per analyzed tilt series: its name, its
                        tilt series STAR file (the input's) and its image
                        measures json
    """
    name = 'emw-tilt-analyze'
    OUTPUT_STAR = 'tilt_analysis.star'
    OUTPUT_TYPE = 'TomogramGroupMetadata.star.emwrap.TiltSeriesAnalysis'
    GLOBAL_COLUMNS = ['rlnTomoName', 'rlnTomoTiltSeriesStarFile', 'emwImageQcFile', 'emwTilts']
    # A tilt average or power spectrum not modified for this long is taken as
    # completely written by the program making it
    SETTLE_SECONDS = 10

    def __init__(self, args, output):
        ProcessingPipeline.__init__(self, args, output)
        self.inputTs = args['input_tiltseries']
        self.medium = str(args.get('medium_jpegs', True)).strip().lower() not in ('false', '0', 'no', '')
        self.threads = max(1, int(args.get('threads', 4) or 1))
        self.wait = {
            'timeout': int(args.get('wait.timeout', 3600)),
            'sleep': int(args.get('wait.sleep', 30)),
        }
        self.writer = TiltThumbnailWriter(self)
        self.outputStar = self.join(self.OUTPUT_STAR)
        # The Warp frame series folder of the input job, when it is Warp's:
        # its averages and power spectra are followed by the tilt watcher
        # (an on the fly Warp job may create it after this job starts)
        self.warpFsDir = os.path.join(os.path.dirname(self.inputTs), 'warp_frameseries')
        self._stopWatcher = threading.Event()
        self._watcher = None
        self._watched = {}       # path -> mtime of the source when made
        self._outputLock = threading.Lock()

    # ------------------------------------------------------------------
    # The images of one tilt
    # ------------------------------------------------------------------
    def _writeTilt(self, avgPath):
        """ The jpegs of a tilt average; returns the number written. """
        n = self.writer.write_tilt(avgPath, tqc.JPEG_SIZE, self.writer.thumbnailsDir)
        if self.medium:
            n += self.writer.write_tilt(avgPath, tqc.MEDIUM_JPEG_SIZE,
                                        self.writer.thumbnailsDir, medium=True)
        return n

    @staticmethod
    def warpXmlPath(psPath):
        """ The Warp xml of a power spectrum of a Warp frame series folder
        (warp_frameseries/powerspectrum/<movie>.mrc -> warp_frameseries/<movie>.xml),
        None if there is none. """
        fsDir = os.path.dirname(os.path.dirname(psPath))
        xml = os.path.join(fsDir, Path.removeBaseExt(psPath) + '.xml')
        return xml if os.path.exists(xml) else None

    @staticmethod
    def warpCtfViewMaker(psPath, xmlPath):
        """ The CTF fit view of a Warp tilt (see warpCtfView), as the
        thumbnail writer takes it: a function returning (image, profile). """
        from emwrap.warp.utils import warpCtfView

        def _make():
            xml = WarpXml(xmlPath)
            arr, profile = warpCtfView(Image.get_array(psPath),
                                       xml.getDict('Movie', 'CTF', 'Param'),
                                       xml.getDict('Movie', 'OptionsCTF', 'Param'))
            res = xml.get('Movie', '@CTFResolutionEstimate')
            profile['fitResolution'] = float(res) if res else None
            return arr, profile
        return _make

    def _writePs(self, psPath):
        """ The CTF fit view of a power spectrum, for the programs it can be
        made for (Warp); returns True if written. """
        if xml := self.warpXmlPath(psPath):
            return self.writer.write_ps(psPath, self.warpCtfViewMaker(psPath, xml),
                                        tqc.PS_JPEG_SIZE, self.writer.thumbnailsDir)
        return False

    # ------------------------------------------------------------------
    # The tilt watcher: the images of each tilt as soon as it is written
    # ------------------------------------------------------------------
    def _settled(self, path):
        """ The mtime of path if it is complete and not made yet, else None. """
        try:
            mtime = os.path.getmtime(path)
        except OSError:
            return None
        if time.time() - mtime < self.SETTLE_SECONDS or self._watched.get(path) == mtime:
            return None
        return mtime

    def _watchOnce(self):
        """ Make the images of the tilts written since the last check. """
        made = 0
        sources = [(p, self._writeTilt) for p in glob(os.path.join(self.warpFsDir, 'average', '*.mrc'))]
        sources += [(p, self._writePs) for p in glob(os.path.join(self.warpFsDir, 'powerspectrum', '*.mrc'))]
        for path, write in sources:
            if self._stopWatcher.is_set():
                break
            if (mtime := self._settled(path)) is None:
                continue
            try:
                made += int(write(path))
                self._watched[path] = mtime
            except Exception as e:
                # Most likely still being written (or its xml): tried again
                # in the next check, and by the tilt series once it is in
                self.log(f"WARNING: could not make the images of {path} yet: {e}")
        return made

    def _watchTilts(self):
        following = False
        while True:
            try:
                if not following and os.path.isdir(self.warpFsDir):
                    following = True
                    self.log(f"Following the Warp tilts in {Color.bold(self.warpFsDir)}")
                if following and (made := self._watchOnce()):
                    self.log(f"Tilt watcher: made {Color.green(made)} images", flush=True)
            except Exception as e:
                self.log(f"WARNING: tilt watcher: {e}")
            if self._stopWatcher.wait(self.wait['sleep']):
                break

    # ------------------------------------------------------------------
    # Per tilt series
    # ------------------------------------------------------------------
    def _newTiltSeries(self):
        """ The tilt series of the input not analyzed yet, as they come. """
        blacklist = []
        if os.path.exists(self.outputStar):
            blacklist = StarFile.getTableFromFile('global', self.outputStar, guessType=False)
            self.log(f"Previously analyzed tilt series: {Color.cyan(len(blacklist))}")
        monitor = StarMonitor(self.inputTs, 'global', lambda row: row.rlnTomoName,
                              timeout=self.wait['timeout'], wait=self.wait['sleep'],
                              blacklist=blacklist, tableKwargs={'guessType': False})
        for row in monitor.newItems():
            yield {'tsName': row.rlnTomoName, 'tsStar': row.rlnTomoTiltSeriesStarFile}

    def _analyze(self, task):
        """ Complete the images of the tilts of a tilt series and write its
        image measures. """
        tsName, tsStar = task['tsName'], task['tsStar']
        try:
            table = StarFile.getTableFromFile(tsName, tsStar, guessType=False)
            made = 0
            for row in table:
                if (avg := getattr(row, 'rlnMicrographName', '')) and os.path.exists(avg):
                    made += self._writeTilt(avg)
                if (ps := getattr(row, 'rlnCtfPowerSpectrum', '')) and os.path.exists(ps):
                    made += int(self._writePs(ps))
            tqc.write_table_json(self.writer.thumbnailsDir, tsName, table)
            task.update(tilts=len(table), made=made,
                        qcJson=tqc.json_path(self.writer.thumbnailsDir, tsName))
        except Exception as e:
            task['error'] = str(e)
        return task

    def _writeOutput(self, task):
        """ Add an analyzed tilt series to tilt_analysis.star. """
        tsName = task['tsName']
        if error := task.get('error'):
            self.log(f"ERROR: could not analyze TS {tsName}: {error}")
            return task
        with self._outputLock:
            table = Table(self.GLOBAL_COLUMNS)
            if os.path.exists(self.outputStar):
                table = StarFile.getTableFromFile('global', self.outputStar, guessType=False)
            table.addRowValues(rlnTomoName=tsName, rlnTomoTiltSeriesStarFile=task['tsStar'],
                               emwImageQcFile=task['qcJson'], emwTilts=task['tilts'])
            self._writeOutputStar(table)
        self.log(f"Analyzed TS {Color.bold(tsName)}: {task['tilts']} tilts, "
                 f"{task['made']} images made here", flush=True)
        return task

    def _writeOutputStar(self, globalTable):
        analysis = Table(['emwTiltAnalysisFolder', 'emwThumbnailsFolder', 'emwInputTiltSeries'])
        analysis.addRowValues(emwTiltAnalysisFolder=self.outputPrefix,
                              emwThumbnailsFolder=self.writer.thumbnailsDir,
                              emwInputTiltSeries=self.inputTs)
        tmp = self.outputStar + '.tmp'
        with StarFile(tmp, 'w') as sf:
            sf.writeTable('analysis', analysis, singleRow=True)
            sf.writeTable('global', globalTable, computeFormat='left', timeStamp=True)
        os.replace(tmp, self.outputStar)
        if not self.exists('RELION_OUTPUT_NODES.star'):
            self.writeRelionOutputNodes([[self.fixOutputPath(self.OUTPUT_STAR), self.OUTPUT_TYPE]])

    # ------------------------------------------------------------------
    # Job
    # ------------------------------------------------------------------
    def prerun(self):
        self.log(f"Input tilt series: {Color.bold(self.inputTs)}"
                 + ('' if os.path.exists(self.inputTs) else ' (not written yet, waiting for it)'))
        self.log(f"Thumbnails folder: {Color.bold(self.writer.thumbnailsDir)}")
        self.inputs = {'input_tiltseries': self.inputTs}
        self.outputs = {'type': 'TiltSeriesAnalysis', 'star': self.outputStar}
        self.writeInfo()

        self._watcher = threading.Thread(target=self._watchTilts, daemon=True)
        self._watcher.start()

        g = self.addGenerator(self._newTiltSeries)
        outputQueue = None
        for _ in range(self.threads):
            p = self.addProcessor(g.outputQueue, self._analyze, outputQueue=outputQueue)
            outputQueue = p.outputQueue
        self.addProcessor(outputQueue, self._writeOutput)

    def postrun(self):
        if self._watcher:
            self._stopWatcher.set()
            self._watcher.join()
        n = 0
        if os.path.exists(self.outputStar):
            n = len(StarFile.getTableFromFile('global', self.outputStar, guessType=False))
        self.log(f"Analyzed {Color.green(n)} tilt series: {Color.cyan(self.outputStar)}")


if __name__ == '__main__':
    TiltAnalyzePipeline.main()
