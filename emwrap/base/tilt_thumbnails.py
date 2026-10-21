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
import re
import json
import threading

from emtools.utils import Path
from emtools.image import Image, TiltThumbnails


class TiltThumbnailWriter:
    """ The jpegs, and CTF fit profiles, of the tilts of a tilt series.

    Whatever program processed the tilts, the job that wrote a tilt's
    average and power spectrum writes them into <job>/tilt_series/thumbnails:

        <average>.jpg           TiltThumbnails.SIZE px: filmstrip, and what
                                TiltQualityCuration measures
        <average>_medium.jpg    TiltThumbnails.MEDIUM_SIZE px: a closer look
        <ps>_ps.jpg             TiltThumbnails.PS_SIZE px: CTF fit view
        <ps>_ctf.json           its radial profiles and their correlation
        <tomoName>_imageqc.json the image content measures of the tilt series

    <average> and <ps> are the base names of the MRCs.  Readers (emhub OTF
    dashboard, TiltQualityCuration) find the folder from the path of the
    MRC: folder().  The images themselves are made by
    emtools.image.TiltThumbnails.

    An instance writes them for the job of a pipeline, for any program
    wrapper: what each wrapper reads from its program's output goes into
    the RELION tilt-series STAR file, and these files are made from it.
    Made in the job, right after the program wrote each MRC, so a dashboard
    never has to read one while acquisition is using the disk.  A failure
    only logs: a missing thumbnail must not fail the job.
    """
    FOLDER = os.path.join('tilt_series', 'thumbnails')
    QUALITY = 80
    _JOB_DIR = re.compile(r'^job\d+$')

    def __init__(self, pipeline, thumbnailsDir=None):
        """ thumbnailsDir: where to write, by default FOLDER in the
        pipeline's job folder, where readers look for them (folder()). """
        self.thumbnailsDir = thumbnailsDir or pipeline.join(self.FOLDER)
        self.log = pipeline.log

    # ------------------------------------------------------------------
    # Paths
    # ------------------------------------------------------------------
    @classmethod
    def folder(cls, path):
        """ The thumbnails folder of the job folder (jobNNN) a file is in,
        relative or absolute as path is; None if it is not in one. """
        dirs = os.path.normpath(path).split(os.sep)[:-1]
        for i in range(len(dirs), 0, -1):
            if cls._JOB_DIR.match(dirs[i - 1]):
                return os.path.join(os.sep.join(dirs[:i]) or os.sep, cls.FOLDER)
        return None

    @classmethod
    def _file(cls, srcPath, suffix, folder=None):
        """ The thumbnail file of srcPath, in folder or its job's, or None. """
        folder = folder or cls.folder(srcPath)
        return os.path.join(folder, Path.removeBaseExt(srcPath) + suffix) if folder else None

    @classmethod
    def tilt_jpeg(cls, avgPath, folder=None, medium=False):
        return cls._file(avgPath, '_medium.jpg' if medium else '.jpg', folder)

    @classmethod
    def ps_jpeg(cls, psPath, folder=None):
        return cls._file(psPath, '_ps.jpg', folder)

    @classmethod
    def ctf_json(cls, psPath, folder=None):
        return cls._file(psPath, '_ctf.json', folder)

    # ------------------------------------------------------------------
    # Files
    # ------------------------------------------------------------------
    @staticmethod
    def tmp_path(path):
        """ A temporary file beside path, unique per process and thread, for
        writing then renaming, so a reader never gets a partial file. """
        return f'{path}.{os.getpid()}.{threading.get_ident()}.tmp'

    @classmethod
    def save_jpeg(cls, pilImage, path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = cls.tmp_path(path)
        pilImage.save(tmp, format='JPEG', quality=cls.QUALITY)
        os.replace(tmp, path)

    @classmethod
    def save_json(cls, data, path):
        """ The temporary file is created with open(), so the folder's
        default ACL applies. """
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = cls.tmp_path(path)
        with open(tmp, 'w') as f:
            json.dump(data, f)
        os.replace(tmp, path)

    @staticmethod
    def _current(path, srcPath):
        return os.path.exists(path) and os.path.getmtime(path) >= os.path.getmtime(srcPath)

    @classmethod
    def _required(cls, path, srcPath):
        if path is None:
            raise ValueError(f"{srcPath} is not in a job folder: give the thumbnails folder")
        return path

    @classmethod
    def write_tilt(cls, avgPath, folder=None, medium=True):
        """ Write the jpegs of a tilt average (and the medium one) that are
        older than it; the MRC is read once, only if one is.  Returns the
        number written. """
        data, written = None, 0
        sizes = ((False, TiltThumbnails.SIZE), (True, TiltThumbnails.MEDIUM_SIZE))
        for isMedium, size in sizes[:1 + medium]:
            path = cls._required(cls.tilt_jpeg(avgPath, folder, isMedium), avgPath)
            if not cls._current(path, avgPath):
                if data is None:
                    data = Image.get_array(avgPath)
                cls.save_jpeg(TiltThumbnails.tilt_image(data, size), path)
                written += 1
        return written

    @classmethod
    def write_ps(cls, psPath, makeView, folder=None):
        """ Write the CTF fit view of a power spectrum, unless it is newer
        than it.  makeView() -> (uint8 array, profile dict): made by each
        program wrapper, as the spectrum and the fit are stored differently
        by each program; the profile has 'k' (1/Å), 'measured',
        'simulated' and 'correlation'.  Returns True if written. """
        jpeg = cls._required(cls.ps_jpeg(psPath, folder), psPath)
        jsonPath = cls.ctf_json(psPath, folder)
        if cls._current(jpeg, psPath) and cls._current(jsonPath, psPath):
            return False
        arr, profile = makeView()
        cls.save_json(profile, jsonPath)
        cls.save_jpeg(TiltThumbnails.ps_image(arr), jpeg)
        return True

    @classmethod
    def read_ctf_correlation(cls, psPath, folder=None):
        """ The CTF profile correlation of a power spectrum, or None. """
        try:
            with open(cls.ctf_json(psPath, folder)) as f:
                return json.load(f).get('correlation')
        except (OSError, ValueError, AttributeError, TypeError):
            return None

    # ------------------------------------------------------------------
    # Writing them for a job (never fails it)
    # ------------------------------------------------------------------
    def tilt(self, avgPath):
        """ <average>.jpg and <average>_medium.jpg of a tilt average. """
        try:
            self.write_tilt(avgPath, self.thumbnailsDir)
        except Exception as e:
            self.log(f"WARNING: could not write thumbnail for {avgPath}: {e}")

    def ps(self, psPath, makeView):
        """ <ps>_ps.jpg and <ps>_ctf.json of a power spectrum; makeView()
        returns (uint8 array, profile dict), see write_ps. """
        try:
            self.write_ps(psPath, makeView, self.thumbnailsDir)
        except Exception as e:
            self.log(f"WARNING: could not write CTF fit view for {psPath}: {e}")

    def imageQc(self, tsName, tsTable):
        """ <tsName>_imageqc.json: the image content measures of a tilt
        series (TiltQualityCuration), from its tilt jpegs, once they are all
        written.  Skipped when up to date. """
        # Imported here: TiltQualityCuration finds its files with this class
        from emwrap.base.tilt_quality_curation import TiltQualityCuration as tqc
        try:
            tilts = tqc.tilts_from_star_rows([row._asdict() for row in tsTable],
                                             ctf_correlation=False, thumbnails=self.thumbnailsDir)
            tqc.write_series_json(tqc.json_path(self.thumbnailsDir, tsName), tsName, tilts)
        except Exception as e:
            self.log(f"WARNING: could not write image QC for TS {tsName}: {e}")
