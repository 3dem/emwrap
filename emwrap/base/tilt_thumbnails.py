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

from emtools.image import TiltThumbnails, tilt_qc


class TiltThumbnailWriter:
    """ Writes the thumbnails of the tilts a job processes into
    <job>/tilt_series/thumbnails (emtools.image.TiltThumbnails), for any
    program wrapper: what each wrapper reads from its program's output goes
    into the RELION tilt-series STAR file, and these files are made from it.

    Made in the job, right after the program wrote each MRC, so a dashboard
    never has to read one while acquisition is using the disk.  A failure
    only logs: a missing thumbnail must not fail the job.
    """
    def __init__(self, pipeline):
        self.folder = pipeline.join(TiltThumbnails.FOLDER)
        self.log = pipeline.log

    def tilt(self, avgPath):
        """ <average>.jpg and <average>_medium.jpg of a tilt average. """
        try:
            TiltThumbnails.write_tilt(avgPath, self.folder)
        except Exception as e:
            self.log(f"WARNING: could not write thumbnail for {avgPath}: {e}")

    def ps(self, psPath, makeView):
        """ <ps>_ps.jpg and <ps>_ctf.json of a power spectrum; makeView()
        returns (uint8 array, profile dict), see TiltThumbnails.write_ps. """
        try:
            TiltThumbnails.write_ps(psPath, makeView, self.folder)
        except Exception as e:
            self.log(f"WARNING: could not write CTF fit view for {psPath}: {e}")

    def imageQc(self, tsName, tsTable):
        """ <tsName>_imageqc.json: the image content measures of a tilt
        series (emtools tilt_qc), from its tilt jpegs, once they are all
        written.  Skipped when up to date. """
        try:
            tilts = tilt_qc.tilts_from_star_rows([row._asdict() for row in tsTable],
                                                 ctf_correlation=False, thumbnails=self.folder)
            tilt_qc.write_series_json(tilt_qc.json_path(self.folder, tsName), tsName, tilts)
        except Exception as e:
            self.log(f"WARNING: could not write image QC for TS {tsName}: {e}")
