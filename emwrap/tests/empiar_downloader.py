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

import glob
import os

from .downloader import BaseDownloader

EMPIAR_FTP_HOST = 'ftp.ebi.ac.uk'
EMPIAR_FTP_ROOT = '/empiar/world_availability'


class EmpiarDownloader(BaseDownloader):
    """Download a subset of files from an EMPIAR dataset using wget."""

    def __init__(self, empiar_id, host=EMPIAR_FTP_HOST):
        self.empiar_id = empiar_id
        self.host = host
        self._base_url = f'ftp://{host}{EMPIAR_FTP_ROOT}/{empiar_id}/data'

    def download_file(self, relative_path, dest_dir, skip_existing=True):
        """Download one file relative to the dataset data/ folder."""
        filename = os.path.basename(relative_path)
        dest_path = os.path.join(dest_dir, filename)
        os.makedirs(dest_dir, exist_ok=True)

        url = f'{self._base_url}/{relative_path}'
        print(f"  Downloading {relative_path} -> {dest_path}")
        self._wget(url, dest_dir, skip_existing=skip_existing)
        return dest_path

    def download_pattern(self, relative_dir, pattern, dest_dir, skip_existing=True):
        """Download files in relative_dir whose names match a shell-style pattern."""
        os.makedirs(dest_dir, exist_ok=True)
        url = f'{self._base_url}/{relative_dir}/{pattern}'
        print(f"  Downloading files matching {pattern} -> {dest_dir}")
        self._wget(url, dest_dir, skip_existing=skip_existing)
        downloaded = sorted(glob.glob(os.path.join(dest_dir, pattern)))
        if not downloaded:
            raise FileNotFoundError(
                f"No files matching {pattern!r} in EMPIAR-{self.empiar_id}:{relative_dir}")
        return downloaded
