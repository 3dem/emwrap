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

import gzip
import os
import shutil

from .downloader import BaseDownloader

EMDB_HOST = 'https://ftp.ebi.ac.uk'
EMDB_ROOT = '/pub/databases/emdb/structures'


class EmdbDownloader(BaseDownloader):
    """Download files from an EMDB entry (https://www.ebi.ac.uk/emdb) using wget.

    Each entry is published under EMD-<id> with the density map in
    'map/emd_<id>.map.gz', the metadata in 'header/emd-<id>.xml' and the
    optional extras in 'images', 'masks' and 'other'. Maps are always stored
    gzipped, so download_map() decompresses them on arrival.
    """

    def __init__(self, emdb_id, host=EMDB_HOST):
        self.emdb_id = self._parse_id(emdb_id)
        self.host = host
        self._base_url = f'{host}{EMDB_ROOT}/EMD-{self.emdb_id}'

    @staticmethod
    def _parse_id(emdb_id):
        """Accept 15854, '15854' or 'EMD-15854' and return the bare number."""
        value = str(emdb_id).strip().upper()
        if value.startswith('EMD-'):
            value = value[len('EMD-'):]
        if not value.isdigit():
            raise ValueError(f"Invalid EMDB id: {emdb_id!r}. "
                             f"Expected a number or 'EMD-<number>'.")
        return value

    @property
    def map_name(self):
        """File name of the entry's density map, once decompressed."""
        return f'emd_{self.emdb_id}.map'

    def download_file(self, relative_path, dest_dir, skip_existing=True):
        """Download one file relative to the entry folder.

        For example 'map/emd_15854.map.gz' or 'header/emd-15854.xml'.
        """
        filename = os.path.basename(relative_path)
        dest_path = os.path.join(dest_dir, filename)
        os.makedirs(dest_dir, exist_ok=True)

        url = f'{self._base_url}/{relative_path}'
        print(f"  Downloading {relative_path} -> {dest_path}")
        self._wget(url, dest_dir, skip_existing=skip_existing)

        if not os.path.exists(dest_path):
            raise FileNotFoundError(
                f"{relative_path!r} not found in EMD-{self.emdb_id} ({url})")
        return dest_path

    def download_map(self, dest_dir, skip_existing=True):
        """Download the entry's density map, decompressed to emd_<id>.map."""
        os.makedirs(dest_dir, exist_ok=True)
        map_path = os.path.join(dest_dir, self.map_name)

        if skip_existing and os.path.exists(map_path):
            print(f"  {self.map_name} already exists, skipped")
            return map_path

        gz_path = self.download_file(f'map/{self.map_name}.gz', dest_dir,
                                     skip_existing=skip_existing)

        # Decompress into a temporary file first, so an interrupted run does
        # not leave behind a truncated map that would then be skipped.
        print(f"  Decompressing {self.map_name}.gz -> {map_path}")
        tmp_path = f'{map_path}.tmp'
        try:
            with gzip.open(gz_path, 'rb') as fin, open(tmp_path, 'wb') as fout:
                shutil.copyfileobj(fin, fout)
            os.replace(tmp_path, map_path)
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

        os.remove(gz_path)
        return map_path
