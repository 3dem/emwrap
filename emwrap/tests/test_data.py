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

import argparse
import os
import sys
import unittest

from emtools.utils import Color

from emwrap.tests.emdb_downloader import EmdbDownloader
from emwrap.tests.empiar_downloader import EmpiarDownloader


class TestData(unittest.TestCase):
    """ Special class for downloading test datasets for automated tests. """

    datasets_map = {
        'WarpApofTutorial': {
            'empiar_id': 10491,
            'tilt_series': [1, 11, 17, 23, 32],
            # Apoferritin reference map, used to build the PyTOM template
            'emdb_id': 15854,
        },
        'RelionTomoTutorial': {
            'empiar_id': 10164,
            # RELION STA tutorial: TS_01, TS_03, TS_43, TS_45, TS_54
            'tilt_series': ['01', '03', '43', '45', '54'],
        }
    }

    @classmethod
    def get_root(cls):
        """Return ROOT (directory containing emwrap.bashrc)."""
        root = os.environ.get('ROOT')
        if not root:
            raise EnvironmentError(
                'ROOT is not set. Source emwrap.bashrc before using test data.')
        return root

    @classmethod
    def get_testdata_root(cls):
        """Return the testdata folder next to emwrap.bashrc."""
        return os.path.join(cls.get_root(), 'testdata')

    @classmethod
    def get_dataset_path(cls, name, validate=False):
        """Return the path to a named dataset under testdata/."""
        if name not in cls.datasets_map:
            known = ', '.join(sorted(cls.datasets_map))
            raise ValueError(f"Unknown test dataset {name!r}. Known datasets: {known}")

        path = os.path.join(cls.get_testdata_root(), name)
        if validate:
            if not os.path.exists(path):
                raise FileNotFoundError(
                    f"Test data folder does not exist: {path}. "
                    f"Download it with: python -m emwrap.tests data -d {name} "
                    f"{cls.get_testdata_root()}")
            if not os.path.isdir(path):
                raise NotADirectoryError(
                    f"Test data path is not a directory: {path}")
        return path

    @classmethod
    def _download_WarpApofTutorial(cls, path):
        """Download WarpApofTutorial dataset."""
        dataset = cls.datasets_map['WarpApofTutorial']
        tilt_series = dataset['tilt_series']

        print(f"Downloading EMPIAR-{dataset['empiar_id']} tilt series: {tilt_series}")
        with EmpiarDownloader(dataset['empiar_id']) as downloader:
            downloader.download_file('gain_ref.mrc', path)

            mdoc_dir = os.path.join(path, 'mdoc')
            frames_dir = os.path.join(path, 'frames')
            for ts in tilt_series:
                print(f"Downloading TS_{ts}")
                downloader.download_file(
                    f'tiltseries/mdoc/TS_{ts}.mrc.mdoc', mdoc_dir)
                downloader.download_pattern(
                    'tiltseries/data', f'*-{ts}_*.tif', frames_dir)

        # Reference map used by emw-pytom-create_template to build the
        # template and mask for template matching.
        print(f"Downloading EMD-{dataset['emdb_id']} map")
        with EmdbDownloader(dataset['emdb_id']) as downloader:
            downloader.download_map(path)

    @classmethod
    def _download_RelionTomoTutorial(cls, path):
        """Download RelionTomoTutorial dataset."""
        dataset = cls.datasets_map['RelionTomoTutorial']
        tilt_series = dataset['tilt_series']

        print(f"Downloading EMPIAR-{dataset['empiar_id']} tilt series: "
              f"{[f'TS_{ts}' for ts in tilt_series]}")
        with EmpiarDownloader(dataset['empiar_id']) as downloader:
            mdoc_dir = os.path.join(path, 'mdoc')
            frames_dir = os.path.join(path, 'frames')
            for ts in tilt_series:
                print(f"Downloading TS_{ts}")
                downloader.download_file(
                    f'mdoc-files/TS_{ts}.mrc.mdoc', mdoc_dir)
                downloader.download_pattern(
                    'frames', f'TS_{ts}_*.mrc', frames_dir)

    @classmethod
    def list_datasets(cls):
        """List all available test datasets."""
        print(Color.bold(">>> Available datasets:"))
        for name in cls.datasets_map.keys():
            print(Color.green(f"  {name}"))

    @classmethod
    def download_dataset(cls, name, download_root=None):
        """Download a single test dataset."""
        if name not in cls.datasets_map:
            raise ValueError(f"Dataset {name} not found")

        if download_root is None:
            download_root = cls.get_testdata_root()
        download_path = os.path.join(download_root, name)
        print(f"Downloading dataset {name} to {download_path}")
        if not os.path.exists(download_path):
            os.makedirs(download_path)
        download_method = getattr(cls, f"_download_{name}")
        download_method(download_path)

    @classmethod
    def run_from_args(cls, args):
        """Download test datasets for automated tests."""
        if args.list:
            cls.list_datasets()
        elif args.download:
            if len(args.download) == 1:
                name = args.download[0]
                download_root = None
            else:
                name, download_root = args.download
            cls.download_dataset(name, download_root=download_root)

    @classmethod
    def set_args(cls, parser):
        parser.add_argument(
            '--list', '-l', action='store_true',
            help='List all available test datasets.')
        parser.add_argument(
            '--download', '-d', metavar=('NAME', 'PATH'), nargs='*',
            help='Download a test dataset to PATH/NAME (default: $ROOT/testdata/NAME).')

    @classmethod
    def get_args(cls):
        parser = cls.get_parser()
        return parser.parse_args()