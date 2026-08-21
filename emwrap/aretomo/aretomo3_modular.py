"""Modular AreTomo3 pipelines for pre-corrected tilt series.

The full ``emw-aretomo3`` pipeline remains the Cmd 0 implementation.  These
pipelines stage RELION5 metadata as the file triplets consumed by Cmd 1/2.
"""

import os
import shutil

import mrcfile
import numpy as np

from emtools.image import Image
from emtools.metadata import StarFile, Table
from emtools.utils import FolderManager

from .aretomo3_pipeline import AreTomo3Pipeline
from .utils import create_dummy_edf_file


def _stack_path(value):
    """Return the MRC path from a RELION ``N@stack`` image reference."""
    value = str(value)
    return value.split('@', 1)[1] if '@' in value else value


def _stack_index(value):
    value = str(value)
    if '@' not in value:
        return None
    try:
        return int(value.split('@', 1)[0]) - 1
    except ValueError:
        return None


class Aretomo3ModularBase(AreTomo3Pipeline):
    """Shared Cmd 1/2 staging helpers and normal AreTomo3 result handling."""

    def newTargetTsPs(self, input_ps):
        # These jobs start after motion correction, so McBin is irrelevant.
        return float(input_ps)

    @staticmethod
    def _pixel_size(row):
        value = getattr(row, 'rlnTomoTiltSeriesPixelSize', None)
        if value in (None, ''):
            value = row.rlnMicrographOriginalPixelSize
        return float(value)

    def _read_series(self, ts_name, row):
        path = row.rlnTomoTiltSeriesStarFile
        if not os.path.exists(path):
            raise ValueError(f'{ts_name}: per-tilt STAR file not found: {path}')
        return StarFile.getTableFromFile(ts_name, path)

    def _write_tlt(self, path, table, use_aligned_angles=False):
        with open(path, 'w') as handle:
            for index, row in enumerate(table, start=1):
                angle = getattr(row, 'rlnTomoYTilt', '') if use_aligned_angles else ''
                if angle in ('', None):
                    angle = getattr(row, 'rlnTomoNominalStageTiltAngle', '')
                if angle in ('', None):
                    raise ValueError(f'Missing tilt angle at row {index}')
                handle.write(f'{float(angle):.6f} {index}\n')

    def _write_stack_from_images(self, path, table, image_column='rlnMicrographName'):
        """Compose an MRC stack from one per-tilt RELION image column."""
        frames = []
        for index, row in enumerate(table, start=1):
            image = getattr(row, image_column, '')
            if not image:
                raise ValueError(f'Missing {image_column} at row {index}')
            source = _stack_path(image)
            if not os.path.exists(source):
                raise ValueError(f'Missing input image at row {index}: {source}')
            with mrcfile.open(source, permissive=True) as mrc:
                data = np.asarray(mrc.data)
                stack_index = _stack_index(image)
                if stack_index is not None:
                    if data.ndim != 3 or not 0 <= stack_index < data.shape[0]:
                        raise ValueError(f'Invalid stack reference at row {index}: {image}')
                    data = data[stack_index]
                elif data.ndim == 3:
                    if data.shape[0] != 1:
                        raise ValueError(
                            f'Input image at row {index} is a stack; use an N@stack reference.')
                    data = data[0]
                if data.ndim != 2:
                    raise ValueError(f'Input image at row {index} is not two-dimensional: {source}')
                frames.append(np.array(data, dtype=np.float32, copy=True))
        if not frames:
            raise ValueError('Cannot build an empty tilt-series stack.')
        shape = frames[0].shape
        if any(frame.shape != shape for frame in frames):
            raise ValueError('All input tilt images must have identical dimensions.')
        with mrcfile.new(path, overwrite=True) as mrc:
            mrc.set_data(np.stack(frames))

    @staticmethod
    def _has_complete_image_column(table, image_column):
        """True only when every input tilt has a usable source image."""
        if image_column not in table.getColumnNames():
            return False
        return all(getattr(row, image_column, '') not in ('', None) for row in table)

    def _stage_stack_and_tlt(self, batch, ts_name, row, aligned_angles=False):
        table = self._read_series(ts_name, row)
        stack = batch.join(f'{ts_name}.mrc')
        tlt = batch.join(f'{ts_name}_TLT.txt')
        self._write_stack_from_images(stack, table)
        self._write_tlt(tlt, table, use_aligned_angles=aligned_angles)
        return table, stack, tlt

    def _copy_result(self, result, ts_name, include_tilt=True):
        ts_folder = self._getOutputTsFolder(ts_name)
        ts_folder.create()
        tom_folder = None

        def copy_file(key, folder):
            source = result.get(key)
            if source and os.path.exists(source):
                destination = folder.join(os.path.basename(source))
                if os.path.abspath(source) != os.path.abspath(destination):
                    shutil.copy2(source, destination)
                result[key] = destination

        if include_tilt:
            for key in ('rlnTiltSeriesAligned', 'rlnTiltSeriesAlignedOdd',
                        'rlnTiltSeriesAlignedEvn', 'at3TomoAlignmentFile',
                        'at3MappingFile', 'at3TomoCtfFile', 'rlnCtfImage',
                        'at3MetricsCsv', 'at3TimeStampCsv'):
                copy_file(key, ts_folder)
            for key in ('rlnTiltSeriesAligned', 'rlnTiltSeriesAlignedOdd',
                        'rlnTiltSeriesAlignedEvn', 'rlnCtfImage'):
                source = result.get(key)
                if source and source.lower().endswith('.mrc'):
                    link = os.path.splitext(source)[0] + '.mrcs'
                    if os.path.lexists(link):
                        os.remove(link)
                    os.symlink(os.path.basename(source), link)
                    result[key] = link

        if result.get('rlnTomoReconstructedTomogram'):
            tom_folder = self._getOutputTomFolder(ts_name)
            tom_folder.create()
            for key in ('rlnTomoReconstructedTomogram', 'rlnTomoNameOdd',
                        'rlnTomoNameEvn', 'at3ThicknessMrc', 'at3ThicknessCsv'):
                copy_file(key, tom_folder)
        return result

