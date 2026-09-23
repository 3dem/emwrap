"""Modular AreTomo3 pipelines for pre-corrected tilt series.

The full ``emw-aretomo3`` pipeline remains the Cmd 0 implementation.  These
pipelines stage RELION5 metadata as the file triplets consumed by Cmd 1/2.
"""

import os
import shutil

import mrcfile
import numpy as np

from emtools.image import Image
from emtools.jobs import TsStarBatchManager
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

    # @staticmethod
    # def _as_bool(value):
    #     return str(value).lower() in ('1', 'true', 'yes', 'y', 'on')

    @staticmethod
    def _write_aretomo3_ctf(path, table):
        """Write AreTomo3 CTF input rows from RELION per-tilt metadata."""
        required = ('rlnDefocusU', 'rlnDefocusV', 'rlnDefocusAngle')
        with open(path, 'w') as handle:
            handle.write(
                '# Columns: #1 micrograph number; #2 - defocus 1 [A]; '
                '#3 - defocus 2; #4 - azimuth of astigmatism; '
                '#5 - additional phase shift [radian]; #6 - cross correlation; '
                '#7 - spacing (in Angstroms) up to which CTF rings were fit '
                'successfully; #8 - dfHand\n'
            )
            for index, row in enumerate(table, start=1):
                if any(getattr(row, key, '') in ('', None) for key in required):
                    raise ValueError(
                        f'CTF correction requires {", ".join(required)} at row {index}'
                    )
                fom = getattr(row, 'rlnCtfFigureOfMerit', 0) or 0
                resolution = getattr(row, 'rlnCtfMaxResolution', 0) or 0
                handle.write(
                    f'{index} {float(row.rlnDefocusU):.6f} '
                    f'{float(row.rlnDefocusV):.6f} '
                    f'{float(row.rlnDefocusAngle):.6f} 0 '
                    f'{float(fom):.6f} {float(resolution):.6f} 0\n'
                )

    def newTargetTsPs(self, input_ps):
        # These jobs start after motion correction, so McBin is irrelevant.
        return float(input_ps)

    def _registeredTsPs(self, ts_row):
        return self._pixel_size(ts_row)

    def _output_directories(self):
        return (self.outputTsDir,)

    def _include_tilt_outputs(self):
        return True

    def _input_row(self, ts_name):
        return next(row for row in self.inputTsTable if row.rlnTomoName == ts_name)

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
        return StarFile.getTableFromFile(ts_name, path, guessType=False, types={'rlnTomoNominalStageTiltAngle': float})

    # TODO: use_algined_angles should be revised carefully before using them 
    def _write_tlt(self, path, table, use_aligned_angles=False):
        # AreTomo3 infers acquisition order from line order, not from a
        # written index, so the input .tlt must only carry the angle. Any
        # gaps in rlnTomoTiltMovieIndex (e.g. after emw-subset-ts removes a
        # tilt) would otherwise break AreTomo3's own index expectations.
        table.sort(key='rlnTomoNominalStageTiltAngle')
        with open(path, 'w') as handle:
            for row in table:
                index = getattr(row, 'rlnTomoTiltMovieIndex', '')
                angle = getattr(row, 'rlnTomoYTilt', '') if use_aligned_angles else ''
                if angle in ('', None):
                    angle = getattr(row, 'rlnTomoNominalStageTiltAngle', '')
                if angle in ('', None):
                    raise NameError(f'Missing tilt angle at row {index}')
                if index in ('', None):
                    raise ValueError(f'Missing order of acquisition at angle {angle}')

                handle.write(f'{float(angle):.6f}\n')

    def _write_rln_index_map(self, path, table):
        """Persist the mapping between AreTomo3's sequential per-tilt index
        (1-based, in the same angle-sorted order used to build the .mrc
        stack and .tlt file) and the original rlnTomoTiltMovieIndex.
        AreTomo3 outputs (*_TLT.txt, *_CTF.txt, *.aln) number rows
        sequentially in stack order, which no longer matches
        rlnTomoTiltMovieIndex once tilt images have been removed
        (e.g. via emw-subset-ts). This file lets us map results back to the
        correct row in the individual tilt-series STAR file.
        """
        table.sort(key='rlnTomoNominalStageTiltAngle')
        with open(path, 'w') as handle:
            handle.write('# at3_index rlnTomoTiltMovieIndex\n')
            for at3Index, row in enumerate(table, start=1):
                rlnIndex = getattr(row, 'rlnTomoTiltMovieIndex', '')
                handle.write(f'{at3Index} {rlnIndex}\n')

    def _write_stack_from_images(self, path, table, image_column='rlnMicrographName'):
        """Compose an MRC stack from one per-tilt RELION image column."""
        frames = []
        table.sort(key='rlnTomoNominalStageTiltAngle')
        for row in table:
            image = getattr(row, image_column, '')
            index = getattr(row, 'rlnTomoTiltMovieIndex', '')
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
        idxMap = batch.join(f'{ts_name}_at3_rln_idx.txt')
        self._write_stack_from_images(stack, table)
        self._write_tlt(tlt, table, use_aligned_angles=aligned_angles)
        self._write_rln_index_map(idxMap, table)
        return table, stack, tlt

    def _resolve_previous_alignment(self, ts_name, required=('stack', 'tlt', 'aln')):
        """Resolve the previous AreTomo3 Cmd 1 outputs for a given tilt series.
        The previous job output tree is expected to contain the series folder
        at tilt_series/<TS_NAME>/ alongside the input aligned tilt-series STAR.
        """
        star_file = self._args.get('input_tiltseries', '')
        if not star_file:
            raise FileNotFoundError(
                f'{ts_name}: Cannot resolve previous AreTomo3 alignment without '
                'input_tiltseries.'
            )

        input_dir = os.path.dirname(os.path.abspath(str(star_file)))
        candidate = os.path.join(input_dir, 'tilt_series', ts_name)
        filenames = {
            'stack': f'{ts_name}.mrc',
            'tlt': f'{ts_name}_TLT.txt',
            'aln': f'{ts_name}.aln',
            'ctf': f'{ts_name}_CTF.txt',
            'ctf_stack': f'{ts_name}_CTF.mrc',
            'idx_map': f'{ts_name}_at3_rln_idx.txt',
        }
        expected = ', '.join(filenames[name] for name in required)
        files = {
            'stack': os.path.join(candidate, filenames['stack']),
            'tlt': os.path.join(candidate, filenames['tlt']),
            'aln': os.path.join(candidate, filenames['aln']),
            'ctf': os.path.join(candidate, filenames['ctf']),
            'ctf_stack': os.path.join(candidate, filenames['ctf_stack']),
            # Optional: only present for previous runs that already persisted it.
            'idx_map': os.path.join(candidate, filenames['idx_map']),
        }
        missing = [name for name in required if not os.path.exists(files[name])]
        if not missing:
            return files
        raise FileNotFoundError(
            f'{ts_name}: Could not find prior AreTomo3 alignment files in the expected '
            f' folder {candidate}. Expected: '
            f'{expected}.'
        )

    def _stage_previous_alignment(self, batch, ts_name,
                                  required=('stack', 'tlt'), include_half_sets=True):
        resolved = self._resolve_previous_alignment(ts_name, required=required)
        staged = {}

        for key in required:
            filename = os.path.basename(resolved[key])
            destination = batch.join(filename)
            if os.path.abspath(resolved[key]) != os.path.abspath(destination):
                shutil.copy2(resolved[key], destination)
            staged[key] = destination

        # idx_map is optional: older runs may not have persisted it.
        if 'idx_map' not in required and os.path.exists(resolved.get('idx_map', '')):
            destination = batch.join(os.path.basename(resolved['idx_map']))
            if os.path.abspath(resolved['idx_map']) != os.path.abspath(destination):
                shutil.copy2(resolved['idx_map'], destination)
            staged['idx_map'] = destination

        if include_half_sets:
            for suffix, key in (('_ODD', 'odd'), ('_EVN', 'evn')):
                source = os.path.join(os.path.dirname(resolved['stack']),
                                      f'{ts_name}{suffix}.mrc')
                if os.path.exists(source):
                    destination = batch.join(os.path.basename(source))
                    if os.path.abspath(source) != os.path.abspath(destination):
                        shutil.copy2(source, destination)
                    staged[key] = destination

        return resolved, staged

    def _copy_result(self, result, ts_name, include_tilt=True):
        ts_folder = self._getOutputTsFolder(ts_name)
        ts_folder.create()
        tom_folder = None

        if include_tilt:
            for key in ('rlnTiltSeriesAligned', 'rlnTiltSeriesAlignedOdd',
                        'rlnTiltSeriesAlignedEvn', 'at3TomoAlignmentFile',
                        'at3MappingFile', 'at3TomoCtfFile', 'rlnCtfImage',
                        'at3MetricsCsv', 'at3TimeStampCsv', 'at3RlnIndexMapFile'):
                self._copy_result_file(result, key, ts_folder)
            for key in ('rlnTiltSeriesAligned', 'rlnTiltSeriesAlignedOdd',
                        'rlnTiltSeriesAlignedEvn', 'rlnCtfImage'):
                self._link_result_stack_as_mrcs(result, key)

        if result.get('rlnTomoReconstructedTomogram'):
            tom_folder = self._getOutputTomFolder(ts_name)
            tom_folder.create()
            for key in ('rlnTomoReconstructedTomogram', 'rlnTomoNameOdd',
                        'rlnTomoNameEvn', 'at3ThicknessMrc', 'at3ThicknessCsv'):
                self._copy_result_file(result, key, tom_folder)
        return result

    def _output(self, batch):
        ts_name = batch['tsName']
        if batch.error:
            self._allResults[ts_name] = {'error': batch.error}
        else:
            result = batch['results'][0] if batch['results'] else {}
            self._allResults[ts_name] = self._copy_result(
                result, ts_name, include_tilt=self._include_tilt_outputs())

        batch.info['tsName'] = ts_name
        self._registerOutputs()
        self.updateBatchInfo(batch)
        return batch

    def prerun(self):
        self.inputTsTable = self._getInputTsTable()
        self.inputTs = self._args['input_tiltseries']
        self.log(f"Input tilt-series: {len(self.inputTsTable)}", flush=True)

        if self.registerOnly:
            self._register_existing_final_outputs()
            return

        for directory in self._output_directories():
            self.mkdir(directory)

        batchMgr = TsStarBatchManager(self.inputTsTable, self.tmpDir)
        generator = self.addGenerator(batchMgr.generate)
        self.addGpuProcessors(generator, self.get_aretomo3_proc, self._output)

