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


import os
import shlex
import shutil
import stat
from glob import glob

from emtools.image import Image
from emtools.jobs import Args, Batch
from emtools.metadata import StarFile, Table, WarpXml
from emtools.utils import FolderManager

from emwrap.warp.warp import WarpBasePipeline


class MissAlignment(WarpBasePipeline):
    """Run Miss-Alignment on a Warp project produced by coarse TS alignment.

    Expected job arguments
    ----------------------
    input_tiltseries
        Global STAR produced by the coarse Warp/AreTomo alignment job.
    config_file
        Miss-Alignment training or inference YAML template.
    mode
        ``train`` (default) or ``infer``.
    gpus
        EMHub GPU selection.  When provided, it is exported as
        ``CUDA_VISIBLE_DEVICES`` by the generated runner.

    Train-only arguments
    --------------------
    training_devices
        Logical CUDA device IDs passed to ``--training-devices``. Default: 0.
    reconstruction_devices
        Logical CUDA device IDs passed to ``--reconstruction-devices``.
        Default: 0,0,0.
    dataloaders_per_trainer
        Value for ``--dataloaders-per-trainer``. Default: 5.

    Infer-only arguments
    --------------------
    model_run_directory
        Finished Miss-Alignment training run containing iterN/model.ckpt.

    Common execution arguments
    --------------------------
    start_at_iteration
        Value for ``--start-at-iteration``. Default: 0.
    prepare_stacks
        Value for ``--prepare-stacks`` in Angstrom/pixel. Default: 10.0.
    omp_num_threads, mkl_num_threads
        Thread limits exported by the runner. Both default to 1.
    extra_miss_alignment
        Extra command-line arguments appended verbatim to the command.

    XML geometry arguments
    ----------------------
    xml.image_size_x, xml.image_size_y
        Original tilt-image dimensions in pixels. If omitted, they are inferred
        from the first movie referenced by the input STAR.
    xml.volume_size_x, xml.volume_size_y, xml.volume_size_z
        Tomogram dimensions in pixels. If omitted, values are read from
        warp_tiltseries.settings.
    xml.pixel_size
        Original pixel size in Angstrom/pixel. If omitted, it is read from the
        input global STAR.
    """
    
    name = 'emw-missalignment'
    PROGRAM = 'MISS_ALIGNMENT'

    CONFIG_NAME = 'miss_alignment_config.yaml'
    PREPARE_SCRIPT = 'prepare_miss_alignment.py' # TODO: what is this script for? It is not defined in the code provided.
    RUNNER_SCRIPT = 'run_miss_alignment.sh'
    OUTPUT_STAR = 'miss_aligned_tilt_series.star'

    def _get_launcher(self):
        """Use the launcher that activates both Miss-Alignment and WarpTools."""
        return self.get_launcher_arg('launcher_miss_alignment', self.PROGRAM)

    @staticmethod
    def _positive_int(value, name):
        try:
            result = int(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f'{name} must be an integer, received: {value!r}') from exc
        if result <= 0:
            raise ValueError(f'{name} must be greater than zero, received: {result}')
        return result

    @staticmethod
    def _nonnegative_int(value, name):
        try:
            result = int(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f'{name} must be an integer, received: {value!r}') from exc
        if result < 0:
            raise ValueError(f'{name} must be zero or greater, received: {result}')
        return result

    @staticmethod
    def _positive_float(value, name):
        try:
            result = float(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f'{name} must be numeric, received: {value!r}') from exc
        if result <= 0:
            raise ValueError(f'{name} must be greater than zero, received: {result}')
        return result

    @classmethod
    def _device_csv(cls, value, name):
        """Normalize a list, comma-separated string, or space-separated string."""
        if isinstance(value, (list, tuple)):
            values = list(value)
        else:
            text = str(value or '').strip().replace(',', ' ')
            values = text.split()
        if not values:
            raise ValueError(f'{name} must contain at least one CUDA device ID.')
        devices = [cls._nonnegative_int(item, name) for item in values]
        return ','.join(str(device) for device in devices)

    def _mode(self):
        mode = str(self._args.get('mode', 'train')).strip().lower()
        if mode not in {'train', 'infer'}:
            raise ValueError(f"mode must be 'train' or 'infer', received: {mode!r}")
        return mode

    def _ensure_project_inputs(self, input_folder):
        """Import the Warp project once and preserve it on resumed executions."""
        expected = [self.join(self.FS), self.join(self.FSS), self.join(self.TS),
                    self.join(self.TSS), self.join(self.TM)]
        if all(os.path.exists(path) for path in expected):
            self.log('Using the existing local Warp project for resume/re-registration.')
            return

        present = [path for path in expected if os.path.exists(path)]
        if present:
            raise RuntimeError(
                'The local Warp project is only partially populated. Remove the partial '
                f'job output or restore the missing inputs. Existing paths: {present}'
            )

        # Keep all relative paths used by tomostar/XML metadata valid. Frameseries is
        # linked, settings are copied, and mutable tilt-series/tomostar metadata is copied.
        self._importInputs(input_folder, keys=['fs', 'fss', 'ts', 'tss', 'tm'])

    def _dataset_geometry(self):
        """Resolve image shape, volume shape, and pixel size for XML preparation."""
        global_table = StarFile.getTableFromFile('global', self.inputTs)
        if len(global_table) == 0:
            raise ValueError(f'Input tilt-series STAR is empty: {self.inputTs}')

        first = global_table[0]
        pixel_size = first.rlnTomoTiltSeriesPixelSize
        pixel_size = self._positive_float(pixel_size, 'xml.pixel_size')

        
        ts_table = StarFile.getTableFromFile(first.rlnTomoName, first.rlnTomoTiltSeriesStarFile)
        if len(ts_table) == 0:
            raise ValueError(f'Tilt-series metadata is empty: {first.rlnTomoTiltSeriesStarFile}')
        
        movie_file = ts_table[0].rlnMicrographMovieName
        dims = Image.get_dimensions(movie_file)
        image_x = dims[0]
        image_y = dims[1]
        self.log(
            f'Inferred original image dimensions from {movie_file}: '
            f'{image_x} x {image_y}'
        )

        settings_dims = WarpXml(self.join(self.TSS)).getDict('Settings', 'Tomo', 'Param')
        
        volume_x = settings_dims['DimensionsX']
        volume_y = settings_dims['DimensionsY']
        volume_z = settings_dims['DimensionsZ']

        geometry = {
            'image_x': self._positive_int(image_x, 'xml.image_size_x'),
            'image_y': self._positive_int(image_y, 'xml.image_size_y'),
            'volume_x': self._positive_int(volume_x, 'xml.volume_size_x'),
            'volume_y': self._positive_int(volume_y, 'xml.volume_size_y'),
            'volume_z': self._positive_int(volume_z, 'xml.volume_size_z'),
            'pixel_size': pixel_size,
        }
        self.log(
            'Miss-Alignment XML geometry: '
            f"image={geometry['image_x']}x{geometry['image_y']}, "
            f"volume={geometry['volume_x']}x{geometry['volume_y']}x"
            f"{geometry['volume_z']}, pixel_size={geometry['pixel_size']} A/px"
        )
        return geometry

    def _write_prepare_script(self, batch):
        """Write a helper executed inside the Miss-Alignment Conda environment."""
        script_path = batch.join(self.PREPARE_SCRIPT)
        script = r'''#!/usr/bin/env python
        import argparse
        from pathlib import Path

        import torch
        import yaml
        from warpylib import TiltSeries


        def parse_args():
            parser = argparse.ArgumentParser()
            parser.add_argument('--data-directory', required=True)
            parser.add_argument('--config-input', required=True)
            parser.add_argument('--config-output', required=True)
            parser.add_argument('--mode', choices=('train', 'infer'), required=True)
            parser.add_argument('--model-run-directory')
            parser.add_argument('--image-x', type=int, required=True)
            parser.add_argument('--image-y', type=int, required=True)
            parser.add_argument('--volume-x', type=int, required=True)
            parser.add_argument('--volume-y', type=int, required=True)
            parser.add_argument('--volume-z', type=int, required=True)
            parser.add_argument('--pixel-size', type=float, required=True)
            return parser.parse_args()


        def main():
            args = parse_args()
            data_directory = Path(args.data_directory).resolve()
            xml_files = sorted(data_directory.glob('*.xml'))
            if not xml_files:
                raise RuntimeError(f'No Warp tilt-series XML files found in {data_directory}')

            image_physical = torch.tensor(
                [args.image_x * args.pixel_size, args.image_y * args.pixel_size],
                dtype=torch.float32,
            )
            volume_physical = torch.tensor(
                [
                    args.volume_x * args.pixel_size,
                    args.volume_y * args.pixel_size,
                    args.volume_z * args.pixel_size,
                ],
                dtype=torch.float32,
            )

            for xml_file in xml_files:
                tilt_series = TiltSeries(xml_file)
                tilt_series.image_dimensions_physical = image_physical.clone()
                tilt_series.volume_dimensions_physical = volume_physical.clone()
                tilt_series.save_meta(xml_file)

            config_input = Path(args.config_input).resolve()
            config_output = Path(args.config_output).resolve()
            with config_input.open('r', encoding='utf-8') as handle:
                config = yaml.safe_load(handle)
            if not isinstance(config, dict):
                raise TypeError(f'Expected a YAML mapping in {config_input}')

            if args.mode == 'train':
                config['training_directory'] = str(data_directory)
            else:
                if not args.model_run_directory:
                    raise ValueError('--model-run-directory is required in inference mode')
                config['data_directory'] = str(data_directory)
                config['model_run_directory'] = str(Path(args.model_run_directory).resolve())

            config_output.parent.mkdir(parents=True, exist_ok=True)
            with config_output.open('w', encoding='utf-8') as handle:
                yaml.safe_dump(config, handle, sort_keys=False)

            print(f'Updated {len(xml_files)} Warp XML files.')
            print(f'Wrote Miss-Alignment config: {config_output}')


        if __name__ == '__main__':
            main()
        '''
        with open(script_path, 'w', encoding='utf-8') as handle:
            handle.write(script)
        os.chmod(script_path, os.stat(script_path).st_mode | stat.S_IXUSR)
        return script_path

    # TODO: Question what is the 'prepare_miss_alignment' command for? It is not defined in the code provided.
    def _prepare_project(self, batch, mode, geometry):
        config_input = self._args.get('config_file', '')
        if not config_input:
            raise ValueError('config_file is required.')
        config_input = os.path.abspath(str(config_input))
        if not os.path.isfile(config_input):
            raise FileNotFoundError(f'Miss-Alignment config file not found: {config_input}')

        config_output = os.path.abspath(self.join(self.TS, self.CONFIG_NAME))
        prepare_script = os.path.abspath(self._write_prepare_script(batch))

        args = Args({
            'python': prepare_script,
            '--data-directory': os.path.abspath(self.join(self.TS)),
            '--config-input': config_input,
            '--config-output': config_output,
            '--mode': mode,
            '--image-x': geometry['image_x'],
            '--image-y': geometry['image_y'],
            '--volume-x': geometry['volume_x'],
            '--volume-y': geometry['volume_y'],
            '--volume-z': geometry['volume_z'],
            '--pixel-size': geometry['pixel_size'],
        })

        if mode == 'infer':
            model_run_directory = self._args.get('model_run_directory', '')
            if not model_run_directory:
                raise ValueError('model_run_directory is required in inference mode.')
            model_run_directory = os.path.abspath(str(model_run_directory))
            if not os.path.isdir(model_run_directory):
                raise FileNotFoundError(
                    f'Miss-Alignment model run directory not found: {model_run_directory}'
                )
            args['--model-run-directory'] = model_run_directory

        self.batch_execute(
            'prepare_miss_alignment', batch, args, launcher=self._get_launcher()
        )
        return config_output

    def _command_tokens(self, mode, config_file):
        start_iteration = self._nonnegative_int(
            self._args.get('start_at_iteration', 0), 'start_at_iteration'
        )
        prepare_stacks = self._positive_float(
            self._args.get('prepare_stacks', 10.0), 'prepare_stacks'
        )

        tokens = [
            'miss-alignment', mode,
            '--config-file', config_file,
            '--start-at-iteration', str(start_iteration),
            '--prepare-stacks', str(prepare_stacks),
        ]

        if mode == 'train':
            training_devices = self._device_csv(
                self._args.get('training_devices', '0') or '0',
                'training_devices',
            )
            reconstruction_devices = self._device_csv(
                self._args.get('reconstruction_devices', '0,0,0') or '0,0,0',
                'reconstruction_devices',
            )
            dataloaders = self._positive_int(
                self._args.get('dataloaders_per_trainer', 5),
                'dataloaders_per_trainer',
            )
            tokens.extend([
                '--training-devices', training_devices,
                '--reconstruction-devices', reconstruction_devices,
                '--dataloaders-per-trainer', str(dataloaders),
            ])

        extra = str(self._args.get('extra_miss_alignment', '') or '').strip()
        if extra:
            tokens.extend(shlex.split(extra))
        return tokens

    def _write_runner(self, batch, mode, config_file):
        runner_path = batch.join(self.RUNNER_SCRIPT)
        omp_threads = self._positive_int(
            self._args.get('omp_num_threads', 1), 'omp_num_threads'
        )
        mkl_threads = self._positive_int(
            self._args.get('mkl_num_threads', 1), 'mkl_num_threads'
        )
        tokens = self._command_tokens(mode, config_file)
        command = ' '.join(shlex.quote(str(token)) for token in tokens)

        lines = [
            '#!/bin/bash',
            'set -euo pipefail',
            f'export OMP_NUM_THREADS={omp_threads}',
            f'export MKL_NUM_THREADS={mkl_threads}',
        ]
        if self.gpuList:
            if isinstance(self.gpuList, str):
                visible = self.gpuList.strip().replace(' ', ',')
            else:
                visible = ','.join(str(device) for device in self.gpuList)
            lines.append(f'export CUDA_VISIBLE_DEVICES={shlex.quote(visible)}')
        lines.extend([
            f'cd {shlex.quote(os.path.abspath(self.join(self.TS)))}',
            f'exec {command}',
            '',
        ])

        with open(runner_path, 'w', encoding='utf-8') as handle:
            handle.write('\n'.join(lines))
        os.chmod(runner_path, os.stat(runner_path).st_mode | stat.S_IXUSR)
        return runner_path

    def runBatch(self, batch, **kwargs):
        self.inputTs = kwargs['inputTs']
        self.writeInfo()

        input_folder = FolderManager(os.path.abspath(os.path.dirname(self.inputTs)))
        self._ensure_project_inputs(input_folder)

        geometry = self._dataset_geometry()
        
        mode = self._mode()
        print(f"Miss-Alignment mode: {mode}")

        config_file = self._prepare_project(batch, mode, geometry)
        # runner = self._write_runner(batch, mode, config_file)

        # # The launcher activates the Miss-Alignment environment, then executes
        # # this runner. The runner owns environment variables and exact CLI quoting.
        # self.batch_execute(
        #     'miss_alignment',
        #     batch,
        #     Args({'bash': os.path.abspath(runner)}),
        #     launcher=self._get_launcher(),
        # )
        # self.updateBatchInfo(batch)

    def _copy_passthrough_metadata(self, batch):
        """Create a local STAR handle while preserving initial Relion matrices."""
        batch.mkdir('tilt_series')
        metadata_dir = FolderManager(batch.join('tilt_series'))
        input_table = StarFile.getTableFromFile('global', self.inputTs)
        output_table = Table(input_table.getColumnNames())

        for row in input_table:
            row_dict = row._asdict()
            source_star = row_dict.get('rlnTomoTiltSeriesStarFile', '')
            if source_star and source_star != 'None' and os.path.isfile(source_star):
                destination_star = metadata_dir.join(os.path.basename(source_star))
                if os.path.abspath(source_star) != os.path.abspath(destination_star):
                    shutil.copy2(source_star, destination_star)
                row_dict['rlnTomoTiltSeriesStarFile'] = destination_star
            output_table.addRowValues(**row_dict)

        output_star = batch.join(self.OUTPUT_STAR)
        self.write_ts_table('global', output_table, output_star)
        return output_star

    def _output(self, batch):
        output_star = self._copy_passthrough_metadata(batch)
        output_nodes = [[
            output_star,
            'TomogramGroupMetadata.star.emwrap.tsalign',
        ]]
        self.writeRelionOutputNodes(output_nodes)

        mode = self._mode()
        config_file = self.join(self.TS, self.CONFIG_NAME)
        files = [[output_star, 'TomogramGroupMetadata']]
        if os.path.exists(self.join(self.TSS)):
            files.append([self.join(self.TSS), 'WarpTiltSeriesSettings'])
        if os.path.exists(config_file):
            files.append([config_file, 'MissAlignmentConfig'])
        if mode == 'train':
            for checkpoint in sorted(glob(self.join(self.TS, 'iter*', 'model.ckpt'))):
                files.append([checkpoint, 'MissAlignmentCheckpoint'])
        else:
            for source in sorted(glob(self.join(self.TS, 'iter*', 'model_source.txt'))):
                files.append([source, 'MissAlignmentModelSource'])

        self.outputs['MissAlignment'] = {
            'label': 'Miss-Alignment',
            'type': 'MissAlignmentRun',
            'info': (
                f'Mode: {mode}; Warp project: {self.join(self.TS)}. '
                'Relion alignment matrices remain the initial coarse alignment.'
            ),
            'files': files,
        }
        self.updateBatchInfo(batch)

    def prerun(self):
        self.inputTs = self._args['input_tiltseries']
        batch = Batch(id=self.name, path=self.path)
        if not self._register_output_only():
            self.runBatch(batch, inputTs=self.inputTs)
        else:
            self.log(
                "Received 'register_output_only'; only registering existing outputs."
            )
        # self._output(batch)


if __name__ == '__main__':
    MissAlignment.main()
  