# **************************************************************************
# *
# * Authors:     Daniel Marchan Torres (danielmarchan3@gmail.com)
# *
# * This program is free software; you can redistribute it and/or modify
# * it under the terms of the GNU General Public License as published by
# * the Free Software Foundation; either version 3 of the License, or
# * (at your option) any later version.
# *
# **************************************************************************

import json
import os
import shutil
import time
from glob import glob

from emtools.image import Image
from emtools.jobs import Args, Batch
from emtools.metadata import StarFile, Table, WarpXml
from emtools.utils import FolderManager

from emwrap.warp.warp import WarpBasePipeline


class MissAlignment(WarpBasePipeline):
    """Run Miss-Alignment on a Warp project produced by coarse TS alignment."""

    name = 'emw-missalignment'
    PROGRAM = 'MISSALIGNMENT'

    MODE_TRAIN_INFER = 0
    MODE_TRAIN_ONLY = 1
    MODE_INFER_ONLY = 2 

    CONFIG_NAME = 'miss_alignment_config.yaml'
    CONFIG_TEMPLATE = 'config_template.yaml'
    UPDATE_SCRIPT = 'update_warp_xml.py'
    TRAINING_DIR = 'warp_tiltseries_training'
    OUTPUT_STAR = 'miss_aligned_tilt_series.star'

    # ------------------------------------------------------------------
    # Launcher and argument helpers
    # ------------------------------------------------------------------
    def _get_launcher(self):
        """Use the launcher that activates the Miss-Alignment environment."""
        return self.get_launcher_arg('launcher_missalignment', self.PROGRAM)

    def _get_args(self, prefix, new_prefix='--'):
        """Return arguments below *prefix* using a new command-line prefix."""
        return self._args.subset(
            prefix,
            new_prefix,
            filters=['remove_empty'],
        )

    @staticmethod
    def _positive_int(value, name):
        try:
            result = int(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f'{name} must be an integer, received: {value!r}'
            ) from exc
        if result <= 0:
            raise ValueError(
                f'{name} must be greater than zero, received: {result}'
            )
        return result

    @staticmethod
    def _nonnegative_int(value, name):
        try:
            result = int(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f'{name} must be an integer, received: {value!r}'
            ) from exc
        if result < 0:
            raise ValueError(
                f'{name} must be zero or greater, received: {result}'
            )
        return result

    @staticmethod
    def _positive_float(value, name):
        try:
            result = float(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f'{name} must be numeric, received: {value!r}'
            ) from exc
        if result <= 0:
            raise ValueError(
                f'{name} must be greater than zero, received: {result}'
            )
        return result

    @staticmethod
    def _nonnegative_float(value, name):
        try:
            result = float(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f'{name} must be numeric, received: {value!r}'
            ) from exc
        if result < 0:
            raise ValueError(
                f'{name} must be zero or greater, received: {result}'
            )
        return result

    @staticmethod
    def _as_bool(value):
        if isinstance(value, bool):
            return value
        text = str(value).strip().lower()
        if text in {'1', 'true', 'yes', 'on'}:
            return True
        if text in {'0', 'false', 'no', 'off', ''}:
            return False
        raise ValueError(f'Cannot interpret as boolean: {value!r}')

    def _get_mode(self):
        return int(self._args.get('mode', self.MODE_TRAIN_INFER))

    # ------------------------------------------------------------------
    # Warp project preparation
    # ------------------------------------------------------------------
    def _ensure_project_inputs(self, input_folder):
        """Import the Warp project once and preserve it on resumed executions."""
        expected = [
            self.join(self.FS),
            self.join(self.FSS),
            self.join(self.TS),
            self.join(self.TSS),
            self.join(self.TM),
        ]

        if all(os.path.exists(path) for path in expected):
            self.log(
                'Using the existing local Warp project for resume/re-registration.'
            )
            return

        present = [path for path in expected if os.path.exists(path)]
        if present:
            raise RuntimeError(
                'The local Warp project is only partially populated. Remove '
                'the partial job output or restore the missing inputs. '
                f'Existing paths: {present}'
            )

        self._importInputs(
            input_folder,
            keys=['fs', 'fss', 'ts', 'tss', 'tm'],
        )

    def _dataset_geometry(self):
        """Resolve image shape, volume shape, and pixel size for XML preparation."""
        global_table = StarFile.getTableFromFile('global', self.inputTs)
        
        first = global_table[0]
        pixel_size = self._positive_float(
            first.rlnTomoTiltSeriesPixelSize,
            'xml.pixel_size',
        )

        ts_table = StarFile.getTableFromFile(
            first.rlnTomoName,
            first.rlnTomoTiltSeriesStarFile,
        )
        if len(ts_table) == 0:
            raise ValueError(
                'Tilt-series metadata is empty: '
                f'{first.rlnTomoTiltSeriesStarFile}'
            )

        mic_file = ts_table[0].rlnMicrographName
        dims = Image.get_dimensions(mic_file)
        image_x, image_y = dims[0], dims[1]

        settings_dims = WarpXml(self.join(self.TSS)).getDict(
            'Settings',
            'Tomo',
            'Param',
        )

        geometry = {
            'image_x': self._positive_int(image_x, 'xml.image_size_x'),
            'image_y': self._positive_int(image_y, 'xml.image_size_y'),
            'volume_x': self._positive_int(
                settings_dims['DimensionsX'],
                'xml.volume_size_x',
            ),
            'volume_y': self._positive_int(
                settings_dims['DimensionsY'],
                'xml.volume_size_y',
            ),
            'volume_z': self._positive_int(
                settings_dims['DimensionsZ'],
                'xml.volume_size_z',
            ),
            'pixel_size': pixel_size,
        }

        self.log(
            'Miss-Alignment XML geometry: '
            f"image={geometry['image_x']}x{geometry['image_y']}, "
            f"volume={geometry['volume_x']}x{geometry['volume_y']}x"
            f"{geometry['volume_z']}, "
            f"pixel_size={geometry['pixel_size']} A/px"
        )
        return geometry

    def _update_warp_xml_script(self):
        """Return the standalone Warp XML update helper."""
        script_path = os.path.abspath(
            os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                self.UPDATE_SCRIPT,
            )
        )
        if not os.path.isfile(script_path):
            raise FileNotFoundError(
                'Warp XML update helper not found. Install '
                f'{self.UPDATE_SCRIPT} beside {os.path.basename(__file__)}: '
                f'{script_path}'
            )
        return script_path

    def _update_warp_xmls(self, batch, geometry):
        """Update all Warp tilt-series XMLs required by Miss-Alignment."""
        script_path = self._update_warp_xml_script()
        xml_directory = os.path.abspath(self.join(self.TS))

        if not os.path.isdir(xml_directory):
            raise NotADirectoryError(
                f'Warp tilt-series directory not found: {xml_directory}'
            )

        args = Args({
            'python': script_path,
            '--xml-directory': xml_directory,
            '--image-x': geometry['image_x'],
            '--image-y': geometry['image_y'],
            '--volume-x': geometry['volume_x'],
            '--volume-y': geometry['volume_y'],
            '--volume-z': geometry['volume_z'],
            '--pixel-size': geometry['pixel_size'],
        })

        self.batch_execute(
            'update_warp_xml',
            batch,
            args,
            launcher=self._get_launcher(),
        )

    # ------------------------------------------------------------------
    # Training subset preparation
    # ------------------------------------------------------------------
    def _prepare_training_subset(self, training_subset):
        """Create a Warp tilt-series directory containing only training XMLs.

        Selected XML files are copied so Miss-Alignment can work on the training
        subset independently. Shared non-XML files/directories from the imported
        Warp tilt-series folder are linked into the training directory.
        """
        source_dir = os.path.abspath(self.join(self.TS))
        training_dir = os.path.abspath(self.join(self.TRAINING_DIR))
        os.makedirs(training_dir, exist_ok=True)

        training_names = [str(row.rlnTomoName) for row in training_subset]

        # Remove only stale top-level XMLs. Keep iterN/ directories so a run can
        # be resumed with --start-at-iteration.
        for xml_file in glob(os.path.join(training_dir, '*.xml')):
            os.remove(xml_file)

        # Link shared Warp processing content. XML files are handled separately.
        for entry in os.listdir(source_dir):
            if entry.endswith('.xml'):
                continue

            src = os.path.join(source_dir, entry)
            dst = os.path.join(training_dir, entry)

            if os.path.lexists(dst):
                continue

            os.symlink(src, dst, target_is_directory=os.path.isdir(src))

        # Copy only the XML files belonging to the selected training tilt series.
        missing = []
        for ts_name in training_names:
            src_xml = os.path.join(source_dir, f'{ts_name}.xml')
            dst_xml = os.path.join(training_dir, f'{ts_name}.xml')

            if not os.path.isfile(src_xml):
                missing.append(src_xml)
                continue

            shutil.copy2(src_xml, dst_xml)

        if missing:
            raise FileNotFoundError(
                'Missing Warp XML files for the selected training tilt series: '
                + ', '.join(missing)
            )

        self.trainingDir = training_dir
        self.log(
            f'Prepared Miss-Alignment training set with '
            f'{len(training_names)} tilt series in: {training_dir}'
        )
        return training_dir

    # ------------------------------------------------------------------
    # Miss-Alignment YAML configuration
    # ------------------------------------------------------------------
    def _config_template_path(self):
        """Return the bundled Miss-Alignment config template."""
        config_template = os.path.abspath(
            os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                self.CONFIG_TEMPLATE,
            )
        )
        if not os.path.isfile(config_template):
            raise FileNotFoundError(
                'Miss-Alignment config template not found. Install '
                f'{self.CONFIG_TEMPLATE} beside {os.path.basename(__file__)}: '
                f'{config_template}'
            )
        return config_template

    @staticmethod
    def _replace_yaml_scalar(lines, section_name, key, value):
        """Replace one scalar key inside a top-level YAML section."""
        section = None

        for i, line in enumerate(lines):
            stripped = line.strip()

            if line and not line[0].isspace() and stripped.endswith(':'):
                section = stripped[:-1]
                continue

            if section == section_name and stripped.startswith(f'{key}:'):
                indent = line[:len(line) - len(line.lstrip())]
                lines[i] = f'{indent}{key}: {value}\n'
                return True

        return False

    @staticmethod
    def _replace_iteration_settings(
        lines,
        anchoring_iterations,
        global_iterations,
        spline_iterations,
    ):
        """Replace general.iteration_settings using the iteration counts.

        Iteration definitions:
        anchoring:
            first iteration  -> downsample 3
            remaining ones   -> downsample 2

        global:
            downsample 1, alignment global

        spline/local:
            downsample 1, alignment [3, 3]
        """
        section = None
        start = None
        base_indent = None

        # Find general.iteration_settings
        for i, line in enumerate(lines):
            stripped = line.strip()

            if line and not line[0].isspace() and stripped.endswith(':'):
                section = stripped[:-1]
                continue

            if (
                section == 'general'
                and stripped.startswith('iteration_settings:')
            ):
                start = i
                base_indent = len(line) - len(line.lstrip())
                break

        if start is None:
            return False

        # Find where the current iteration_settings list ends.
        end = start + 1

        while end < len(lines):
            line = lines[end]
            stripped = line.strip()

            if not stripped:
                end += 1
                continue

            indent = len(line) - len(line.lstrip())

            # Stop when reaching the next key in the general section,
            # for example:
            #
            #   seed: 45132
            #
            if not stripped.startswith('#') and indent <= base_indent:
                break

            end += 1

        new_settings = []

        # Anchoring iterations.
        #
        # First:
        #   { downsample: 3, alignment: anchoring }
        #
        # Additional:
        #   { downsample: 2, alignment: anchoring }
        for index in range(anchoring_iterations):
            downsample = 3 if index == 0 else 2

            new_settings.append(
                f'    - {{ downsample: {downsample}, '
                'alignment: anchoring }\n'
            )

        # Global iterations.
        for _ in range(global_iterations):
            new_settings.append(
                '    - { downsample: 1, alignment: global }\n'
            )

        # Spline/local refinement iterations.
        for _ in range(spline_iterations):
            new_settings.append(
                '    - { downsample: 1, alignment: [3, 3] }\n'
            )

        # Replace the old list with the newly generated one.
        lines[start + 1:end] = new_settings

        return True

    def _update_config_yaml(self, training_directory):
        """Copy the template and update training values from the EMHub form."""
        config_template = self._config_template_path()
        training_directory = os.path.abspath(training_directory)
        config_file = os.path.join(training_directory, self.CONFIG_NAME)

        apply_ctf = self._as_bool(
            self._args.get('train.yml.apply_ctf', False)
        )
        anchoring_iterations = self._nonnegative_int(
            self._args.get('train.yml.iterations_anchoring', 2),
            'train.yml.iterations_anchoring',
        )
        global_iterations = self._nonnegative_int(
            self._args.get('train.yml.iterations_global', 2),
            'train.yml.iterations_global',
        )
        spline_iterations = self._nonnegative_int(
            self._args.get('train.yml.iterations_spline', 4),
            'train.yml.iterations_spline',
        )

        learning_rate = self._positive_float(
            self._args.get('train.yml.learning_rate', 1.0e-3),
            'train.yml.learning_rate',
        )
        max_epochs = self._positive_int(
            self._args.get('train.yml.max_epochs_per_iteration', 30),
            'train.yml.max_epochs_per_iteration',
        )

        data_batch_size = self._positive_int(
            self._args.get('train.yml.dt_batch_size', 32),
            'train.yml.dt_batch_size',
        )
        data_patch_size = self._positive_int(
            self._args.get('train.yml.dt_patch_size', 96),
            'train.yml.dt_patch_size',
        )
        steps_per_epoch = self._positive_int(
            self._args.get('train.yml.dt_steps_per_epoch', 1000),
            'train.yml.dt_steps_per_epoch',
        )

        alignment_patch_size = self._positive_int(
            self._args.get('train.yml.al_patch_size', 96),
            'train.yml.al_patch_size',
        )
        alignment_batch_size = self._positive_int(
            self._args.get('train.yml.al_batch_size', 32),
            'train.yml.al_batch_size',
        )
        alignment_patch_overlap = self._nonnegative_float(
            self._args.get('train.yml.al_patch_overlap', 0.1),
            'train.yml.al_patch_overlap',
        )

        shutil.copy2(config_template, config_file)

        with open(config_file, 'r', encoding='utf-8') as handle:
            lines = handle.readlines()

        updates = [
            (
                'general',
                'training_directory',
                json.dumps(training_directory),
            ),
            (
                'general',
                'apply_ctf',
                'True' if apply_ctf else 'False',
            ),
            (
                'model_training',
                'learning_rate',
                learning_rate,
            ),
            (
                'model_training',
                'max_epochs_per_iteration',
                max_epochs,
            ),
            (
                'data_loading',
                'batch_size',
                data_batch_size,
            ),
            (
                'data_loading',
                'patch_size',
                data_patch_size,
            ),
            (
                'data_loading',
                'steps_per_epoch',
                steps_per_epoch,
            ),
            (
                'tilt_series_alignment',
                'patch_size',
                alignment_patch_size,
            ),
            (
                'tilt_series_alignment',
                'patch_overlap',
                alignment_patch_overlap,
            ),
            (
                'tilt_series_alignment',
                'batch_size',
                alignment_batch_size,
            ),
        ]

        missing = []
        for section_name, key, value in updates:
            if not self._replace_yaml_scalar(
                lines,
                section_name,
                key,
                value,
            ):
                missing.append(f'{section_name}.{key}')

        if not self._replace_iteration_settings(
            lines,
            anchoring_iterations,
            global_iterations,
            spline_iterations,
        ):
            missing.append('general.iteration_settings')

        if missing:
            raise ValueError(
                'Could not update expected keys in '
                f'{config_template}: {", ".join(missing)}'
            )

        with open(config_file, 'w', encoding='utf-8') as handle:
            handle.writelines(lines)

        self.log(
            'Miss-Alignment config updated: '
            f'training_directory={training_directory}, '
            f'iterations='
            f'{anchoring_iterations} anchoring/'
            f'{global_iterations} global/'
            f'{spline_iterations} spline, '
            f'alignment_batch_size={alignment_batch_size}'
        )
        return config_file

    # ------------------------------------------------------------------
    # Miss-Alignment training
    # ------------------------------------------------------------------
    def _run_miss_alignment(self, batch, config_file):
        """Launch Miss-Alignment training through the configured launcher.

        Training/reconstruction device allocation is intentionally kept in the
        initial single-GPU configuration while the command is being validated.
        The form-provided Miss-Alignment CLI options are appended directly.
        """
        omp_threads = 1
        mkl_threads = 1

        # Initial single-large-GPU configuration.
        training_devices = '0'
        reconstruction_devices = '0,0,0'

        if self.gpuList:
            if isinstance(self.gpuList, str):
                visible_devices = self.gpuList.strip().replace(' ', ',')
            else:
                visible_devices = ','.join(
                    str(device) for device in self.gpuList
                )
        else:
            visible_devices = '0'

        args = Args({
            'env': '',
            f'OMP_NUM_THREADS={omp_threads}': '',
            f'MKL_NUM_THREADS={mkl_threads}': '',
            f'CUDA_VISIBLE_DEVICES={visible_devices}': '',
            'miss-alignment': '',
            'train': '',
            '--config-file': config_file,
            '--training-devices': training_devices,
            '--reconstruction-devices': reconstruction_devices,
        })

        # Adds:
        #   --dataloaders-per-trainer
        #   --prepare-stacks
        #   --start-at-iteration
        args.update(self._get_args('train.missalign'))

        self.log(f'Miss-Alignment training args: {args}')

        self.batch_execute(
            'miss_alignment_train',
            batch,
            args,
            launcher=self._get_launcher(),
        )

    # ------------------------------------------------------------------
    # Input monitoring
    # ------------------------------------------------------------------
    def _getInputTsTable(self):
        """Read input STAR file and return the global table."""
        input_star = self._args['input_tiltseries']
        if os.path.exists(input_star):
            with StarFile(input_star) as sf:
                table = sf.getTable('global')
                self.inputLen = len(table)
                return table
        return None

    def _wait_for_input_table(self):
        table = self._getInputTsTable()
        while table is None:
            self.log('No input found yet, sleeping 30s')
            time.sleep(30)
            table = self._getInputTsTable()
        return table

    def _wait_for_training_set(self):
        """Wait until the requested number of tilt series is available."""
        while True:
            table = self._getInputTsTable()
            rows = list(table) if table is not None else []

            if len(rows) >= self.n_training:
                return rows[:self.n_training]

            self.log(
                f'Waiting for enough tilt series '
                f'({len(rows)}/{self.n_training})'
            )
            time.sleep(30)

    # ------------------------------------------------------------------
    # Training lifecycle
    # ------------------------------------------------------------------
    def launch_training(self, training_subset):
        """Prepare the training subset and launch Miss-Alignment training."""
        self.log(
            f'Launching training with {len(training_subset)} tilt series'
        )
        self.writeInfo()

        batch = Batch(id=self.name, path=self.path)

        # The input Warp project belongs to the input STAR job folder.
        input_folder = FolderManager(os.path.abspath(os.path.dirname(self.inputTs)))
        self._ensure_project_inputs(input_folder)

        # Update every imported Warp tilt-series XML first.
        geometry = self._dataset_geometry()
        self._update_warp_xmls(batch, geometry)

        # Then create an isolated training dataset containing only the
        # selected tilt-series XML files.
        training_directory = self._prepare_training_subset(training_subset)

        # Build the training YAML inside the training dataset.
        config_file = self._update_config_yaml(training_directory)

        # Launch training.
        self._run_miss_alignment(batch, config_file)

        self.updateBatchInfo(batch)

        self.trainingBestModel = os.path.join(training_directory, 'model.ckpt')
        if not os.path.isfile(self.trainingBestModel):
            raise FileNotFoundError(
                'Miss-Alignment training finished but the best model was not found: '
                f'{self.trainingBestModel}'
            )

        self.modelPath = self.trainingBestModel

        self.log(
            f'Miss-Alignment training finished. '
            f'Best model: {self.trainingBestModel}'
        )

    # ------------------------------------------------------------------
    # Output registration
    # ------------------------------------------------------------------
    def _copy_passthrough_metadata(self, batch):
        """Create a local STAR handle while preserving initial Relion matrices."""
        batch.mkdir('tilt_series')
        metadata_dir = FolderManager(batch.join('tilt_series'))
        input_table = StarFile.getTableFromFile('global', self.inputTs)
        output_table = Table(input_table.getColumnNames())

        for row in input_table:
            row_dict = row._asdict()
            source_star = row_dict.get(
                'rlnTomoTiltSeriesStarFile',
                '',
            )

            if (
                source_star
                and source_star != 'None'
                and os.path.isfile(source_star)
            ):
                destination_star = metadata_dir.join(
                    os.path.basename(source_star)
                )
                if (
                    os.path.abspath(source_star)
                    != os.path.abspath(destination_star)
                ):
                    shutil.copy2(source_star, destination_star)

                row_dict['rlnTomoTiltSeriesStarFile'] = destination_star

            output_table.addRowValues(**row_dict)

        output_star = batch.join(self.OUTPUT_STAR)
        self.write_ts_table('global', output_table, output_star)
        return output_star

    def _output(self, batch):
        output_star = self._copy_passthrough_metadata(batch)
        self.writeRelionOutputNodes([[
            output_star,
            'TomogramGroupMetadata.star.emwrap.tsalign',
        ]])

        files = [[output_star, 'TomogramGroupMetadata']]

        if os.path.exists(self.join(self.TSS)):
            files.append([
                self.join(self.TSS),
                'WarpTiltSeriesSettings',
            ])

        training_directory = getattr(
            self,
            'trainingDir',
            self.join(self.TRAINING_DIR),
        )
        config_file = os.path.join(
            training_directory,
            self.CONFIG_NAME,
        )

        if os.path.exists(config_file):
            files.append([
                config_file,
                'MissAlignmentConfig',
            ])

        best_model = os.path.join(training_directory, 'model.ckpt')
        if os.path.exists(best_model):
            files.append([
                best_model,
                'MissAlignmentCheckpoint',
            ])

        self.outputs['MissAlignment'] = {
            'label': 'Miss-Alignment',
            'type': 'MissAlignmentRun',
            'info': (
                f'Training directory: {training_directory}. '
                'Relion alignment matrices remain the initial coarse alignment.'
            ),
            'files': files,
        }

        self.updateBatchInfo(batch)

    # ------------------------------------------------------------------
    # Pipeline lifecycle
    # ------------------------------------------------------------------
    def prerun(self):
        self.inputTs = self._args['input_tiltseries']
        self.n_training = int(self._args.get('train.n_training', 10))
        mode = self._get_mode()

        self.inputTsTable = self._wait_for_input_table()
        self.log(
            f'Found input tilt series: {len(self.inputTsTable)}'
        )

        if mode == self.MODE_INFER_ONLY:
            raise NotImplementedError(
                'Infer-only mode is not implemented yet.'
            )

        training_subset = self._wait_for_training_set()
        self.log(
            f'Starting training with {len(training_subset)} tilt series'
        )

        self.launch_training(training_subset)
        
        # self._output(Batch(id=self.name, path=self.path))

        # if mode == self.MODE_TRAIN_ONLY:
        #     self.log(
        #         f'Training-only mode finished. '
        #         f'Latest model: {self.modelPath}'
        #     )
        #     return

        # # Train & Infer reaches this point after training. Inference will be
        # # implemented separately.
        # self.log(
        #     f'Training completed. Model available for inference: '
        #     f'{self.modelPath}'
        # )


if __name__ == '__main__':
    MissAlignment.main()