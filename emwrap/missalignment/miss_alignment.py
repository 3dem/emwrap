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
from emtools.metadata import Imod, RelionStar, StarFile, Table, WarpXml
from emtools.utils import FolderManager

from emwrap.warp.warp import WarpBasePipeline

from .utils import warp_xml_to_imod_xf 


class MissAlignment(WarpBasePipeline):
    """Run Miss-Alignment on a Warp project produced by coarse TS alignment."""

    name = 'emw-missalignment'
    PROGRAM = 'MISSALIGNMENT'

    MODE_TRAIN_INFER = 0
    MODE_TRAIN_ONLY = 1
    MODE_INFER_ONLY = 2 

    CONFIG_NAME = 'miss_alignment_config.yaml'
    CONFIG_TEMPLATE = 'config_template.yaml'
    INFERENCE_CONFIG_NAME = 'miss_alignment_inference_config.yaml'
    INFERENCE_CONFIG_TEMPLATE = 'inference_config_template.yaml'
    UPDATE_SCRIPT = 'update_warp_xml.py'
    TRAINING_DIR = 'warp_tiltseries_training'
    OUTPUT_STAR = 'aligned_tilt_series.star'

    # ------------------------------------------------------------------
    # Launcher and argument helpers
    # ------------------------------------------------------------------
    def _get_launcher(self):
        """Use the launcher that activates the Miss-Alignment environment."""
        return self.get_launcher_arg('launcher_missalignment', self.PROGRAM)

    def _get_args(self, prefix, new_prefix='--'):
        """Return arguments below *prefix* using a new command-line prefix."""
        return self._args.subset(prefix, new_prefix, filters=['remove_empty'])

    def _get_mode(self):
        return int(self._args.get('mode', self.MODE_TRAIN_INFER))

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
            self.join(self.FRAMES),
            self.join(self.MDOCS)
        ]

        if all(os.path.exists(path) for path in expected):
            self.log('Using the existing local Warp project for resume/re-registration.')
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
            keys=['fs', 'fss', 'ts', 'tss', 'tm', 'frames', 'mdocs'],
        )

    def _dataset_geometry(self):
        """Resolve image shape, volume shape, and pixel size for XML preparation."""
        global_table = StarFile.getTableFromFile('global', self.inputTs)
        
        first = global_table[0]
        pixel_size = float(first.rlnTomoTiltSeriesPixelSize)
        ts_table = StarFile.getTableFromFile(first.rlnTomoName, first.rlnTomoTiltSeriesStarFile)

        if len(ts_table) == 0:
            raise ValueError('Tilt-series metadata is empty: '
            f'{first.rlnTomoTiltSeriesStarFile}')

        mic_file = ts_table[0].rlnMicrographName
        dims = Image.get_dimensions(mic_file)
        image_x, image_y = dims[0], dims[1]

        settings_dims = WarpXml(self.join(self.TSS)).getDict(
            'Settings',
            'Tomo',
            'Param',
        )

        geometry = {
            'image_x': image_x,
            'image_y': image_y,
            'volume_x': settings_dims['DimensionsX'],
            'volume_y': settings_dims['DimensionsY'], 
            'volume_z': settings_dims['DimensionsZ'],
            'pixel_size': pixel_size
        }

        self.log(
            'Miss-Alignment XML geometry: '
            f"image={geometry['image_x']}x{geometry['image_y']}, "
            f"volume={geometry['volume_x']}x{geometry['volume_y']}x{geometry['volume_z']}, "
            f"pixel_size={geometry['pixel_size']} A/px"
        )
        return geometry

    def _update_warp_xml_script(self):
        """Return the standalone Warp XML update helper."""
        script_path = os.path.abspath(
            os.path.join(os.path.dirname(os.path.abspath(__file__)), self.UPDATE_SCRIPT))
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
        subset independently, this are modified by the program.
        Shared non-XML files/directories from the imported Warp tilt-series folder
        are linked into the training directory.
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
            os.path.join(os.path.dirname(os.path.abspath(__file__)), self.CONFIG_TEMPLATE))

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
    def _replace_iteration_settings(lines, anchoring_iterations, global_iterations, spline_iterations):
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

            if (section == 'general' and stripped.startswith('iteration_settings:')):
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

            # Stop when reaching the next key in the general section, for example:
            #   seed: 45132
            if not stripped.startswith('#') and indent <= base_indent:
                break

            end += 1

        new_settings = []

        # Anchoring iterations.
        # First: { downsample: 3, alignment: anchoring }
        # Additional: { downsample: 2, alignment: anchoring }
        for index in range(anchoring_iterations):
            downsample = 3 if index == 0 else 2
            new_settings.append(
                f'    - {{ downsample: {downsample}, '
                'alignment: anchoring }\n'
            )

        # Global iterations.
        for _ in range(global_iterations):
            new_settings.append('    - { downsample: 1, alignment: global }\n')

        # Spline/local refinement iterations.
        for _ in range(spline_iterations):
            new_settings.append('    - { downsample: 1, alignment: [3, 3] }\n')

        # Replace the old list with the newly generated one.
        lines[start + 1:end] = new_settings

        return True

    def _update_config_yaml(self, training_directory):
        """Copy the template and update training values from the EMHub form."""
        config_template = self._config_template_path()
        training_directory = os.path.abspath(training_directory)
        config_file = os.path.join(training_directory, self.CONFIG_NAME)

        apply_ctf = self._as_bool(self._args.get('train.yml.apply_ctf', False))
        anchoring_iterations = int(self._args.get('train.yml.iterations_anchoring', 2))
        global_iterations = int(self._args.get('train.yml.iterations_global', 2))
        spline_iterations = int(self._args.get('train.yml.iterations_spline', 4))

        learning_rate = self._args.get('train.yml.learning_rate', 1.0e-3)
        max_epochs = self._args.get('train.yml.max_epochs_per_iteration', 30)
        data_batch_size = self._args.get('train.yml.dt_batch_size', 32)
        data_patch_size = self._args.get('train.yml.dt_patch_size', 96)
        steps_per_epoch = self._args.get('train.yml.dt_steps_per_epoch', 1000)

        alignment_patch_size = self._args.get('train.yml.al_patch_size', 96)
        alignment_batch_size = self._args.get('train.yml.al_batch_size', 32)
        alignment_patch_overlap = self._args.get('train.yml.al_patch_overlap', 0.1)

        shutil.copy2(config_template, config_file)

        with open(config_file, 'r', encoding='utf-8') as handle:
            lines = handle.readlines()

        updates = [
            ('general', 'training_directory', json.dumps(training_directory)),
            ('general', 'apply_ctf', 'True' if apply_ctf else 'False'),
            ('model_training', 'learning_rate', learning_rate),
            ('model_training', 'max_epochs_per_iteration', max_epochs),
            ('data_loading', 'batch_size', data_batch_size),
            ('data_loading', 'patch_size', data_patch_size),
            ('data_loading', 'steps_per_epoch', steps_per_epoch),
            ('tilt_series_alignment', 'patch_size', alignment_patch_size),
            ('tilt_series_alignment', 'patch_overlap', alignment_patch_overlap),
            ('tilt_series_alignment', 'batch_size', alignment_batch_size),
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
                f'{config_template}: {", ".join(missing)}')

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
    # Miss-Alignment inference YAML configuration
    # ------------------------------------------------------------------
    def _inference_config_template_path(self):
        """Return the bundled Miss-Alignment inference config template."""
        config_template = os.path.abspath(
            os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                self.INFERENCE_CONFIG_TEMPLATE,
            )
        )
        if not os.path.isfile(config_template):
            raise FileNotFoundError(
                'Miss-Alignment inference config template not found. Install '
                f'{self.INFERENCE_CONFIG_TEMPLATE} beside '
                f'{os.path.basename(__file__)}: {config_template}'
            )
        return config_template

    def _validate_model_run_directory(self, model_run_directory, n_iterations):
        """Validate that all per-iteration checkpoints required by inference exist."""
        # TODO: check if you can have less iterations in the inference than the ones you used for training
        model_run_directory = os.path.abspath(model_run_directory)

        missing = []
        for iteration in range(1, n_iterations + 1):
            checkpoint = os.path.join(model_run_directory, f'iter{iteration}', 'model.ckpt')

            if not os.path.isfile(checkpoint):
                missing.append(checkpoint)

        if missing:
            raise FileNotFoundError(
                'The selected Miss-Alignment model run does not contain all '
                f'{n_iterations} checkpoints required by the inference '
                'iteration schedule. Missing: '
                + ', '.join(missing))

        return model_run_directory

    def _update_inference_config_yaml(self, data_directory, model_run_directory):
        """Create the inference YAML from the bundled template and form values."""
        config_template = self._inference_config_template_path()
        data_directory = os.path.abspath(data_directory)

        apply_ctf = self._as_bool(self._args.get('infer.yml.apply_ctf', False))
        anchoring_iterations = int(self._args.get('infer.yml.iterations_anchoring', 2))
        global_iterations = int(self._args.get('infer.yml.iterations_global', 2))
        spline_iterations = int(self._args.get('infer.yml.iterations_spline', 4))

        alignment_patch_size = self._args.get('infer.yml.al_patch_size', 96)
        alignment_batch_size = self._args.get('infer.yml.al_batch_size', 32)
        alignment_patch_overlap = self._args.get('infer.yml.al_patch_overlap', 0.1)

        n_iterations = (
            anchoring_iterations
            + global_iterations
            + spline_iterations)
            
        if n_iterations == 0:
            raise ValueError('Inference requires at least one iteration.')

        model_run_directory = self._validate_model_run_directory(
            model_run_directory,
            n_iterations)

        config_file = os.path.join(data_directory, self.INFERENCE_CONFIG_NAME)
        shutil.copy2(config_template, config_file)

        with open(config_file, 'r', encoding='utf-8') as handle:
            lines = handle.readlines()

        updates = [
            ('general', 'data_directory', json.dumps(data_directory)),
            ('general', 'model_run_directory', json.dumps(model_run_directory)),
            ('general', 'apply_ctf', 'True' if apply_ctf else 'False'),
            ('tilt_series_alignment', 'patch_size', alignment_patch_size),
            ('tilt_series_alignment', 'patch_overlap', alignment_patch_overlap),
            ('tilt_series_alignment', 'batch_size', alignment_batch_size),
        ]

        missing = []
        for section_name, key, value in updates:
            if not self._replace_yaml_scalar(lines, section_name, key, value):
                missing.append(f'{section_name}.{key}')

        if not self._replace_iteration_settings(lines, anchoring_iterations, 
                                                global_iterations, 
                                                spline_iterations):
            missing.append('general.iteration_settings')

        if missing:
            raise ValueError(
                'Could not update expected keys in '
                f'{config_template}: {", ".join(missing)}')

        with open(config_file, 'w', encoding='utf-8') as handle:
            handle.writelines(lines)

        self.modelRunDirectory = model_run_directory

        self.log(
            'Miss-Alignment inference config updated: '
            f'data_directory={data_directory}, '
            f'model_run_directory={model_run_directory}, '
            f'iterations={anchoring_iterations} anchoring/'
            f'{global_iterations} global/'
            f'{spline_iterations} spline, '
            f'alignment_batch_size={alignment_batch_size}'
        )

        return config_file

    # ------------------------------------------------------------------
    # Miss-Alignment training
    # ------------------------------------------------------------------
    def _training_gpu_devices(self):
        """Build logical GPU assignments for Miss-Alignment training.

        ``self.gpuList`` contains the physical GPUs selected/reserved by EMHub.
        Once those GPUs are exposed through CUDA_VISIBLE_DEVICES, Miss-Alignment
        sees them as logical devices 0..N-1.

        Training devices are assigned first, followed by reconstruction devices.
        Each reconstruction GPU runs three reconstruction workers.
        """
        if not self.gpuList:
            raise ValueError(
                'Miss-Alignment training requires at least one GPU.'
            )

        n_gpus = len(self.gpuList)
        n_training = int(self._args.get('train.training_gpus', 1))
        n_reconstruction = int(self._args.get('train.reconstruction_gpus', 1))

        workers_per_reconstruction_gpu = 3

        # A single large GPU is shared by training and reconstruction.
        if n_gpus == 1:
            if n_training != 1 or n_reconstruction != 1:
                raise ValueError(
                    'With one reserved GPU, train.training_gpus and '
                    'train.reconstruction_gpus must both be 1.')
            return '0', ','.join(
                ['0'] * workers_per_reconstruction_gpu)

        if n_training + n_reconstruction > n_gpus:
            raise ValueError(
                f'Requested {n_training} training GPU(s) and '
                f'{n_reconstruction} reconstruction GPU(s), but only '
                f'{n_gpus} GPU(s) are available to the job.')

        logical_gpus = list(range(n_gpus))
        training_ids = logical_gpus[:n_training]
        reconstruction_ids = logical_gpus[n_training:n_training + n_reconstruction]

        training_devices = ','.join(str(device) for device in training_ids)
        reconstruction_devices = ','.join(
            str(device)
            for device in reconstruction_ids
            for _ in range(workers_per_reconstruction_gpu)
        )

        return training_devices, reconstruction_devices

    def _run_miss_alignment(self, batch, config_file):
        """Launch Miss-Alignment training through the configured launcher."""
        omp_threads = 1
        mkl_threads = 1

        training_devices, reconstruction_devices = (self._training_gpu_devices())

        if isinstance(self.gpuList, str):
            visible_devices = self.gpuList.strip().replace(' ', ',')
        else:
            visible_devices = ','.join(str(device) for device in self.gpuList)

        self.log(
            'Miss-Alignment GPU allocation: '
            f'CUDA_VISIBLE_DEVICES={visible_devices}; '
            f'training={training_devices}; '
            f'reconstruction={reconstruction_devices}'
        )

        args = Args({
            'env': '',
            f'OMP_NUM_THREADS={omp_threads}': '',
            f'MKL_NUM_THREADS={mkl_threads}': '',
            f'CUDA_VISIBLE_DEVICES={visible_devices}': '',
            'miss-alignment': '',
            'train': '',
            '--config-file': config_file,
            '--training-devices': training_devices,
            '--reconstruction-devices': reconstruction_devices
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
    # Miss-Alignment inference
    # ------------------------------------------------------------------
    def _run_miss_alignment_infer(self, batch, config_file):
        """Launch Miss-Alignment inference on all GPUs visible to the job."""
        if not self.gpuList:
            raise ValueError('Miss-Alignment inference requires at least one GPU.')

        omp_threads = 1
        mkl_threads = 1

        if isinstance(self.gpuList, str):
            visible_devices = self.gpuList.strip().replace(' ', ',')
        else:
            visible_devices = ','.join(str(device) for device in self.gpuList)

        args = Args({
            'env': '',
            f'OMP_NUM_THREADS={omp_threads}': '',
            f'MKL_NUM_THREADS={mkl_threads}': '',
            f'CUDA_VISIBLE_DEVICES={visible_devices}': '',
            'miss-alignment': '',
            'infer': '',
            '--config-file': config_file,
        })

        # Adds:
        #   --prepare-stacks
        #   --start-at-iteration
        args.update(self._get_args('infer.missalign'))

        self.log('Miss-Alignment inference GPU allocation: '
            f'CUDA_VISIBLE_DEVICES={visible_devices}')
        self.log(f'Miss-Alignment inference args: {args}')

        self.batch_execute(
            'miss_alignment_infer',
            batch,
            args,
            launcher=self._get_launcher()
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
        self.writeInfo()

        batch = Batch(id=self.name, path=self.path)

        # The input Warp project belongs to the input STAR job folder.
        input_folder = FolderManager(os.path.abspath(os.path.dirname(self.inputTs)))
        self._ensure_project_inputs(input_folder)

        # Update every imported Warp tilt-series XML first.
        geometry = self._dataset_geometry()
        self._update_warp_xmls(batch, geometry)

        # Then create an isolated training dataset containing only the selected tilt-series XML files.
        training_directory = self._prepare_training_subset(training_subset)

        # Build the training YAML inside the training dataset.
        config_file = self._update_config_yaml(training_directory)

        # Launch training.
        self._run_miss_alignment(batch, config_file)

        self.updateBatchInfo(batch)

        trainingBestModel = os.path.join(training_directory, 'model.ckpt')
        
        if not os.path.isfile(trainingBestModel):
            raise FileNotFoundError(
                'Miss-Alignment training finished but the best model was not found: '
                f'{trainingBestModel}')

        self.log(
            f'Miss-Alignment training finished. '
            f'Best model: {trainingBestModel}')

        return training_directory

    def launch_inference(self, model_run_directory, mode):
        """Prepare the full Warp dataset and launch Miss-Alignment inference."""
        
        self.log('Launching Miss-Alignment inference with model run: '
            f'{model_run_directory}')

        batch = Batch(id=self.name, path=self.path)

        input_folder = FolderManager(os.path.abspath(os.path.dirname(self.inputTs)))

        self._ensure_project_inputs(input_folder)
        geometry = self._dataset_geometry()
        # If mode is train+infer, the Warp project was already imported during training. 
        # If mode is infer-only, we need to import the Warp project and update the XMLs now. 
        if mode == self.MODE_INFER_ONLY:
            self._update_warp_xmls(batch, geometry)

        data_directory = os.path.abspath(self.join(self.TS))
        config_file = self._update_inference_config_yaml(
            data_directory,
            model_run_directory)

        self._run_miss_alignment_infer(batch, config_file)
        self.updateBatchInfo(batch)

        self._write_imod_xfs(data_directory, geometry['pixel_size'])

        # Convert the optimized global XF transforms back into the RELION 5
        # alignment columns and register a new aligned_tilt_series.star.
        self._output(batch, geometry['pixel_size'])

        self.log(
            'Miss-Alignment inference finished. '
            f'Aligned snapshots are in: {data_directory}/iterN/')

    def _write_imod_xfs(self, data_directory, pixel_size):
        """Export updated Warp global alignments as IMOD XF files.
        Each ``TS_NAME.xml`` in the inference data directory is converted to
        ``TS_NAME.xf`` in the same directory. The XF contains only the global
        affine alignment represented by AxisAngle/AxisOffsetX/AxisOffsetY.
        """
        data_directory = os.path.abspath(data_directory)
        xml_files = sorted(glob(os.path.join(data_directory, '*.xml')))

        if not xml_files:
            raise FileNotFoundError(
                'No Warp tilt-series XML files were found for XF export in: '
                f'{data_directory}')

        xf_files = []
        for xml_file in xml_files:
            xf_file = os.path.splitext(xml_file)[0] + '.xf'
            warp_xml_to_imod_xf(xml_file, xf_file, pixel_size)
            xf_files.append(xf_file)

        self.log(
            f'Exported {len(xf_files)} IMOD XF alignment file(s) to: '
            f'{data_directory}')

    # ------------------------------------------------------------------
    # Output registration
    # ------------------------------------------------------------------
    def _compute_relion_alignments_from_xf(self, xf_file, tilt_angles, pixel_size):
        """Convert one IMOD XF file to RELION per-tilt alignment values."""
        imod_alignments = Imod.get_alignment_from_xf(xf_file)

        if len(imod_alignments) != len(tilt_angles):
            raise ValueError(
                f'XF/STAR row count mismatch for {xf_file}: '
                f'{len(imod_alignments)} XF transforms versus '
                f'{len(tilt_angles)} tilt angles.'
            )

        return RelionStar.alignments_from_imod(
            tilt_angles,
            imod_alignments,
            pixel_size,
        )

    def _write_individual_tilt_series_star(self, batch, ts_row, pixel_size):
        """Copy one input TS STAR while replacing only RELION alignment fields."""
        ts_name = str(ts_row.rlnTomoName)
        input_star = ts_row.rlnTomoTiltSeriesStarFile

        if not input_star or not os.path.isfile(input_star):
            raise FileNotFoundError(
                f'Input tilt-series STAR not found for {ts_name}: {input_star}')

        input_table = StarFile.getTableFromFile(ts_name, input_star)
        if len(input_table) == 0:
            raise ValueError(
                f'Input tilt-series STAR is empty for {ts_name}: {input_star}')

        tilt_angles = []
        for tilt_row in input_table:
            tilt_dict = tilt_row._asdict()
            tilt_angle = tilt_dict.get('rlnTomoNominalStageTiltAngle', None)
            if tilt_angle in ('', None):
                raise ValueError(
                    f'{ts_name}: rlnTomoNominalStageTiltAngle is required '
                    'to convert the IMOD XF transform to RELION alignment.')
            
            tilt_angles.append(float(tilt_angle))

        xf_file = os.path.join(os.path.abspath(self.join(self.TS)), f'{ts_name}.xf')
        if not os.path.isfile(xf_file):
            raise FileNotFoundError(
                f'Miss-Alignment XF file not found for {ts_name}: {xf_file}')

        relion_alignments = self._compute_relion_alignments_from_xf(
            xf_file,
            tilt_angles,
            pixel_size,
        )

        if len(relion_alignments) != len(input_table):
            raise ValueError(
                f'RELION alignment count mismatch for {ts_name}: '
                f'{len(relion_alignments)} alignments versus '
                f'{len(input_table)} STAR rows.')

        alignment_columns = (
            'rlnTomoXTilt',
            'rlnTomoYTilt',
            'rlnTomoZRot',
            'rlnTomoXShiftAngst',
            'rlnTomoYShiftAngst',
        )

        input_columns = input_table.getColumnNames()
        missing_columns = [
            column
            for column in alignment_columns
            if column not in input_columns
        ]
        if missing_columns:
            raise ValueError(
                f'{ts_name}: input STAR does not contain the expected '
                'RELION alignment columns: '
                + ', '.join(missing_columns))

        output_table = Table(input_columns)

        for tilt_row, alignment in zip(input_table, relion_alignments):
            tilt_dict = tilt_row._asdict()

            for column in alignment_columns:
                if column not in alignment:
                    raise ValueError(
                        f'{ts_name}: converted RELION alignment is missing '
                        f'{column}.'
                    )
                tilt_dict[column] = alignment[column]

            output_table.addRowValues(**tilt_dict)

        output_star = batch.join('tilt_series', f'{ts_name}.star')
        self.write_ts_table(ts_name, output_table, output_star)

        return output_star

    def _build_relion_output_metadata(self, batch, pixel_size):
        """Build RELION 5 metadata using the optimized Miss-Alignment XF files."""
        batch.mkdir('tilt_series')

        input_table = StarFile.getTableFromFile('global', self.inputTs)
        output_table = Table(input_table.getColumnNames())
        individual_stars = []

        for ts_row in input_table:
            output_ts_star = self._write_individual_tilt_series_star(
                batch,
                ts_row,
                pixel_size,
            )
            individual_stars.append(output_ts_star)

            row_dict = ts_row._asdict()
            row_dict['rlnTomoTiltSeriesStarFile'] = output_ts_star
            output_table.addRowValues(**row_dict)

        output_star = batch.join(self.OUTPUT_STAR)
        self.write_ts_table('global', output_table, output_star)

        return output_star, individual_stars

    def _output(self, batch, pixel_size):
        """Register RELION metadata containing Miss-Alignment global alignment."""
        output_star, individual_stars = self._build_relion_output_metadata(
            batch,
            pixel_size)

        self.writeRelionOutputNodes([[
            output_star,
            'TomogramGroupMetadata.star.relion.tomo.aligntiltseries',
        ]])

        self.updateBatchInfo(batch)

    # ------------------------------------------------------------------
    # Pipeline lifecycle
    # ------------------------------------------------------------------
    def prerun(self):
        self.inputTs = self._args['input_tiltseries']
        self.n_training = int(self._args.get('train.n_training', 10))
        mode = self._get_mode()

        # TODO: remove once scheduler is ready
        self.inputTsTable = self._wait_for_input_table()
        self.log(f'Found input tilt series: {len(self.inputTsTable)}')

        if mode == self.MODE_INFER_ONLY:
            model_run_directory = str(self._args.get('infer.model_run_directory', ''))
            if not model_run_directory:
                raise ValueError('Infer-only mode requires infer.model_run_directory.')
        else:
            training_subset = self._wait_for_training_set()
            self.log(f'Starting training with {len(training_subset)} tilt series')
            model_run_directory = self.launch_training(training_subset)

            if mode == self.MODE_TRAIN_ONLY:
                self.log('Training-only mode finished.')
                return

        self.log('Training completed. Starting inference using model run: '
            f'{model_run_directory}')

        self.launch_inference(model_run_directory, mode)        


if __name__ == '__main__':
    MissAlignment.main()