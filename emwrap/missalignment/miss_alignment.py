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

from cProfile import label
import json
import os
import shutil
import time
import uuid
from glob import glob

from emtools.image import Image
from emtools.jobs import Args, Batch
from emtools.metadata import Imod, RelionStar, StarFile, Table, WarpXml
from emtools.utils import FolderManager

from emwrap.warp.warp import WarpBasePipeline

from .utils import warp_xml_to_imod_xf, get_warp_movie_names


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

    def _get_tmpdir(self):
        """Return a TMPDIR path short enough for multiprocessing Unix sockets."""
        tmpdir = os.path.abspath(self.tmpDir)
      
        if len(tmpdir.strip()) < 94:  # 107 the limit - 14 exclusive from multiprocessing lib
            return tmpdir

        link = os.path.join('/tmp', f'emw-tmp-{os.getpid()}-{uuid.uuid4().hex[:8]}')
        os.symlink(tmpdir, link)
        self._tmpdir_link = link

        return link

    def _remove_tmpdir_link(self):
        if link := getattr(self, '_tmpdir_link', None):
            if os.path.islink(link):
                os.unlink(link)
            self._tmpdir_link = None

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
        # First: { downsample: 2 or 3, alignment: anchoring } 
        # Additional: { downsample: 2, alignment: anchoring }
        downsample = 2
        for index in range(anchoring_iterations):
            # downsample = 3 if index == 0 else 2 
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

        anchoring_iterations = int(self._args.get('train.yml.iterations_anchoring', 2))
        global_iterations = int(self._args.get('train.yml.iterations_global', 2))
        spline_iterations = int(self._args.get('train.yml.iterations_spline', 4))

        learning_rate = self._args.get('train.yml.learning_rate', 1.0e-3)
        max_epochs = self._args.get('train.yml.max_epochs_per_iteration', 30)
        steps_per_epoch = self._args.get('train.yml.dt_steps_per_epoch', 1000)

        batch_size = self._args.get('yml.batch_size', 32)
        patch_size = self._args.get('yml.patch_size', 96)
        alignment_patch_overlap = self._args.get('yml.patch_overlap', 0.1)

        shutil.copy2(config_template, config_file)

        with open(config_file, 'r', encoding='utf-8') as handle:
            lines = handle.readlines()

        updates = [
            ('general', 'training_directory', json.dumps(training_directory)),
            ('model_training', 'learning_rate', learning_rate),
            ('model_training', 'max_epochs_per_iteration', max_epochs),
            ('data_loading', 'batch_size', batch_size),
            ('data_loading', 'patch_size', patch_size),
            ('data_loading', 'steps_per_epoch', steps_per_epoch),
            ('tilt_series_alignment', 'patch_size', patch_size),
            ('tilt_series_alignment', 'patch_overlap', alignment_patch_overlap),
            ('tilt_series_alignment', 'batch_size', batch_size),
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
            f'alignment_batch_size={batch_size}'
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

        anchoring_iterations = int(self._args.get('infer.yml.iterations_anchoring', 2))
        global_iterations = int(self._args.get('infer.yml.iterations_global', 2))
        spline_iterations = int(self._args.get('infer.yml.iterations_spline', 4))

        alignment_patch_size = self._args.get('yml.patch_size', 96)
        alignment_batch_size = self._args.get('yml.batch_size', 32)
        alignment_patch_overlap = self._args.get('yml.patch_overlap', 0.1)

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

        return config_file, n_iterations

    # ------------------------------------------------------------------
    # Miss-Alignment training
    # ------------------------------------------------------------------
    @staticmethod
    def _parse_gpu_device_list(value, name, available_gpus, allow_repeats=False):
        """Parse comma-separated physical GPU IDs.
        Parameters
        - value: User-provided GPU list, e.g. ``"2,5"`` or ``"7,7,9,9"``.
        - name: Parameter name used in error messages.
        - available_gpus: Physical GPUs reserved by EMHub, e.g. ``[2, 5, 7, 9]``.
        - allow_repeats: Whether the same physical GPU may appear more than once.
        Reconstruction allows this because every entry creates a worker.
        """
        if value is None:
            return []

        text = str(value).strip()
        if not text:
            return []

        devices = []

        for token in text.split(','):
            token = token.strip()

            if not token:
                raise ValueError(f'{name} contains an empty GPU device entry: {value!r}')

            try:
                device = int(token)
            except ValueError as exc:
                raise ValueError(
                    f'{name} must be a comma-separated list of GPU device '
                    f'indices, received: {value!r}'
                ) from exc

            if device not in available_gpus:
                raise ValueError(
                    f'{name} requests GPU {device}, but the GPUs reserved '
                    f'for this EMHub job are: '
                    f'{",".join(str(gpu) for gpu in available_gpus)}'
                )

            devices.append(device)

        if not allow_repeats and len(devices) != len(set(devices)):
            raise ValueError(
                f'{name} contains repeated GPU devices: {value!r}. '
                'Training uses one worker per GPU, so training devices '
                'must be unique.'
            )

        return devices

    def _training_gpu_devices(self):
        """Build Miss-Alignment logical GPU assignments.
        self.gpuList contains the physical GPU IDs reserved by EMHub.
        The form parameters also use physical GPU IDs. For example, with:
            self.gpuList = [2, 5, 7, 9]
        the user may request::
            train.training_gpus = "2,5"
            train.reconstruction_gpus = "7,9"

        Because CUDA_VISIBLE_DEVICES is set to ``2,5,7,9``, Miss-Alignment sees
        these GPUs as logical devices ``0,1,2,3``. Therefore the physical IDs
        selected in the form are translated to their corresponding logical IDs
        before building the Miss-Alignment command.

        If training devices are empty, the first half of the reserved GPUs is
        used for training.

        If reconstruction devices are empty, the remaining GPUs are used with
        three workers per GPU. If no separate reconstruction GPU is available,
        reconstruction shares the training GPU(s).
        """
        if not self.gpuList:
            raise ValueError('Miss-Alignment training requires at least one GPU.')

        physical_gpus = [int(gpu) for gpu in self.gpuList]

        # CUDA_VISIBLE_DEVICES maps physical -> logical GPU IDs.
        # Example:
        #   CUDA_VISIBLE_DEVICES=2,5,7,9
        # gives:
        #   physical 2 -> logical 0
        #   physical 5 -> logical 1
        #   physical 7 -> logical 2
        #   physical 9 -> logical 3
        physical_to_logical = {
            physical: logical
            for logical, physical in enumerate(physical_gpus)
        }

        # --------------------------------------------------------------
        # Training and Reconstruction GPUs
        # --------------------------------------------------------------
        training_physical = self._parse_gpu_device_list(
            self._args.get('train.training_gpus', ''),
            'train.training_gpus',
            physical_gpus,
            allow_repeats=False,
        )

        reconstruction_physical = self._parse_gpu_device_list(
            self._args.get('train.reconstruction_gpus', ''),
            'train.reconstruction_gpus',
            physical_gpus,
            allow_repeats=True,
        )

        if (not training_physical and reconstruction_physical) or (training_physical and not reconstruction_physical):
            raise ValueError('Miss-Alignment requires that both training and reconstructions GPUs are set, leave both empty for automatic estimation.')
        
        if not training_physical:
            if len(physical_gpus) == 1:
                training_physical = physical_gpus[:]
            else:
                # Prioritize training when the number of GPUs is odd.
                n_training = max(1, (len(physical_gpus) + 1) // 2)
                training_physical = physical_gpus[:n_training]
       
        if not reconstruction_physical:
            # Prefer GPUs not used for training.
            reconstruction_gpus = [
                gpu
                for gpu in physical_gpus
                if gpu not in training_physical
            ]

            # Single GPU / all GPUs explicitly used for training:
            # allow reconstruction to overlap with training.
            if not reconstruction_gpus:
                reconstruction_gpus = list(training_physical)

            workers_per_gpu = 3

            reconstruction_physical = [
                gpu
                for gpu in reconstruction_gpus
                for _ in range(workers_per_gpu)
            ]

        # --------------------------------------------------------------
        # Translate physical GPU IDs into CUDA-visible logical IDs.
        # --------------------------------------------------------------
        training_logical = [
            physical_to_logical[gpu]
            for gpu in training_physical
        ]

        reconstruction_logical = [
            physical_to_logical[gpu]
            for gpu in reconstruction_physical
        ]

        training_devices = ','.join(str(device) for device in training_logical)
        reconstruction_devices = ','.join(str(device) for device in reconstruction_logical)

        self.log(
            'Miss-Alignment GPU mapping: '
            f'physical={physical_gpus}; '
            f'training physical={training_physical} -> '
            f'logical={training_logical}; '
            f'reconstruction physical={reconstruction_physical} -> '
            f'logical={reconstruction_logical}'
        )

        return training_devices, reconstruction_devices

    def _run_miss_alignment(self, mode, batch, config_file, extra_args):
        omp_threads = 1
        mkl_threads = 1
        nccl_p2p_disable = 1 # force NCCL to use the shared-memory transport
        # The shared-memory path is slightly lower bandwidth than direct P2P, 
        # but is stable across all PCIe topologies and typically has negligible impact on overall training time. 

        if not self.gpuList:
            raise ValueError('Miss-Alignment requires at least one GPU.')

        if isinstance(self.gpuList, str):
            visible_devices = self.gpuList.strip().replace(' ', ',')
        else:
            visible_devices = ','.join(str(device) for device in self.gpuList)

        self.log(
            f'Miss-Alignment {mode}, GPU allocation: '
            f'CUDA_VISIBLE_DEVICES={visible_devices};'
        )

        # ENV Variables
        args = Args({
            'env': '',
            f'NCCL_P2P_DISABLE={nccl_p2p_disable}': '', 
            f'OMP_NUM_THREADS={omp_threads}': '',
            f'MKL_NUM_THREADS={mkl_threads}': '',
            f'CUDA_VISIBLE_DEVICES={visible_devices}': ''
        })

        if self.scratchDir:
            args.update({
                f'TMPDIR={self._get_tmpdir()}': '',
            })

        args.update({
            'miss-alignment': '',
            mode: '',
            '--config-file': config_file,
            })

        args.update(extra_args)
        try:
            label = f'miss_alignment_{mode}'
            self.log(f'Running {label}, args: {args}')
            self.batch_execute(
                label,
                batch,
                args,
                launcher=self._get_launcher(),
            )
        finally:
            self._remove_tmpdir_link()

    def _run_miss_alignment_train(self, batch, config_file):
        """Launch Miss-Alignment training through the configured launcher."""
        training_devices, reconstruction_devices = self._training_gpu_devices()

        extra_args = {
            '--training-devices': training_devices,
            '--reconstruction-devices': reconstruction_devices
        }

        extra_args.update(self._get_args('train.missalign'))
        extra_args.update(self._get_args('missalign')) # common params (prepare-stack)

        
        self._run_miss_alignment('train', batch, config_file, extra_args)

    # ------------------------------------------------------------------
    # Miss-Alignment inference
    # ------------------------------------------------------------------
    def _run_miss_alignment_infer(self, batch, config_file):
        """Launch Miss-Alignment inference on all GPUs visible to the job."""
        extra_args = self._get_args('infer.missalign')
        extra_args.update(self._get_args('missalign')) # common params (prepare-stack)

        self._run_miss_alignment('infer', batch, config_file, extra_args)

    def _validate_inference_output(self, data_directory, n_iter):
        """Ensure an output directory exists for every configured iteration."""
        missing = [
            os.path.join(data_directory, f'iter{iteration}')
            for iteration in range(n_iter)
            if not os.path.isdir(os.path.join(data_directory, f'iter{iteration}'))
        ]
        if missing:
            raise RuntimeError(
                'Miss-Alignment inference did not produce complete output; '
                'missing iteration directories: ' + ', '.join(missing)
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
        self._run_miss_alignment_train(batch, config_file)

        trainingBestModel = os.path.join(training_directory, 'model.ckpt')
        
        if not os.path.isfile(trainingBestModel):
            raise FileNotFoundError(
                'Miss-Alignment training finished but the best model was not found: '
                f'{trainingBestModel}')

        self.log(
            f'Miss-Alignment training finished. '
            f'Best model: {trainingBestModel}')

        self.updateBatchInfo(batch)

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
        config_file, n_iter = self._update_inference_config_yaml(
            data_directory,
            model_run_directory)

        self._run_miss_alignment_infer(batch, config_file)

        self._validate_inference_output(data_directory, n_iter)
        
        self._write_imod_xfs(data_directory, geometry['pixel_size'])

        self.log(
            'Miss-Alignment inference finished. '
            f'Aligned snapshots are in: {data_directory}/iterN/')

        # Convert the optimized global XF transforms back into the RELION 5
        # alignment columns and register a new aligned_tilt_series.star.
        output_star, individual_stars = self._build_relion_output_metadata(batch, geometry['pixel_size'])
        self.updateBatchInfo(batch)

        return output_star

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
        """Copy one input TS STAR while replacing only RELION alignment fields.

        Warp XML/XF entries are matched to RELION rows by movie basename rather
        than by row position. The output STAR keeps the original RELION row
        order.
        """
        ts_name = str(ts_row.rlnTomoName)
        input_star = ts_row.rlnTomoTiltSeriesStarFile

        if not input_star or not os.path.isfile(input_star):
            raise FileNotFoundError(
                f'Input tilt-series STAR not found for {ts_name}: {input_star}')

        input_table = StarFile.getTableFromFile(ts_name, input_star)
        if len(input_table) == 0:
            raise ValueError(
                f'Input tilt-series STAR is empty for {ts_name}: {input_star}')

        data_directory = os.path.abspath(self.join(self.TS))
        xml_file = os.path.join(data_directory, f'{ts_name}.xml')
        xf_file = os.path.join(data_directory, f'{ts_name}.xf')

        if not os.path.isfile(xml_file):
            raise FileNotFoundError(
                f'Miss-Alignment Warp XML not found for {ts_name}: {xml_file}')

        if not os.path.isfile(xf_file):
            raise FileNotFoundError(
                f'Miss-Alignment XF file not found for {ts_name}: {xf_file}')

        # Movie order in the Warp XML is the order used to write the XF.
        warp_movie_names = get_warp_movie_names(xml_file)

        # Build a lookup from movie basename to the corresponding RELION row.
        star_rows_by_movie = {}

        for tilt_row in input_table:
            tilt_dict = tilt_row._asdict()
            movie_path = tilt_dict.get('rlnMicrographMovieName', None)

            if movie_path in ('', None):
                raise ValueError(
                    f'{ts_name}: rlnMicrographMovieName is required to match '
                    'RELION rows to Warp MoviePath entries.')

            movie_name = os.path.basename(str(movie_path))

            if movie_name in star_rows_by_movie:
                raise ValueError(
                    f'{ts_name}: duplicate rlnMicrographMovieName basename: '
                    f'{movie_name}')

            star_rows_by_movie[movie_name] = tilt_row

        # Require an exact one-to-one identity match between Warp and RELION.
        warp_movies = set(warp_movie_names)
        star_movies = set(star_rows_by_movie)

        missing_from_star = sorted(warp_movies - star_movies)
        missing_from_warp = sorted(star_movies - warp_movies)

        if missing_from_star or missing_from_warp:
            raise ValueError(
                f'{ts_name}: Warp/RELION movie identity mismatch. '
                f'Missing from STAR: {missing_from_star}; '
                f'Missing from Warp XML: {missing_from_warp}')

        # RelionStar.alignments_from_imod is positional, so build the tilt-angle
        # list in the exact Warp/XF order.
        # NOTE: We intentionally keep rlnTomoNominalStageTiltAngle here for now.
        # Choosing between RELION nominal angles and Warp XML Angles is handled
        # as a separate alignment-convention issue.
        tilt_angles = []

        for movie_name in warp_movie_names:
            tilt_row = star_rows_by_movie[movie_name]
            tilt_dict = tilt_row._asdict()
            tilt_angle = tilt_dict.get('rlnTomoNominalStageTiltAngle', None)

            if tilt_angle in ('', None):
                raise ValueError(
                    f'{ts_name}: rlnTomoNominalStageTiltAngle is required '
                    f'for movie {movie_name}.')

            tilt_angles.append(float(tilt_angle))

        relion_alignments = self._compute_relion_alignments_from_xf(
            xf_file,
            tilt_angles,
            pixel_size,
        )

        # Associate each converted alignment with its movie identity.
        alignments_by_movie = {
            movie_name: alignment
            for movie_name, alignment in zip(
                warp_movie_names,
                relion_alignments,
            )
        }

        alignment_columns = (
            'rlnTomoXTilt',
            'rlnTomoYTilt',
            'rlnTomoZRot',
            'rlnTomoXShiftAngst',
            'rlnTomoYShiftAngst',
        )

        input_columns = input_table.getColumnNames()
        output_table = Table(input_columns)

        # Preserve the original STAR row order, but look up each alignment by
        # movie identity instead of assuming STAR and XF have the same order.
        for tilt_row in input_table:
            tilt_dict = tilt_row._asdict()
            movie_name = os.path.basename(str(tilt_dict['rlnMicrographMovieName']))
            alignment = alignments_by_movie[movie_name]

            for column in alignment_columns:
                if column not in alignment:
                    raise ValueError(
                        f'{ts_name}: converted RELION alignment is missing '
                        f'{column} for movie {movie_name}.'
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

    # ------------------------------------------------------------------
    # Pipeline lifecycle
    # ------------------------------------------------------------------
    def prerun(self):
        self.inputTs = self._args['input_tiltseries']
        self.n_training = int(self._args.get('train.n_training', 10))
        mode = self._get_mode()
        do_train = mode in [self.MODE_TRAIN_ONLY, self.MODE_TRAIN_INFER]
        do_infer = mode in [self.MODE_INFER_ONLY, self.MODE_TRAIN_INFER]

        # TODO: remove once scheduler is ready
        self.inputTsTable = self._wait_for_input_table()
        self.log(f'Found input tilt series: {len(self.inputTsTable)}')
        output_nodes = []

        if do_train:
            training_subset = self._wait_for_training_set()
            self.log(f'Starting training with {len(training_subset)} tilt series')
            model_run_directory = self.launch_training(training_subset)
            output_nodes.append([
                model_run_directory,
                'TomogramGroupMetadata.star.relion.tomo.MissAlignmentModelDir',
            ])
        else:
            model_run_directory = str(self._args.get('infer.model_run_directory', ''))
            self.log(f'Skipping training. Input model provided from directory: {model_run_directory}')
        
        if do_infer:
            output_star = self.launch_inference(model_run_directory, mode)
            output_nodes.append([
                output_star,
                'TomogramGroupMetadata.star.relion.tomo.aligntiltseries',
            ])

        self.writeRelionOutputNodes(output_nodes)


if __name__ == '__main__':
    MissAlignment.main()