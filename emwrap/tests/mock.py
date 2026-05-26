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

import os
import glob
import sys
import shutil
from pprint import pprint
from emtools.utils import Color, Process, FolderManager
from emtools.jobs import Args
from emtools.metadata import Table, Acquisition, StarFile, RelionStar, WarpPopulation
from emtools.jobs import BatchManager


class MockWarpApoF:
    # FIXME: Read this from the config
    path = '/Users/jdela80/work/data/TOMO/WarpTutorial2'
    fm = FolderManager(path)

    acquisition = Acquisition(
        pixel_size=0.885,
        voltage=200,
        cs=1.4,
        amplitude_contrast=0.1
    )

    def _join(self, *args):
        return os.path.join(self.path, *args)

    def _copy(self, src, dst):
        shutil.copy(self._join(src), dst)

    def _link(self, src, dst):
        """Create a symlink at dst pointing to src."""
        dst_dir = os.path.dirname(dst)
        if dst_dir:
            os.makedirs(dst_dir, exist_ok=True)
        if os.path.lexists(dst):
            os.remove(dst)
        link_target = os.path.relpath(src, dst_dir or '.')
        os.symlink(link_target, dst)

    def _copyFolder(self, src, dst, link_files=True):
        binary_extensions = {'.mrc', '.mrcs', '.tiff', '.png', '.tif'}
        base_src = os.path.basename(src)
        dst_folder = os.path.join(dst, base_src)
        os.makedirs(dst_folder, exist_ok=True)

        if link_files:
            os.makedirs(dst, exist_ok=True)
            for root, _, files in os.walk(src):
                rel_root = os.path.relpath(root, src)
                dst_root = os.path.join(dst_folder, rel_root)
                os.makedirs(dst_root, exist_ok=True)
                for fn in files:
                    src_file = os.path.join(root, fn)
                    dst_file = os.path.join(dst_root, fn)
                    ext = os.path.splitext(fn)[1].lower()
                    if ext in binary_extensions:
                        self._link(src_file, dst_file)
                    else:
                        shutil.copy2(src_file, dst_file)
        else:
            os.system(f"rsync -av '{src}/' '{dst_folder}/'")

    def _argsCompare(self, msg, args, data, skip_keys=[]):
        """ Compare reference arguments with the given arguments.
        """
        ref_args = Args.fromString(data['args'])

        for k in skip_keys:
            args.pop(k, None)
            ref_args.pop(k, None)

        if args != ref_args:
            keys = set(args.keys())
            ref_keys = set(ref_args.keys())
            if missing_keys := ref_keys - keys:
                raise Exception(f"{msg}: missing arguments: {missing_keys}")
            if extra_keys := keys - ref_keys:
                raise Exception(f"{msg}: extra arguments: {extra_keys}")
            diff_values = {k: f'{args[k]} != {ref_args[k]}' for k in keys if args[k] != ref_args[k]}
            if diff_values:
                from pprint import pprint
                print(f">>> {msg}: invalid arguments:")
                pprint(diff_values)
                raise Exception(f"{msg}: invalid arguments")

    def _runWarpTools(self, args):
        data_map = {
            'create_settings': {
                'warp_frameseries.settings': {
                    'run': "External/job004",
                    'args': "--folder_data frames --extension *.tif --folder_processing warp_frameseries --output warp_frameseries.settings --angpix 0.789 --exposure 2.64 --gain_path gain_ref.mrc --gain_flip_y"
                },
                'warp_tiltseries.settings': {
                    'run': "External/job005",
                    'args': "--folder_data warp_tomostar --extension *.tomostar --folder_processing warp_tiltseries --output warp_tiltseries.settings --angpix 0.789 --exposure 2.64 --tomo_dimensions 4400x6000x1000"
                }
            },
            'fs_motion_and_ctf': {
                'run': "External/job004",
                'args': " --settings warp_frameseries.settings --c_voltage 300 --c_cs 2.7 --c_amplitude 0.1 --out_averages --m_grid 1x1x3 --c_grid 2x2x1 --m_range_min 500 --m_range_max 10 --m_bfac -500 --c_range_min 30 --c_range_max 7 --c_defocus_min 0.5 --c_defocus_max 8 --c_window 512 --c_use_sum"
            },
            'ts_import': {
                'run': "External/job005",
                'args': "--frameseries warp_frameseries --tilt_exposure 2.64 --output warp_tomostar --mdocs mdocs --min_ntilts 3 --override_axis -85.6"
            },
            'ts_aretomo': {
                'run': "External/job005",
                'args': "--settings warp_tiltseries.settings --alignz 800 --angpix 10 --axis_iter 0 --axis_batch 0"
            },
            'ts_ctf': {
                'run': "External/job006",
                'args': "--settings warp_tiltseries.settings --voltage 300 --cs 2.7 --amplitude 0.1 --auto_hand 8 --window 512 --range_low 30 --range_high 7 --defocus_min 0.5 --defocus_max 8"
            },
            'ts_reconstruct': {
                'run': "External/job006",
                'args': "--settings warp_tiltseries.settings --angpix 10"
            },
            'ts_export_particles': {
                'run': "External/job008",
                'args': "--settings warp_tiltseries.settings --diameter 140 --output_angpix 4 --box 64 --input_star all_coordinates.star --coords_angpix 10.0 --output_star warp_particles.star --output_processing Particles --2d"
            }
        }
        print(f">>> Running {Color.blue('WarpTools')} with arguments: {args}")
        cmd = args[0]
        if cmd not in data_map:
            raise Exception(f">>> {Color.red('Error')} program {cmd} not found.")
        
        data = data_map[cmd]
        args = Args.fromList(args[1:])
        skip_keys = ['--exe', '--device_list', '--perdevice']

        if cmd == 'create_settings':
            # We need to determine if it is the frameseries or the tiltseries
            output = args['--output']
            if data := data.get(output, None):
                self._argsCompare(f"WarpTools create_settings", args, data)
                # Copy the reference settings file
                self._copy(f"{data['run']}/{output}", output)
            else:
                raise Exception(f">>> {Color.red('Error')} output {output} not found in data.")

        elif cmd == 'fs_motion_and_ctf':
            self._argsCompare(f"WarpTools fs_motion_and_ctf", args, data, skip_keys=skip_keys)
            # Copy output folder
            self._copyFolder(self._join(data['run'], 'warp_frameseries'), ".")  

        elif cmd == 'ts_import':
            self._argsCompare(f"WarpTools ts_import", args, data, skip_keys=skip_keys)
            # Copy output folder
            self._copyFolder(self._join(data['run'], 'warp_tomostar'), ".")  

        elif cmd == 'ts_aretomo':
            self._argsCompare(f"WarpTools ts_aretomo", args, data, skip_keys=skip_keys)
            # Copy output folder
            self._copyFolder(self._join(data['run'], 'warp_tiltseries'), ".")  

        elif cmd == 'ts_ctf': 
            self._argsCompare(f"WarpTools ts_ctf", args, data, skip_keys=skip_keys)
            # Copy output folder
            self._copyFolder(self._join(data['run'], 'warp_tiltseries', 'powerspectrum'), "warp_tiltseries") 

        elif cmd == 'ts_reconstruct': 
            self._argsCompare(f"WarpTools ts_reconstruct", args, data, skip_keys=skip_keys)
            # Copy output folder
            self._copyFolder(self._join(data['run'], 'warp_tiltseries', 'reconstruction'), "warp_tiltseries")  
        elif cmd == 'ts_export_particles':            
            self._argsCompare(f"WarpTools ts_export_particles", args, data, skip_keys=skip_keys)
            # Copy output folder
            self._copyFolder(self._join(data['run'], 'Particles'), "Particles")  
            # Copy the output star files
            for suffix in ['_optimisation_set', '', '_tomograms']:
                self._copy(f"{data['run']}/warp_particles{suffix}.star", f"warp_particles{suffix}.star")
        else:
            raise Exception(f">>> {Color.red('Error')} WarpTools command {cmd} not found.")

    def _runPyTom(self, args):
        data = {
            'run': "External/job007",
            'args': "--voltage 300.0 --spherical-aberration 2.7 --amplitude-contrast 0.1 --voxel-size-angstrom 10.0 --destination output --low-pass 20 --high-pass 400 --random-phase-correction --per-tilt-weighting --tomogram-ctf-model phase-flip --template emd_15854_10Apx.mrc --mask emd_15854_10Apx_mask.mrc --angular-search 20"
        }
        args = Args.fromList(args)
        tomogram = args['--tomogram']
        tomoPrefix = tomogram.replace('.mrc', '')
        skip_keys = ['--tomogram', '--tilt-angles', '--dose-accumulation', '--defocus', '--g']
        self._argsCompare(f"PyTom pytom_match_template", args, data, skip_keys=skip_keys)
        # Only copy the results files for this tomogram
        pattern = self._join(data['run'], 'Coordinates', f'{tomoPrefix}*')
        for fn in glob.glob(pattern):
            self._copy(fn, 'output')

        #self._copy(os.path.join(data['run'], 'Coordinates', f'{tomoPrefix}*'), 'output')

        
    def _runRelion(self, program, args):
        print(f">>> Running {Color.blue(program)} with arguments: {args}")
        
        skip_keys = ['--gpu', '--j', '--ios', '--pool', '--dont_combine_weights_via_disc', '--i', '--o', '--nr_pool', '--ref']
        args = Args.fromList(args[1:])  # Ignore first argument (MPI processes)
        from pprint import pprint
        pprint(args)

        if program == 'relion_refine':
            output_run = os.path.dirname(os.path.dirname(args['--o']))

            if '--denovo_3dref' in args:  # tomo abinitio refinement
                data = {
                    'run': "External/job009",
                    'args': "--o output/run --grad --denovo_3dref --pad 1 --oversampling 1 --healpix_order 1 --offset_range 6 --offset_step 2 --auto_sampling --zero_mask --j 5 --gpu --ctf --iter 100 --tau2_fudge 4 --K 1 --particle_diameter 140 --flatten_solvent --sym O"
                }
                self._argsCompare(f"Relion relion_refine", args, data, skip_keys=skip_keys)                
                self._copyFolder(self._join(data['run'], 'output'), output_run, link_files=True)

            elif '--auto_refine' in args:
                data = {
                    'run': "External/job010",
                    'args': "--auto_refine --split_random_halves --flatten_solvent --oversampling 1 --low_resol_join_halves 40 --norm --scale --offset_step 2 --trust_ref_size --ini_high 20 --sym O --ctf --particle_diameter 140 --zero_mask --healpix_order 2 --offset_range 5 --auto_local_healpix_order 4 --pad 2 --firstiter_cc"
                }
                self._argsCompare(f"Relion relion_refine", args, data, skip_keys=skip_keys)
                self._copyFolder(self._join(data['run'], 'output'), output_run, link_files=True)

            else:
                raise Exception(f">>> {Color.red('Error')} relion_refine case not identified by its params.")

        elif program == 'relion_align_symmetry':
            data = {
                'run': "External/job009",
                'args': "--sym C1 --apply_sym --select_largest_class"
            }
            self._argsCompare(f"Relion relion_align_symmetry", args, data, skip_keys=skip_keys)
            # We don't need to copy anything, since the output was already copied by relion_refine

        elif program == 'relion_mask_create':
            data = {
                'run': "External/job003",
                'args': "--ini_threshold 0.02 --i run_class001.mrc --o mask.mrc"
            }
            self._argsCompare(f"Relion relion_mask_create", args, data, skip_keys=skip_keys)
            # Copy the output mask
            self._copy(self._join(data['run'], 'mask.mrc'), ".")

        else:
            raise Exception(f">>> {Color.red('Error')} program {program} not found.")

        
    def _runMTools(self, args):
        command = args[0]
        data_map = {
            'create_population': {
                'run': "External/job011",
                'args': "--directory m --name ApoF"
            },
            'create_source': {
                'run': "External/job011",
                'args': "--population m/ApoF.population --name ApoF --processing_settings warp_tiltseries.settings"
            },
            'create_species': {
                'run': "External/job011",
                'args': "--population m/ApoF.population --sym O --name ApoF --diameter 140 --particles_relion run_data.star --mask mask.mrc --angpix_resample 0.7894 --lowpass 10 --half1 run_half1_class001_unfil.mrc --half2 run_half2_class001_unfil.mrc"
            }, 
            'resample_trajectories': {
                'run': "External/job021",
                'args': "--population m/ApoF.population --species m/species/ApoF_07e226b6/ApoF.species --samples 2"
            }
        }
        
        cmd = args[0]
        args = Args.fromList(args[1:])
        print(f">>> Running {Color.blue('MTools')} {cmd}, arguments:")
        pprint(args)


        if cmd not in data_map:
            raise Exception(f">>> {Color.red('Error')} program {cmd} not found.")
        data = data_map[cmd]
        
        skip_keys = ['--exe', '--device_list', '--perdevice']
        self._argsCompare(f"MTools {cmd}", args, data, skip_keys=skip_keys)

        if cmd in ['create_population', 'resample_trajectories']:
            self._copyFolder(self._join(data['run'], 'm'), ".")
        elif cmd in ['create_source', 'create_species']:
            pass  # all files were already copied by create_population
        else:
            raise Exception(f">>> {Color.red('Error')} MTools command {cmd} not found.")
        
    def _runMCore(self, args):
        
        # For MCore, the key will be the INPUT species version
        data_map = {
            'HCQJVjZM': {
                'run': "External/job012",
                'args': "--iter 0 "
            },
            'umKpaXPA': {
                'run': "External/job013",
                'args': "--refine_imagewarp 3x3 --refine_particles --ctf_defocus --ctf_defocusexhaustive"
            },
            'O3Yqek1e': {
                'run': "External/job014",
                'args': "--refine_imagewarp 3x3 --refine_particles --ctf_defocus"
            },
            'xS2y9iel': {
                'run': "External/job015",
                'args': "--refine_imagewarp 3x3 --refine_particles --refine_stageangles"
            },
            'iKtfhG2H': {
                'run': "External/job016",
                'args': "--refine_imagewarp 3x3 --refine_particles --refine_mag --ctf_defocus --ctf_cs --ctf_zernike3"
            },
            'UzJ_3hul': {
                'run': "External/job018",
                'args': ""
            },
            'Ju5dJxie': {
                'run': "External/job020",
                'args': "--refine_particles",
            },
            'DMsq7LJc': {
                'run': "External/job022",
                'args': " --refine_imagewarp 3x3 --refine_particles --refine_mag --refine_stageangles --ctf_defocus --ctf_cs --ctf_zernike3",
            }
        }
        args = Args.fromList(args)
        print(f">>> Running {Color.blue('MCore')}, arguments: ")
        pprint(args)

        skip_keys = ['--port', '--device_list', '--perdevice', '--perdevice_refine', '--population']
        population = WarpPopulation(args['--population'])
        species_version = population.getSpecies(0)['Version']

        print(f"Species: version = {population.getSpecies(0)['Version']}")

        if data := data_map.get(species_version, None):            
            self._argsCompare(f"MCore", args, data, skip_keys=skip_keys)
            # Copy the output folder
            self._copyFolder(self._join(data['run'], 'm'), ".")
        else:
            raise Exception(f">>> {Color.red('Error')} MCore invalid parameters for this step.")

    def _runEstimateWeights(self, args):
        """
        """
        args = Args.fromList(args)
        print(f">>> Running {Color.blue('EstimateWeights')} with arguments: {args}")

        skip_keys = ['--port', '--device_list', '--perdevice', '--perdevice_refine', '--population', '--source']
        population = WarpPopulation(args['--population'])
        species_version = population.getSpecies(0)['Version']

        print(f"Species: version = {population.getSpecies(0)['Version']}")
        data_map = {
            'UzJ_3hul': {
                'run': "External/job017",
                'args': "--resolve_items --source ApoF",
            },
            'Ju5dJxie': {
                'run': "External/job019",
                'args': "--resolve_frames --source ApoF",
            }
        }
        if data := data_map.get(species_version, None):
            self._argsCompare(f"EstimateWeights", args, data, skip_keys=skip_keys)
            # Copy the output folder
            self._copyFolder(self._join(data['run'], 'm'), ".")
        else:
            raise Exception(f">>> {Color.red('Error')} EstimateWeights invalid parameters for this step.")
        

    def main(self):
        print(f">>> This is a {Color.blue('EMWRAP_MOCKUP')} program, running on the server.")
        program = sys.argv[1]
        functionMap = {
            'WarpTools': '_runWarpTools',
            'MTools': '_runMTools',
            'MCore': '_runMCore',
            'EstimateWeights': '_runEstimateWeights',
            'pytom_match_template.py': '_runPyTom',
        }
        if program.startswith('relion_'):
            self._runRelion(program, sys.argv[2:])
        elif funcName := functionMap.get(program, None):
            getattr(self, funcName)(sys.argv[2:])
        else:
            raise Exception(f">>> {Color.red('Error')} program {program} not found.")


if __name__ == '__main__':
    try:    
        MockWarpApoF().main()
    except Exception as e:
        print(f">>> {Color.red('Error')} {e}\n")
        sys.exit(1)