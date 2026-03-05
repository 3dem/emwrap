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

from emtools.utils import Color, Timer, Path, FolderManager
from emtools.jobs import Args
from emtools.metadata import Table, StarFile, TextFile, Acquisition
from emtools.image import Image


class Motioncor:
    """ Motioncor wrapper to run in a batch folder. """
    def __init__(self, acq, **kwargs):
        if path := kwargs.get('path', None):
            self.path = path
            self.version = int(kwargs['version'])
        else:
            self.path, self.version = Motioncor.__get_environ()
        self.ctf = kwargs.get('ctf', False)
        self.framesPerMovie = kwargs['movieDims'][2]
        self.minFractionDose = float(kwargs['min_fraction_dose'])

        # Get from project acquisition settings: PixSize, kV, Cs, AmpCont, Gain
        self.acq = Acquisition(acq)
        self.args = self.argsFromAcq(acq)

        # Get from protocol GUI
        self.frameDose = 0.0
        self.argsFromGui(kwargs)
        self.outputPrefix = "output/aligned_"

    @property
    def bin(self):
        return self.args.get('-FtBin', 1.0)

    @property
    def local_alignment(self):
        return self.args.get('-Patch', '1 1') != '1 1'

    def process_batch(self, batch, **kwargs):
        gpu = kwargs['gpu']

        outputDir = batch.mkdir('output')
        logDir = batch.mkdir('log')
        ext = Path.getExt(batch['items'][0]['rlnMicrographMovieName'])
        extLower = ext.lower()

        kwargs = {
            '-OutMrc': self.outputPrefix, '-InSuffix': ext,
            '-Serial': 1, '-Gpu': gpu, '-LogDir': 'log/'
        }
        if extLower.startswith('.tif'):
            inArg = '-InTiff'
        elif extLower.startswith('.mrc'):
            inArg = '-InMrc'
        elif extLower.startswith('.eer'):
            inArg = '-InEer'
            kwargs['-EerSampling'] = 1
            self.make_frame_integration(batch)
        else:
            raise Exception(f"Unsupported movie format: {ext}")

        kwargs[inArg] = './movie'
        kwargs.update(self.args)

        t = Timer()
        batch.call(self.path, kwargs)

        batch.info.update({
            'mc_input': len(batch['items']),
            'mc_elapsed': str(t.getElapsedTime())
        })

        batch['results'] = []
        batch['outputs'] = []
        total = 0

        def _rename(output, outputPrefix):
            """ Rename files in that directory. """
            fm = FolderManager(output)
            for fn in fm.listdir():
                if fn.startswith(outputPrefix):
                    fm.rename(fn, fn.replace(outputPrefix, 'micrograph-'))

        _rename(outputDir, 'aligned_-')
        _rename(logDir, 'movie-')

        for row in batch['items']:
            result = {}
            try:
                movieName = row['rlnMicrographMovieName']
                baseName = Path.removeBaseExt(movieName)
                # For tomo, we rename the links, but the movie names do not
                # start with 'movie'
                if baseName.startswith('movie-'):
                    baseName = baseName.replace('movie-', 'micrograph-')
                else:
                    baseName = 'micrograph-' + baseName

                suffix = '_DW' if '-FmDose' in self.args else ''
                # TODO: Allow an option to save non-DW movies if required
                micName = batch.join('output', f"{baseName}{suffix}.mrc")

                # Check that the expected output micrograph file exists
                # and move it to the final output directory
                self.__expect(micName)
                batch['outputs'].append(micName)
                result['rlnMicrographName'] = micName

                if self.local_alignment:
                    logsFull = batch.join('log', f"{baseName}-Patch-Full.log")
                    logsPatch = batch.join('log', f"{baseName}-Patch-Patch.log")
                else:
                    logsFull = batch.join('log', f"{baseName}-Full.log")
                    logsPatch = None

                shiftsStar = Path.replaceExt(micName, '.star')
                batch['outputs'].append(shiftsStar)
                self.__write_shift_star(logsFull, logsPatch, movieName, micName, shiftsStar)
                result['rlnMicrographMetadata'] = shiftsStar
                total += 1

            except Exception as e:
                result['error'] = str(e)
                print(Color.red(f"ERROR: {result['error']}"))

            batch['results'].append(result)

        batch.info.update({
            'mc_output': total
        })

    def make_frame_integration(self, batch):
        """Create frame integration file for EER files."""

        self.frame_file = os.path.join(batch.path, 'motioncor-frame.txt')
        numFramesToCombine = max( int(self.minFractionDose/self.frameDose), 1)
        with open(self.frame_file, 'w') as f:
            f.write(f"{self.framesPerMovie} {numFramesToCombine} {self.frameDose:0.3f}")
        self.args['–FmIntFile'] = self.frame_file

    def __expect(self, fileName):
        if not os.path.exists(fileName):
            raise Exception(f"Missing expected output: {fileName}")

    def __write_shift_star(self, logsFull, logsPatch, movieName, micName, shiftsStar):
        # Parse global motion movements
        self.__expect(logsFull)
        tGeneral = Table(
            ['rlnImageSizeX', 'rlnImageSizeY', 'rlnImageSizeZ',
             'rlnMicrographMovieName', 'rlnMicrographBinning',
             'rlnMicrographOriginalPixelSize', 'rlnMicrographDoseRate',
             'rlnMicrographPreExposure', 'rlnVoltage',
             'rlnMicrographStartFrame', 'rlnMotionModelVersion'
             ])
        x, y = Image.get_dimensions(micName)
        tGeneral.addRowValues(x, y, 1, movieName,
                              self.bin,
                              self.acq.pixel_size, 1.0, 0.0,
                              self.acq.voltage, 1, 0)

        t = Table(['rlnMicrographFrameNumber',
                   'rlnMicrographShiftX',
                   'rlnMicrographShiftY'])

        for line in TextFile.stripLines(logsFull):
            t.addRowValues(*line.split())

        with StarFile(shiftsStar, 'w') as sf:
            sf.writeTimeStamp()
            sf.writeTable('general', tGeneral, singleRow=True)
            sf.writeTable('global_shift', t)

            # Parse local motions
            if logsPatch:
                self.__expect(logsPatch)
                t = Table(['rlnMicrographFrameNumber',
                           'rlnCoordinateX', 'rlnCoordinateY',
                           'rlnMicrographShiftX', 'rlnMicrographShiftY'])
                for line in TextFile.stripLines(logsPatch):
                    parts = line.split()
                    t.addRowValues(*parts[:5])
                sf.writeTable('local_shift', t)

    @staticmethod
    def __get_environ():
        varPath = 'MOTIONCOR_PATH'
        varVersion = 'MOTIONCOR_VERSION'

        if program := os.environ.get(varPath, None):
            if not os.path.exists(program):
                raise Exception(f"Motioncor path ({varPath}={program}) does not exists.")
        else:
            raise Exception(f"Motioncor path variable {varPath} is not defined.")

        if version := int(os.getenv(varVersion, 3)):
            pass
        else:
            raise Exception(f"Motioncor version variable {varVersion} is not defined.")

        return program, version

    @staticmethod
    def __parse_dimensions(logFile):
        """ Parse output dimensions from the log file. """
        with open(logFile) as f:
            for line in f:
                if 'size mode:' in line:
                    return line.split(':')[-1].split()
        return None

    def argsFromAcq(self, acq):
        """
        Add arguments from acquisition parameters:
            PixSize
            kV
            Cs
            AmpCont
            Gain (can be overridden from protocol GUI)
        """

        args = Args({
            '-PixSize': acq.pixel_size,
            '-kV': acq.voltage,
            '-Cs': acq.cs,
            '-AmpCont': acq.amplitude_contrast,
        })
        if gain := acq.get('gain', None):
            args['-Gain'] = gain

        return args

    def argsFromGui(self, kwargs):
        """Add arguments from GUI entry."""

        #del kwargs['input_star_mics']
        #del kwargs['gpu_ids']
        #del kwargs['__j']
        ###print(f"\n{os.path.basename(__file__)}:248: self.args({type(self.args)})='{self.args}'")
        self.args.update(kwargs.get('extra_args', {}))
        ##del kwargs['extra_args']
        ###print(f"\n{os.path.basename(__file__)}:251: kwargs({type(kwargs)})='{kwargs}'")

        self.args['-Patch'] = f"{kwargs['patch_x']} {kwargs['patch_y']}"
        ###del kwargs['patch_x'] ; del kwargs['patch_y']
        if kwargs['gain_rot'] != '0'  : self.args['-RotGain']  = kwargs['gain_rot']
        if kwargs['gain_flip'] != '0' : self.args['-FlipGain'] = kwargs['gain_flip']
        ###del kwargs['gain_rot'] ; del kwargs['gain_flip']
        if kwargs['fn_defect'] : self.args['-DefectFile'] = kwargs['fn_defect']
        if int(kwargs['reference_frame']) > 0 : self.args['-FmRef'] = kwargs['reference_frame']
        ###del kwargs['fn_defect'] ; del kwargs['reference_frame']
        if kwargs['do_split_sum'] : self.args['-SplitSum'] = 1
        ###del kwargs['do_split_sum']
        if kwargs['num_iters'] : self.args['-Iter'] = kwargs['num_iters']
        if kwargs['err_tolerance'] : self.args['-Tol'] = kwargs['err_tolerance']
        ###del kwargs['num_iters'] ; del kwargs['err_tolerance']

        # If Kv, PixSize, and FmDose are provided, then dose-weighted sums are generated
        if kwargs['do_dose_weighting'] : self.args['-FmDose'] = kwargs['dose_per_frame']
        self.frameDose = float(kwargs['dose_per_frame'])
        ###del kwargs['dose_per_frame']

        # Override gain reference if provided in protocol GUI
        if kwargs['fn_gain_ref'] : self.args['-Gain'] = kwargs['fn_gain_ref']
        ###del kwargs['fn_gain_ref'] ; print(f"\n{os.path.basename(__file__)}:274: kwargs({type(kwargs)})='{kwargs}'")

        return
