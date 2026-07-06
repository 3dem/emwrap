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
from emtools.utils import Color, Timer, Path
from emtools.jobs import Args
from emtools.metadata import Table, StarFile, Acquisition

from emwrap.base import ProcessingPipeline


# TODO: 
# - Fix sampling size
# - 'rlnTiltSeriesAligned' is not aligned is the normal tilt series unaligned

class AreTomo3:
    """ AreTomo3 wrapper to run in a batch folder. """

    # Maps form param name -> CLI flag, for simple 1:1 scalar params.
    _SCALAR_FLAGS = {
        'fm_int': '-FmInt',
        'eer_sampling': '-EerSampling',
        'mc_bin': '-McBin',
        'align_z': '-AlignZ',
        'dark_tolerance': '-DarkTol',
        'ctf_amp_contrast': '-AmpContrast',
        'vol_z': '-VolZ',
        'extra_z': '-ExtZ',
        'tilt_cor': '-TiltCor',
        'out_imod': '-OutImod',
    }

    # Maps form param name -> CLI flag, for BooleanParam fields. Value is
    # the Python bool arriving from the form; serialized to '1'/'0'.
    _BOOLEAN_FLAGS = {
        'wbp': '-Wbp',
        'correct_ctf': '-CorrCTF',
        'flip_vol': '-FlipVol',
        'split_sum': '-SplitSum',
    }

    # Maps form param name -> CLI flag, for "XxY"-notation multi-value
    # strings (e.g. '5x5' -> '5 5'). Single bare numbers pass through
    # unchanged since there's no 'x' to split on.
    _GRID_FLAGS = {
        'mc_patch': '-McPatch',
        'at_patch': '-AtPatch',
        'mc_group': '-Group',
    }

    # Paired Line fields (two separate form keys) -> single CLI flag,
    # joined with a space. Second key is optional; if absent/empty,
    # only the first value is passed.
    _PAIRED_FLAGS = {
        '-AtBin': ('at_bin', 'at_bin2'),
        '-Sart': ('sart_iter', 'sart_proj'),
        '-ReconRange': ('recon_range_min', 'recon_range_max'),
    }

    def __init__(self, acq, **kwargs):
        self.acq = acq
        self.args = self.argsFromAcq(acq)
        form_args = kwargs.get('extra_args', kwargs)
        self.args.update(self._serialize_form_args(form_args))
        self.outputPrefix = "output/"
        self.launcher_aretomo3 = kwargs.get('launcher_aretomo3', None)

    @classmethod
    def _serialize_form_args(cls, formArgs):
        """ Translate flat form-param names/values (as defined in the
        EMHub form JSON) into AreTomo3 CLI flag/value pairs. """
        args = Args({})

        for name, flag in cls._SCALAR_FLAGS.items():
            if name in formArgs and formArgs[name] not in (None, ''):
                args[flag] = formArgs[name]

        for name, flag in cls._BOOLEAN_FLAGS.items():
            if name in formArgs and formArgs[name] is not None:
                args[flag] = 1 if formArgs[name] else 0

        for name, flag in cls._GRID_FLAGS.items():
            if name in formArgs and formArgs[name] not in (None, ''):
                args[flag] = str(formArgs[name]).replace('x', ' ').split()

        # This needs to be store as a list
        for flag, (firstKey, secondKey) in cls._PAIRED_FLAGS.items():
            first = formArgs.get(firstKey, None)
            second = formArgs.get(secondKey, None)
            if first not in (None, ''):
                if second not in (None, ''):
                    args[flag] = [str(first), str(second)]
                else:
                    args[flag] = [str(first)]

        # -TiltAxis: optional first value (angle), optional second value
        # (refinement mode).
        #   omitted entirely         -> AreTomo3 auto-searches full range
        #   angle + 0                -> AreTomo3 refines within ±10°
        #   angle + positive number  -> AreTomo3 refines within ±3°
        #   angle + negative number  -> AreTomo3 uses angle as-is, no refinement
        tiltAxis = formArgs.get('ts_import.override_axis', None)
        if tiltAxis not in (None, ''):
            tiltAxisRefine = formArgs.get('tilt_axis_refine', None)
            if tiltAxisRefine not in (None, ''):
                args['-TiltAxis'] = [str(tiltAxis), str(tiltAxisRefine)]
            else:
                args['-TiltAxis'] = [str(tiltAxis)]
                    
        # Any keys not recognized above pass straight through, in case
        # they're already raw CLI flags (e.g. from a previous version of
        # extra_args, or manually-entered '-Flag' keys in extra_a3_args).
        known = (set(cls._SCALAR_FLAGS) | set(cls._BOOLEAN_FLAGS) |
                set(cls._GRID_FLAGS) |
                {k for pair in cls._PAIRED_FLAGS.values() for k in pair} |
                {'ts_import.override_axis', 'tilt_axis_refine'})
        for key, value in formArgs.items():
            if key not in known and key.startswith('-'):
                args[key] = value

        return args

    # @property
    # def bin(self):
    #     return self.args.get('-McBin', 1.0)

    # @property
    # def at_bin(self):
    #     return self.args.get('-AtBin', '1.0')

    # @property
    # def local_alignment(self):
    #     return self.args.get('-McPatch', '1 1') != '1 1'

    @property
    def reconstruct(self):
        # VolZ must be > 0 (or -1 for auto-estimate) to produce a tomogram.
        # Default 0 means aligned tilt series only, no reconstruction.
        return str(self.args.get('-VolZ', 0)) != '0'

    @property
    def auto_estimate_thickness(self):
        """True when AreTomo3 is asked to automatically estimate the
        tomogram thickness (-VolZ -1)."""
        return str(self.args.get('-VolZ', '0')) == '-1'

    @property
    def ctf_estimation(self):
        return str(self.args.get('-CorrCTF', 1)) != '0'

    @property
    def split_sum(self):
        return str(self.args.get('-SplitSum', 1)) != '0'

    def _get_launcher(self):
        return self.launcher_aretomo3 or ProcessingPipeline.get_launcher('ARETOMO3')
    
    def process_batch(self, batch, **kwargs):
        gpu = kwargs['gpu']

        outputDir = batch.mkdir('output')
        logDir = batch.mkdir('log')
        tmpDir = batch.mkdir('tmp')

        items = batch['items']
        mdoc = batch['tsMdoc']
        mdocBase = os.path.basename(mdoc)
        tsName = Path.removeExt(mdocBase) # 'Position_46' from 'Position_46.mdoc'

        kwargs = {
            '-InPrefix': f'./{Path.removeExt(mdocBase)}',
            '-InSuffix': '.mdoc',
            '-OutDir': './output',
            '-LogDir': './log/',
            '-TmpDir': './tmp/',
            '-Serial': 60,
            '-Cmd': 0,
            '-Gpu': gpu,
        }
        kwargs.update(self.args)

        t = Timer()
        launcher = self._get_launcher()
        print("AreTomo3 argv:", launcher, kwargs)
        batch.call(launcher, kwargs)

        batch.info.update({
            'aretomo_input': len(items),
            'aretomo_elapsed': str(t.getElapsedTime())
        })

        batch['results'] = []
        batch['outputs'] = []
        total = 0

        result = {'rlnTomoName': tsName}
        try:
            # Aligned tilt series MRC is always produced, regardless of
            # whether tomogram reconstruction is enabled.
            outTiltSeriesMrc = batch.join('output', f'{tsName}.mrc')
            self.__expect(outTiltSeriesMrc)
            batch['outputs'].append(outTiltSeriesMrc)
            result['rlnTiltSeriesAligned'] = outTiltSeriesMrc
            # result['rlnTomoTiltSeriesStarFile'] = outTiltSeriesStar we dont have it
            # Alignment file (.aln) is always produced alongside it.
            alnFile = batch.join('output', f'{tsName}.aln')
            if os.path.exists(alnFile):
                batch['outputs'].append(alnFile)
                result['rlnTomoAlignmentFile'] = alnFile

            suffix = ''
            if self.reconstruct:
                suffix = '_Vol'
                outTomogramMrc = batch.join('output', f'{tsName}{suffix}.mrc')
                self.__expect(outTomogramMrc)
                batch['outputs'].append(outTomogramMrc)
                result['rlnTomogram'] = outTomogramMrc

                if self.auto_estimate_thickness:
                    thickMrc = batch.join('tmp', f'{tsName}_Thick.mrc')
                    if os.path.exists(thickMrc):
                        batch['outputs'].append(thickMrc)
                        result['aretomo3ThicknessMrc'] = thickMrc

                    thickCsv = batch.join('tmp', f'{tsName}_Thick_CC.csv')
                    if os.path.exists(thickCsv):
                        batch['outputs'].append(thickCsv)
                        result['aretomo3ThicknessCsv'] = thickCsv

            if self.ctf_estimation:
                ctfFileTxt = batch.join('output', f'{tsName}_CTF.txt')
                self.__expect(ctfFileTxt)
                batch['outputs'].append(ctfFileTxt)
                result['rlnTomoCtfFile'] = ctfFileTxt

                ctfFileMrc = batch.join('output', f'{tsName}_CTF.mrc')
                if os.path.exists(ctfFileMrc):
                    batch['outputs'].append(ctfFileMrc)
                    result['rlnTomoCtfMrc'] = ctfFileMrc
    
            if self.split_sum:
                for tag, key in (('_ODD', 'rlnTomoNameOdd'),
                                  ('_EVN', 'rlnTomoNameEvn')):
                    splitName = batch.join('output', f'{tsName}{tag}{suffix}.mrc')
                    self.__expect(splitName)
                    batch['outputs'].append(splitName)
                    result[key] = splitName

            metricsCsv = batch.join('output', 'TiltSeries_Metrics.csv')
            if os.path.exists(metricsCsv):
                batch['outputs'].append(metricsCsv)
                result['aretomo3MetricsCsv'] = metricsCsv

            timestampCsv = batch.join('output', 'TiltSeries_TimeStamp.csv')
            if os.path.exists(timestampCsv):
                batch['outputs'].append(timestampCsv)
                result['aretomo3TimeStampCsv'] = timestampCsv
            
            # EMHub-internal metadata star file, distinct from any files
            # AreTomo3 itself writes to 'output/'.
            tomoStar = batch.join('output', f'{tsName}.star')
            self.__write_tomo_star(mdoc, result, tomoStar)
            result['rlnTomoMetadata'] = tomoStar
            batch['outputs'].append(tomoStar)
            total += 1

        except Exception as e:
            result['error'] = str(e)
            print(Color.red(f"ERROR: {result['error']}"))

        batch['results'].append(result)

        batch.info.update({
            'aretomo_output': total
        })

    def __expect(self, fileName):
        if not os.path.exists(fileName):
            raise Exception(f"Missing expected output: {fileName}")

    def __write_tomo_star(self, mdoc, result, tomoStar):
        """ Write an EMHub-internal star file summarizing this tilt series'
        AreTomo3 outputs, using whatever keys process_batch collected into
        `result` (only the ones that actually exist for this run, since
        reconstruction/CTF/split-sum are all optional). """
        columns = ['rlnTomoName', 'rlnTomoTiltSeriesMdocFile',
                'rlnVoltage', 'rlnSphericalAberration',
                'rlnAmplitudeContrast', 'rlnTomoTiltSeriesPixelSize']
        values = [result.get('rlnTomoName', ''), mdoc,
                self.acq.voltage, self.acq.cs,
                self.acq.amplitude_contrast, self.acq.pixel_size]

        # Append only the optional fields that were actually populated.
        optionalFields = [
            ('rlnTiltSeriesAligned', result.get('rlnTiltSeriesAligned')),
            ('rlnTomoAlignmentFile', result.get('rlnTomoAlignmentFile')),
            ('rlnTomogram', result.get('rlnTomogram')),
            ('rlnTomoCtfFile', result.get('rlnTomoCtfFile')),
            ('rlnTomoNameOdd', result.get('rlnTomoNameOdd')),
            ('rlnTomoNameEvn', result.get('rlnTomoNameEvn')),
        ]
        for colName, value in optionalFields:
            if value is not None:
                columns.append(colName)
                values.append(value)

        tGeneral = Table(columns)
        tGeneral.addRowValues(*values)

        with StarFile(tomoStar, 'w') as sf:
            sf.writeTimeStamp()
            sf.writeTable('general', tGeneral, singleRow=True)

    def argsFromAcq(self, acq):
        """ Define arguments from a given acquisition """
        args = Args({
            '-PixSize': acq.pixel_size,
            '-kV': acq.voltage,
            '-Cs': acq.cs,
            '-AmpContrast': acq.amplitude_contrast
        })
        if gain := acq.get('gain', None):
            args['-Gain'] = gain
        if dose := acq.get('total_dose', None):
            args['-FmDose'] = dose

        return args