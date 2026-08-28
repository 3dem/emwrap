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

from emtools.utils import Color, Timer, Path
from emtools.jobs import Args

from emwrap.base import ProcessingPipeline


# TODO: 
# - 'rlnTiltSeriesAligned' is not aligned is the normal tilt series unaligned

class AreTomo3:
    """ AreTomo3 wrapper to run in a batch folder. """

    _MULTIPLE_VALUE_FLAGS = ['McPatch', 'AtPatch', 'Group', 'TiltAxis', 'ReconRange', 'AtBin', 'Sart']

    def __init__(self, acq, **kwargs):
        self.acq = acq
        self.args = self.argsFromAcq(acq)
        form_args = kwargs.get('extra_args', kwargs)
        self.args.update(self._serialize_form_args(form_args))
        self.outputPrefix = "output/"
        self.launcher_aretomo3 = kwargs.get('launcher_aretomo3', None)

    @classmethod
    def _serialize_form_args(cls, formArgs):
        args = Args(formArgs)
        subargs = args.subset('aretomo3', new_prefix="-", 
                                 filters=['remove_empty', 'binary_boolean', 'multiple_values'], 
                                 multiple_values=cls._MULTIPLE_VALUE_FLAGS)
        
        subargs.update(Args.fromString(subargs.pop('-ExtraArgs', '')))
        
        return subargs

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

        return args
    
    def _get_launcher(self):
        return self.launcher_aretomo3 or ProcessingPipeline.get_launcher('ARETOMO3')
    
    def __expect(self, fileName):
        if not os.path.exists(fileName):
            raise Exception(f"Missing expected output: {fileName}")
    
    def process_batch(self, batch, cmd=0, input_prefix=None, input_suffix=None, 
                      input_skips=None, ts_name=None, expect_tilt_series=True,
                      expect_split_tilt_series=True, expect_ctf_output=True,
                      **kwargs):
        """Run AreTomo3 on assets staged in *batch*.

        Cmd 0 keeps the historical MDOC/movie convention.  Cmd 1 and Cmd 2
        use a pre-built MRC stack and its basename as the input prefix.
        """
        gpu = kwargs['gpu']

        batch.mkdir('output')
        batch.mkdir('tmp')
        # batch.mkdir('log')

        items = batch['items']
        mdoc = batch.get('tsMdoc', '')
        mdocBase = os.path.basename(mdoc) if mdoc else ''
        tsName = ts_name or (Path.removeExt(mdocBase) if mdocBase else batch['tsName'])
        input_prefix = input_prefix or f'./{Path.removeExt(mdocBase)}'
        input_suffix = input_suffix or '.mdoc'

        kwargs = {
            '-InPrefix': input_prefix,
            '-InSuffix': input_suffix,
            '-InSkips': input_skips, 
            '-OutDir': './output',
            '-LogDir': './log/',
            '-TmpDir': './tmp/',
            '-Serial': 60,
            '-Cmd': cmd,
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
            # Cmd 2 may only write volumes. Cmd 0/1 always write a tilt stack.
            outTiltSeriesMrc = batch.join('output', f'{tsName}.mrc')
            if expect_tilt_series:
                self.__expect(outTiltSeriesMrc)
                batch['outputs'].append(outTiltSeriesMrc)
                result['rlnTiltSeriesAligned'] = outTiltSeriesMrc

            # Mapping file Mic index vs Tilt Angle
            outTiltSeriesMapping = batch.join('output', f'{tsName}_TLT.txt')
            if os.path.exists(outTiltSeriesMapping):
                result['at3MappingFile'] = outTiltSeriesMapping

            # Alignment file (.aln) is always produced alongside it.
            alnFile = batch.join('output', f'{tsName}.aln')
            if os.path.exists(alnFile):
                result['at3TomoAlignmentFile'] = alnFile

            suffix = ''
            if self.reconstruct:
                suffix = '_Vol'
                outTomogramMrc = batch.join('output', f'{tsName}{suffix}.mrc')
                self.__expect(outTomogramMrc)
                batch['outputs'].append(outTomogramMrc)
                result['rlnTomoReconstructedTomogram'] = outTomogramMrc

                if self.auto_estimate_thickness:
                    thickMrc = batch.join('tmp', f'{tsName}_Thick.mrc')
                    if os.path.exists(thickMrc):
                        batch['outputs'].append(thickMrc)
                        result['at3ThicknessMrc'] = thickMrc

                    thickCsv = batch.join('tmp', f'{tsName}_Thick_CC.csv')
                    if os.path.exists(thickCsv):
                        batch['outputs'].append(thickCsv)
                        result['at3ThicknessCsv'] = thickCsv

            if self.ctf_estimation and expect_ctf_output:
                ctfFileTxt = batch.join('output', f'{tsName}_CTF.txt')
                self.__expect(ctfFileTxt)
                batch['outputs'].append(ctfFileTxt)
                result['at3TomoCtfFile'] = ctfFileTxt

                ctfFileMrc = batch.join('output', f'{tsName}_CTF.mrc')
                if os.path.exists(ctfFileMrc):
                    batch['outputs'].append(ctfFileMrc)
                    result['rlnCtfImage'] = ctfFileMrc
    
            if self.split_sum:
                # Tomogram
                for tag, key in (('_ODD', 'rlnTomoNameOdd'),
                                  ('_EVN', 'rlnTomoNameEvn')):
                    splitName = batch.join('output', f'{tsName}{tag}{suffix}.mrc')
                    self.__expect(splitName)
                    batch['outputs'].append(splitName)
                    result[key] = splitName
                if expect_split_tilt_series:
                    # Tilt series
                    suffix = ''
                    for tag, key in (('_ODD', 'rlnTiltSeriesAlignedOdd'),
                                     ('_EVN', 'rlnTiltSeriesAlignedEvn')):
                        splitName = batch.join('output', f'{tsName}{tag}{suffix}.mrc')
                        self.__expect(splitName)
                        batch['outputs'].append(splitName)
                        result[key] = splitName

            imodFolder = batch.join('output', f'{tsName}_Imod')
            if os.path.isdir(imodFolder):
                batch['outputs'].append(imodFolder)
                result['at3ImodFolder'] = imodFolder

            metricsCsv = batch.join('output', 'TiltSeries_Metrics.csv')
            if os.path.exists(metricsCsv):
                batch['outputs'].append(metricsCsv)
                result['at3MetricsCsv'] = metricsCsv

            timestampCsv = batch.join('output', 'TiltSeries_TimeStamp.csv')
            if os.path.exists(timestampCsv):
                batch['outputs'].append(timestampCsv)
                result['at3TimeStampCsv'] = timestampCsv
            
            if mdoc:
                result['rlnTomoMdocFile'] = mdoc
            total += 1

        except Exception as e:
            result['error'] = str(e)
            print(Color.red(f"ERROR: {result['error']}"))

        batch['results'].append(result)

        batch.info.update({
            'aretomo3_output': total
        })
