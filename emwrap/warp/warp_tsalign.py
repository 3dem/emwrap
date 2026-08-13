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

from emtools.jobs import Args
from emtools.metadata import StarFile

from .warp import WarpBaseTsAlign


class WarpTsAlign(WarpBaseTsAlign):
    """ Warp wrapper for tilt-series alignment.

    Supports AreTomo2 (ts_aretomo) and Etomo patches (ts_etomo_patches).
    It will run:
        - ts_import -> mdocs
        - create_settings -> warp_tiltseries.settings
        - alignment step selected by the method parameter
    """
    name = 'emw-warp-tsalign'

    def _method(self):
        return int(self._args.get('method', 0))

    def alignmentPs(self):
        if self._method() == 2:
            key = 'ts_etomo_patches.angpix'
        else:
            key = 'ts_aretomo.angpix'
        v = (self._args.get(key, '') or
             self._args.get('wat.ts_aretomo.angpix', '') or 0)
        return float(v)

    def _alignmentPerdevice(self):
        return self._args.get('perdevice', None)

    def runAlignment(self, batch):
        method = self._method()
        if method == 0:
            self._runAlignmentAretomo2(batch)
        elif method == 2:
            self._runAlignmentEtomoPatches(batch)
        else:
            raise Exception(f"Unknown alignment method: {method}")

    def _runAlignmentAretomo2(self, batch):
        aretomo_launcher = self.get_launcher_arg('launcher_aretomo', 'ARETOMO2')

        args = Args({
            'WarpTools': 'ts_aretomo',
            '--settings': self.TSS,
            '--exe': aretomo_launcher
        })
        if self.gpuList:
            args['--device_list'] = self.gpuList

        subargs = self._args.subset('ts_aretomo', '--',
                                    filters=['remove_false', 'remove_empty'])
        if perdevice := self._alignmentPerdevice():
            subargs['--perdevice'] = perdevice
        if patches := subargs.pop('--patches', None):
            patches = patches.lower().strip()
            if patches not in ['0x0', '1x1']:
                args['--patches'] = patches
        args.update(subargs)
        self.batch_execute('ts_aretomo', batch, args)

    def _runAlignmentEtomoPatches(self, batch):
        args = Args({
            'WarpTools': 'ts_etomo_patches',
            '--settings': self.TSS
        })
        if self.gpuList:
            args['--device_list'] = self.gpuList

        subargs = self._args.subset('ts_etomo_patches', '--',
                                    filters=['remove_false', 'remove_empty'])
        if perdevice := self._alignmentPerdevice():
            subargs['--perdevice'] = perdevice
        args.update(subargs)
        self.batch_execute('ts_etomo_patches', batch, args)

        imod_launcher = self.get_launcher_arg('launcher_imod', 'IMOD')
        tsAllTable = StarFile.getTableFromFile('global', self.inputTs)

        def _tsFile(tsName, suffix):
            return os.path.join(self.TS, 'tiltstack', tsName, f"{tsName}{suffix}")

        for tsRow in tsAllTable:
            tsName = tsRow.rlnTomoName
            args = Args({
                'newstack': '',
                '-InputFile': _tsFile(tsName, '.st'),
                '-OutputFile': _tsFile(tsName, '_aligned.mrc'),
                '-TransformFile': _tsFile(tsName, '.xf')
            })
            batch.call(imod_launcher, args, logfile=self.join('run.out'))


if __name__ == '__main__':
    WarpTsAlign.main()
