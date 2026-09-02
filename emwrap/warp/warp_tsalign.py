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

# Helper to parse flexible patches input used by both Aretomo2 and Aretomo3
def parse_patches(val):
    """Parse a user-provided patches string into a (x, y) tuple.

    Accepts: "4x4", "4 x 4", "4 4", "4,4", "4, 4", or single-number "4".
    Empty or zero-like values map to (1,1).
    Returns (int, int) or None if parsing fails.
    """
    import re
    if val is None:
        return None
    s = str(val).strip()
    if s == '' or s in ['0x0', '0', '0,0', '0 0']:
        return (1, 1)
    parts = re.split(r'[xX,\s]+', s)
    parts = [p for p in parts if p != '']
    try:
        if len(parts) == 1:
            v = int(parts[0])
            return (v, v)
        elif len(parts) >= 2:
            return (int(parts[0]), int(parts[1]))
    except Exception:
        return None
    return None


def format_patches_for_launcher(launcher, x, y):
    """Format patches for a given launcher name.

    If launcher name contains 'aretomo3' (case-insensitive) returns "X Y".
    Otherwise returns "XxY".
    """
    try:
        from os.path import basename
        launcher_name = basename(launcher).lower() if launcher else ''
    except Exception:
        launcher_name = str(launcher).lower() if launcher else ''

    if 'aretomo3' in launcher_name:
        return f"{x} {y}"
    return f"{x}x{y}"


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
        v = (self._args.get(key, '') or 0)        
        return float(v)

    def _alignmentPerdevice(self):
        return self._args.get('perdevice', None)

    def alignmentFiles(self, tsName):
        if self._method() == 2:
            tsDir = self.join(self.TS, 'tiltstack', tsName)
            return (
                os.path.join(tsDir, f'{tsName}.xf'),
                os.path.join(tsDir, f'{tsName}.tlt')
            )

        return super().alignmentFiles(tsName)

    def alignedTS(self, tsName):
        if self._method() == 1:
            return self.join(self.TS, 'tiltstack', tsName, f'{tsName}_Imod', f"{tsName}_st.mrc" )
        return super().alignedTS(tsName)

    def runAlignment(self, batch):
        method = self._method()
        if method == 0:
            self._runAlignmentAretomo2(batch)
        elif method == 1:
            self._runAlignmentAretomo3(batch)
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
        commargs = self._args.subset('ts_align', '--',
                                    filters=['remove_false', 'remove_empty'])
        subargs.update(commargs)
        
        if perdevice := self._alignmentPerdevice():
            subargs['--perdevice'] = perdevice

        # Parse flexible patches input and format for Aretomo2 (expects 'XxY')
        if patches_raw := subargs.pop('--patches', None):
            parsed = parse_patches(patches_raw)
            if parsed and parsed != (1, 1):
                x, y = parsed
                # Use formatter that adapts per launcher (aretomo3 => 'X Y', else 'XxY')
                subargs['--patches'] = format_patches_for_launcher(aretomo_launcher, x, y)

        args.update(subargs)
        self.batch_execute('ts_aretomo', batch, args)

    def _runAlignmentAretomo3(self, batch):
        aretomo3_launcher = self.get_launcher_arg('launcher_aretomo', 'ARETOMO3')

        args = Args({
            'WarpTools': 'ts_aretomo3',
            '--settings': self.TSS,
            '--exe': aretomo3_launcher
        })
        if self.gpuList:
            args['--device_list'] = self.gpuList

        subargs = self._args.subset('ts_aretomo', '--',
                                    filters=['remove_false', 'remove_empty'])
        commargs = self._args.subset('ts_align', '--',
                                    filters=['remove_false', 'remove_empty'])
        subargs.update(commargs)
        
        if perdevice := self._alignmentPerdevice():
            subargs['--perdevice'] = perdevice

        # Parse flexible patches input and format for Aretomo3 (expects 'X Y')
        if patches_raw := subargs.pop('--patches', None):
            parsed = parse_patches(patches_raw)
            if parsed and parsed != (1, 1):
                x, y = parsed
                subargs['--patches'] = format_patches_for_launcher(aretomo3_launcher, x, y)

        args.update(subargs)
        self.batch_execute('ts_aretomo3', batch, args)

    def _runAlignmentEtomoPatches(self, batch):
        args = Args({
            'WarpTools': 'ts_etomo_patches',
            '--settings': self.TSS
        })
        if self.gpuList:
            args['--device_list'] = self.gpuList

        subargs = self._args.subset('ts_etomo_patches', '--',
                                    filters=['remove_false', 'remove_empty'])
        commargs = self._args.subset('ts_align', '--',
                                    filters=['remove_false', 'remove_empty'])
        subargs.update(commargs)
        
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
