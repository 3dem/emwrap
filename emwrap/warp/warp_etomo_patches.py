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
from emtools.metadata import StarFile, Table
from emwrap.base import ProcessingPipeline

from .warp import WarpBaseTsAlign


class WarpEtomoPatches(WarpBaseTsAlign):
    """ Warp wrapper to run warp_ts_aretomo.
    It will run:
        - ts_import -> mdocs
        - create_settings -> warp_tiltseries.settings
        - ts_aretomo -> ts alignment
    """
    name = 'emw-warp-etomo_patches'
    output_angpix = "ts_etomo_patches.angpix"

    def runAlignment(self, batch):
        # Run ts_aretomo wrapper
        args = Args({
            'WarpTools': 'ts_etomo_patches',
            '--settings': self.TSS
        })
        if self.gpuList:
            args['--device_list'] = self.gpuList

        subargs = Args(self._args).subset('ts_etomo_patches', '--', 
                                          filters=['remove_false', 'remove_empty'])
        args.update(subargs)
        self.batch_execute('ts_etomo_patches', batch, args)

        # Generate aligned TS using IMOD's newstack
        imod_launcher = ProcessingPipeline.get_launcher('IMOD')

        tsAllTable = StarFile.getTableFromFile('global', self.inputTs)

        def _tsFile(tsName, suffix):
            # Paths relative to the job directory, since will be executed from the batch there
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
    WarpEtomoPatches.main()
