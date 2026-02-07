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

from .warp import WarpBaseTsAlign


class WarpAreTomo(WarpBaseTsAlign):
    """ Warp wrapper to run warp_ts_aretomo.
    It will run:
        - ts_import -> mdocs
        - create_settings -> warp_tiltseries.settings
        - ts_aretomo -> ts alignment
    """
    name = 'emw-warp-aretomo'
    output_angpix = "ts_aretomo.angpix"

    def runAlignment(self, batch):
        aretomo_launcher = self.get_launcher_arg('launcher_aretomo', 'ARETOMO2')

        # Run ts_aretomo wrapper
        args = Args({
            'WarpTools': 'ts_aretomo',
            '--settings': self.TSS,
            '--exe': aretomo_launcher
        })
        if self.gpuList:
            args['--device_list'] = self.gpuList

        if ts_import_extra := self._args.get('extra_ts_import', None):
            args.update(Args.fromString(ts_import_extra))

        subargs = self._args.subset('ts_aretomo', '--', filters=['remove_false', 'remove_empty'])
        args.update(subargs)
        self.batch_execute('ts_aretomo', batch, args) 
                           #launcher=self.get_launcher_arg('launcher_warp', 'WARP'))


if __name__ == '__main__':
    WarpAreTomo.main()
