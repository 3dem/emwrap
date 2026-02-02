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

    def runAlignment(self, batch):
        # Run ts_aretomo wrapper
        args = Args({
            'WarpTools': 'ts_aretomo',
            '--settings': self.TSS,
            '--exe': os.environ['ARETOMO2']
        })
        if self.gpuList:
            args['--device_list'] = self.gpuList

        subargs = self.get_subargs('ts_aretomo', '--')
        args.update(subargs)
        self.batch_execute('ts_aretomo', batch, args)


if __name__ == '__main__':
    WarpAreTomo.main()
