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

from emtools.jobs import Args

from .warp import WarpBasePopulationPipeline


class WarpMcore(WarpBasePopulationPipeline):
    """Warp wrapper to run MCore refinements."""
    name = 'emw-warp-mcore'

    def runBatch(self, batch, **kwargs):
        subargs = self._get_subargs('mcore', 'extra_mcore')
        population_file = self._setup_population_input(subargs)
        args = Args({ 
            'MCore': '', 
            '--population': population_file 
        })
        args.update(subargs)
        self.batch_execute('mcore', batch, args, call=True)
        self.updateBatchInfo(batch)


if __name__ == '__main__':
    WarpMcore.main()
