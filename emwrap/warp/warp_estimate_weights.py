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
from emtools.metadata import WarpPopulation

from .warp import WarpBasePopulationPipeline


class WarpEstimateWeights(WarpBasePopulationPipeline):
    """Warp wrapper to run EstimateWeights.

    Estimates per-item or per-frame weights for the population.
    """
    name = 'emw-warp-estimate_weights'

    def runBatch(self, batch, **kwargs):
        subargs = self._args.subset('estimate_weights', '--',
                                    filters=['remove_false', 'remove_empty'])
        extra = Args.fromString(self._args.get('extra_estimate_weights', ''))

        population_file = self._setup_population_input(subargs)

        args = Args({
            'EstimateWeights': '',
            '--population': population_file
        })
        args.update(subargs)
        args.update(extra)

        if '--source' not in args:
            wp = WarpPopulation(self.join(population_file))
            source_name = wp.Sources[0]['name']
            self.log(f"Loading first source from Population: {source_name}")
            args['--source'] = source_name

        self.batch_execute('estimate_weights', batch, args, call=True)
        self.updateBatchInfo(batch)


if __name__ == '__main__':
    WarpEstimateWeights.main()
