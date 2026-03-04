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


class WarpMtoolsResample(WarpBasePopulationPipeline):
    """Warp wrapper to run MTools resample_trajectories.

    Resamples temporal trajectories of a species. The number of samples
    is usually between 1 (small particles) and 3 (very large particles).
    See sample_scripts/MCore_alliters.txt Iter 7.

    MTools API: https://warpem.github.io/reference/mtools/api/mtools/
    (resample_trajectories)
    """
    name = 'emw-warp-mtools_resample'

    def runBatch(self, batch, **kwargs):
        subargs = self._get_subargs('resample_trajectories', 'extra_resample')
        population_file = self._setup_population_input(subargs)

        species = subargs.pop('--species', '')
        if not species:
            wp = WarpPopulation(self.join(population_file))
            species = wp.Species[0]['path']
            self.log(f"Loading first species from Population: {species}")

        if '/m/' in species:
            _, species = species.split('/m/', 1)

        args = Args({
            'MTools': 'resample_trajectories',
            '--population': population_file,
            '--species': os.path.join('m', species),
        })
        args.update(subargs)
        args.update(extra)
        self.batch_execute('resample_trajectories', batch, args, call=True)
        self.updateBatchInfo(batch)


if __name__ == '__main__':
    WarpMtoolsResample.main()
