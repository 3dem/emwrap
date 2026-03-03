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

from emtools.jobs import Batch, Args
from emtools.metadata import WarpPopulation

from .warp import WarpBasePipeline


class WarpMtoolsResample(WarpBasePipeline):
    """ Warp wrapper to run MTools resample_trajectories.

    Resamples temporal trajectories of a species. The number of samples
    is usually between 1 (small particles) and 3 (very large particles).
    See sample_scripts/MCore_alliters.txt Iter 7.

    MTools API: https://warpem.github.io/reference/mtools/api/mtools/
    (resample_trajectories)
    """
    name = 'emw-warp-mtools_resample'

    def _split_population(self, population):
        """ Split the population path into input folder and relative population. """
        return population.split('/m/')

    def runBatch(self, batch, **kwargs):
        subargs = self._args.subset('resample_trajectories', '--',
                                    filters=['remove_false', 'remove_empty'])
        extra = Args.fromString(self._args.get('extra_resample', ''))

        pop_arg = subargs.pop('--population', None) or subargs.pop('-p', None)
        if not pop_arg:
            raise Exception("--population is required for resample_trajectories.")

        inputWarp, self.population = self._split_population(pop_arg)
        populationFile = os.path.join('m', self.population)

        self.log(f"Input Warp folder: {inputWarp}, population: {self.population}")
        self._importInputs(inputWarp, keys=['fs', 'fss', 'ts', 'tss', 'tm', 'm'])

        species = subargs.pop('--species', '')
        if not species:
            wp = WarpPopulation(self.join(populationFile))
            species = wp.Species[0]['path']
            self.log(f"Loading first species from Population: {species}")            

        # Species path: use m/ prefix relative to output (after import)
        if '/m/' in species:
            _, species = species.split('/m/', 1)

        args = Args({
            'MTools': 'resample_trajectories',
            '--population': populationFile,
            '--species': os.path.join('m', species),
        })
        args.update(subargs)
        args.update(extra)
        self.batch_execute('resample_trajectories', batch, args, call=True)
        self.updateBatchInfo(batch)

    def _output(self, batch):
        """ Register output population. """
        self.log("Registering output population.")
        population_file = self.join(self.M, self.population)
        population_name = self.population.replace('.population', '')
        if os.path.exists(population_file):
            self.outputs['Population'] = {
                'label': 'Population',
                'type': 'WarpPopulation',
                'info': f"Name: {population_name}",
                'files': [[population_file, 'WarpPopulation']]
            }
        else:
            self.log(f"Population file not found: {population_file}")

        self.updateBatchInfo(batch)

    def prerun(self):
        batch = Batch(id=self.name, path=self.path)
        self.runBatch(batch)
        self._output(batch)


if __name__ == '__main__':
    WarpMtoolsResample.main()
