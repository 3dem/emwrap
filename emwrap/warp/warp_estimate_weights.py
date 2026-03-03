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


class WarpEstimateWeights(WarpBasePipeline):
    """ Warp wrapper to run EstimateWeights.

    Estimates per-item or per-frame weights for the population.
    """
    name = 'emw-warp-estimate_weights'

    def _split_population(self, population):
        """ Split the population path into input folder and relative population. """
        return population.split('/m/')

    def runBatch(self, batch, **kwargs):
        subargs = self._args.subset('estimate_weights', '--',
                                    filters=['remove_false', 'remove_empty'])
        extra = Args.fromString(self._args.get('extra_estimate_weights', ''))

        inputWarp, self.population = self._split_population(subargs.pop('--population'))
        populationFile = os.path.join('m', self.population)

        self.log(f"Input Warp folder: {inputWarp}, population: {self.population}")
        self._importInputs(inputWarp, keys=['fs', 'fss', 'ts', 'tss', 'tm', 'm'])

        args = Args({
            'EstimateWeights': '',
            '--population': populationFile
        })
        args.update(subargs)
        args.update(extra)

        # Get the first source if not explicity passed
        if '--source' not in args:
            wp = WarpPopulation(self.join(populationFile))
            sourceName = wp.Sources[0]['name']
            self.log(f"Loading first source from Population: {sourceName}")            
            args['--source'] = sourceName

        self.batch_execute('estimate_weights', batch, args, call=True)
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
    WarpEstimateWeights.main()
