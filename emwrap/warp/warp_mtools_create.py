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

from emtools.utils import FolderManager
from emtools.jobs import Batch, Args

from .warp import WarpBasePipeline



class WarpMtoolsCreate(WarpBasePipeline):
    """ Script to run MTools create_population, create_source, and create_species.
    Uses form emw-warp-mtools_create.json and follows the command structure
    from sample_scripts/MCore_alliters.txt.
    """
    name = 'emw-warp-mtools_create'

    def runBatch(self, batch, **kwargs):
        new_population = self._args.get('new_population', True)
        pop_name = self._args.get('create_population.name', 'population')
        input_population = self._args.get('input_population', '')

        if kwargs.get('importInputs', True):
            input_run = kwargs.get('input_run')
            if input_run:
                self._importInputs(input_run)

        batch.mkdir(self.M)

        if new_population:
            # MTools create_population --directory m --name <name>
            args = Args({
                'MTools': 'create_population',
                '--directory': self.M,
                '--name': pop_name
            })
            subargs = self.get_subargs('create_population', '--')
            args.update(subargs)
            self.batch_execute('create_population', batch, args)
            pop_path = f"{self.M}/{pop_name}.population"
        else:
            if not input_population:
                raise Exception("input_population is required when not creating a new population.")
            pop_path = input_population

        pop_arg = '--population'

        # MTools create_source ${POPULATION} --name <name> --processing_settings warp_tiltseries.settings
        args = Args({
            'MTools': 'create_source',
            pop_arg: pop_path,
            '--name': self._args.get('create_source.name') or pop_name,
            '--processing_settings': self.TSS
        })
        subargs = self.get_subargs('create_source', '--')
        # Filter out empty so --nframes 0 is not passed if that means "use max"
        subargs = {k: v for k, v in subargs.items() if v is not None and str(v).strip() != ''}
        args.update(subargs)
        self.batch_execute('create_source', batch, args)

        # MTools create_species ${POPULATION} --name ... --diameter ... etc.
        args = Args({
            'MTools': 'create_species',
            pop_arg: pop_path,
        })
        subargs = self.get_subargs('create_species', '--')
        subargs = {k: v for k, v in subargs.items() if v is not None and str(v).strip() != ''}
        args.update(subargs)
        extra = (self._args.get('extra_create_species') or '').strip()
        if extra:
            # Append extra arguments (e.g. --half1, --half2, --temporal_samples)
            current_key = None
            for part in extra.split():
                if part.startswith('--'):
                    if current_key is not None and args.get(current_key) == '':
                        pass  # previous was a flag with no value
                    current_key = part
                    args[current_key] = ''
                elif current_key is not None:
                    args[current_key] = part
                    current_key = None
        self.batch_execute('create_species', batch, args)

        self.updateBatchInfo(batch)

    def _output(self, batch):
        """ Register output population and species paths. """
        self.log("Registering output population and species.")
        new_population = self._args.get('new_population', True)
        pop_name = self._args.get('create_population.name', 'population')
        species_name = self._args.get('create_species.name', '')
        population_file = (
            batch.join(self.M, f"{pop_name}.population")
            if new_population
            else self._args.get('input_population', '')
        )
        species_dir = batch.join(self.M, 'species')
        self.outputs = {}
        if population_file and (os.path.isfile(population_file) or not new_population):
            self.outputs['Population'] = {
                'label': 'Population',
                'type': 'WarpPopulation',
                'info': f"Population: {pop_name}",
                'files': [[population_file, 'WarpPopulation.population']]
            }
        if species_name and os.path.isdir(species_dir):
            # Species path is typically m/species/<species_id>/<name>.species
            for sub in os.listdir(species_dir):
                subpath = os.path.join(species_dir, sub)
                if os.path.isdir(subpath):
                    species_file = os.path.join(subpath, f"{species_name}.species")
                    if os.path.isfile(species_file):
                        self.outputs['Species'] = {
                            'label': 'Species',
                            'type': 'WarpSpecies',
                            'info': f"Species: {species_name}",
                            'files': [[batch.join(species_file), 'WarpSpecies.species']]
                        }
                        break
        self.updateBatchInfo(batch)

    def prerun(self):
        """ Run MTools create steps and register outputs. """
        batch = Batch(id=self.name, path=self.path)
        input_run = self._args.get('input_run')
        if self._args.get('__j') != 'only_output':
            self.log("Running MTools create_population, create_source, create_species.")
            self.runBatch(batch, input_run=input_run)
        else:
            self.log("Received special argument 'only_output', only registering outputs.")
        self._output(batch)


if __name__ == '__main__':
    WarpMtoolsCreate.main()
