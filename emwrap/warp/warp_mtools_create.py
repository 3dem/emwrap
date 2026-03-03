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
        batch.mkdir(self.M)

        if new_population:
            pop_name = self._args.get('create_population.name', 'population')
            # MTools create_population --directory m --name <name>
            args = Args({
                'MTools': 'create_population',
                '--directory': self.M,
                '--name': pop_name
            })
            self.batch_execute('create_population', batch, args, call=True)
            pop_path = f"{self.M}/{pop_name}.population"
            warpFolder = self._args['warp_folder']
        else:
            raise Exception("Not implemented: to import an existing population.")
            input_population = self._args.get('input_population', '')
            warpFolder = None  # FIXME: Get warp folder from input population
            pop_name = None # FIXME: Get name from input population
            if not input_population:
                raise Exception("input_population is required when not creating a new population.")
            pop_path = input_population

        self.log(f"Importing import folder from previous WARP run: {warpFolder}")
        self._importInputs(warpFolder, keys=['fs', 'fss', 'ts', 'tss', 'tm'])

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
        self.batch_execute('create_source', batch, args, call=True)

        def _validate(key, value):
            if not value or not os.path.exists(value):
                raise Exception(f"Expected file '{key}' does not exist: {value}")
            return True

        # MTools create_species ${POPULATION} --name ... --diameter ... etc.
        args = Args({
            'MTools': 'create_species',
            pop_arg: pop_path,
        })
     
        subargs = self.get_subargs('create_species', '--')
        subargs = {k: v for k, v in subargs.items() if v is not None and str(v).strip() != ''}
        
        if _validate('mask', subargs.get('--mask', '')):
            subargs['--mask'] = self.link(subargs['--mask'])

        particles_relion = subargs.get('--particles_relion', '')
        if _validate('particles STAR', particles_relion):
            subargs['--particles_relion'] = self.link(particles_relion)
            for i in range(1, 3):
                half = particles_relion.replace('_data.star', f'_half{i}_class001_unfil.mrc')
                if _validate(f'half{i} map', half):
                    subargs[f'--half{i}'] = self.link(half)

        extra = Args.fromString(self._args.get('extra_create_species', ''))
        args.update(subargs)        
        args.update(extra)
        self.batch_execute('create_species', batch, args, call=True)

        self.updateBatchInfo(batch)

    def _output(self, batch):
        """ Register output population and species paths. """
        self.log("Registering output population and species.")
        new_population = self._args.get('new_population', True)
        pop_name = self._args.get('create_population.name', 'population')
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
                'info': f"Name: {pop_name}",
                'files': [[population_file, 'WarpPopulation']]
            }
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
