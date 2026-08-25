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
    SOURCES = 'warp_sources'
    SOURCE_IMPORT_KEYS = ['fs', 'fss', 'ts', 'tss', 'tm']

    def _get_sources(self):
        """Return validated source rows from the sources TableParam."""
        sources = self._args.get('sources', [])
        if isinstance(sources, str):
            from emwrap.base.job_form import _parse_table_param_value
            sources = _parse_table_param_value(sources)

        if not sources:
            raise Exception("At least one source is required.")

        result = []
        seen_names = set()
        for row in sources:
            if not isinstance(row, dict):
                continue
            name = str(row.get('name', '')).strip()
            warp_folder = str(row.get('warp_folder', '')).strip()
            if not name and not warp_folder:
                continue
            if not name:
                raise Exception("Source name is required for each source.")
            if ' ' in name:
                raise Exception(f"Source name must not contain spaces: '{name}'")
            if name in seen_names:
                raise Exception(f"Duplicate source name: '{name}'")
            if not warp_folder:
                raise Exception(f"Previous WARP run is required for source '{name}'.")
            seen_names.add(name)
            result.append({'name': name, 'warp_folder': warp_folder})

        if not result:
            raise Exception("At least one source is required.")
        return result

    def runBatch(self, batch, **kwargs):
        batch.mkdir(self.M)
        pop_name = self._args.get('create_population.name', 'population')
        args = Args({
            'MTools': 'create_population',
            '--directory': self.M,
            '--name': pop_name
        })
        self.batch_execute('create_population', batch, args, call=True)
        pop_path = f"{self.M}/{pop_name}.population"

        if not self.exists(pop_path):
            raise Exception(f"create_population: Error, population file was not generated: {pop_path}")

        sources = self._get_sources()
        self.mkdir(self.SOURCES)

        pop_arg = '--population'
        subargs = self.get_subargs('create_source', '--')
        subargs = {k: v for k, v in subargs.items()
                   if v is not None and str(v).strip() != ''}

        for source in sources:
            source_name = source['name']
            warp_folder = source['warp_folder']
            source_dir = self.join(self.SOURCES, source_name)
            self.mkdir(source_dir)
            self.log(f"Importing inputs for source '{source_name}' from previous WARP run: {warp_folder}")
            self._importInputs(warp_folder, keys=self.SOURCE_IMPORT_KEYS, dest=source_dir)

            tss = os.path.join(self.SOURCES, source_name, self.TSS)
            args = Args({
                'MTools': 'create_source',
                pop_arg: pop_path,
                '--name': source_name,
                '--processing_settings': tss
            })
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
        pop_name = self._args.get('create_population.name', 'population')
        population_file = batch.join(self.M, f"{pop_name}.population")

        #TODO: Review registration and info for population outputs
        if os.path.isfile(population_file):
            outputNodes = [[population_file, 'WarpPopulation']]
            self.writeRelionOutputNodes(outputNodes)

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
