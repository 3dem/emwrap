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
from emtools.metadata import StarFile, Table, RelionStar, WarpPopulation

from .warp import WarpBasePipeline


class WarpMtoolsCreate(WarpBasePipeline):
    """ Script to run MTools create_population, create_source, and create_species.
    Uses form emw-warp-mtools_create.json and follows the command structure
    from sample_scripts/MCore_alliters.txt.
    """
    name = 'emw-warp-mtools_create'
    SOURCES = 'sources'
    SOURCE_IMPORT_KEYS = ['fs', 'fss', 'ts', 'tss', 'tm']
    ACQ_STAR_NAMES = ('tomograms.star', 'tilt_series.star', 'movies.star')
    TOMO_METADATA_STAR = 'tomograms.star'
    TOMO_COORD_METADATA_COLUMNS = (
        'rlnTomoSizeX', 'rlnTomoSizeY', 'rlnTomoSizeZ',
        'rlnTomoTiltSeriesPixelSize', 'wrpTomostar',
    )

    def _warpParticlesStarPath(self):
        """Project-relative path to the Warp particles STAR under m/."""
        return f'{self.M}/{self.WARP_PARTICLES_STAR}'

    @staticmethod
    def _tomogramsStarFromParticlePath(particle_path):
        """Infer a project-relative tomograms.star from a particle image path."""
        parts = str(particle_path).replace('\\', '/').split('/')
        if 'Particles' in parts:
            idx = parts.index('Particles')
            if idx >= 1:
                return '/'.join(parts[:idx]) + '/tomograms.star'
        return None

    @staticmethod
    def _warpTomoName(tomo_row):
        """Return the tomostar file name expected by Warp (warp_tomostar basename)."""
        tomostar = getattr(tomo_row, 'wrpTomostar', None)
        if not tomostar:
            raise Exception(
                "tomograms.star row is missing column wrpTomostar required "
                "to map rlnTomoName for Warp."
            )
        return os.path.basename(str(tomostar))

    @staticmethod
    def _rowHasTomoCoordMetadata(row):
        """Return True when a tomogram row has metadata needed for conversion."""
        return all(
            getattr(row, col, None) not in (None, '')
            for col in WarpMtoolsCreate.TOMO_COORD_METADATA_COLUMNS
        )

    def _mergeTomoLookup(self, lookup, tomograms_star):
        """Add tomogram rows from tomograms.star keyed by rlnTomoName."""
        if not self.projectExists(tomograms_star):
            return
        tomo_table = StarFile.getTableFromFile(
            'global', self.project.join(tomograms_star))
        if not tomo_table:
            raise Exception(
                f"Could not read 'global' table from {tomograms_star}."
            )
        if not tomo_table.hasColumn('rlnTomoName'):
            raise Exception(
                f"{tomograms_star} does not contain column rlnTomoName."
            )
        for row in tomo_table:
            if not self._rowHasTomoCoordMetadata(row):
                continue
            lookup[row.rlnTomoName] = row

    def _collectTomogramsStars(self, particles_table):
        """Collect tomograms.star files used to match particle rlnTomoName."""
        tomograms_stars = set()

        for source in self._get_sources():
            star = os.path.join(source['warp_folder'], self.TOMO_METADATA_STAR)
            if self.projectExists(star):
                tomograms_stars.add(self.toProjectPath(star))

        image_col = 'rlnImageName' if particles_table.hasColumn('rlnImageName') else None
        if image_col:
            for row in particles_table:
                image_path = getattr(row, image_col, '')
                if not image_path:
                    continue
                star = self._tomogramsStarFromParticlePath(
                    self.toProjectPath(image_path))
                if star and self.projectExists(star):
                    tomograms_stars.add(star)

        if not tomograms_stars:
            raise Exception(
                "Could not find tomograms.star metadata to convert particle "
                "coordinates. Check WARP source folders or particle image paths."
            )
        return tomograms_stars

    def _collectTomoLookup(self, particles_table):
        """Build tomogram metadata lookup keyed by rlnTomoName."""
        lookup = {}
        for star in sorted(self._collectTomogramsStars(particles_table)):
            self._mergeTomoLookup(lookup, star)

        missing = sorted({
            row.rlnTomoName
            for row in particles_table
            if row.rlnTomoName not in lookup
        })
        if missing:
            raise Exception(
                "Missing tomograms.star metadata for particle "
                f"rlnTomoName value(s): {missing[:5]}"
                f"{'...' if len(missing) > 5 else ''}. "
                "Ensure tomograms.star contains matching rlnTomoName entries."
            )
        return lookup

    def _particlesOutputColumns(self, particles_table):
        """Return output column names with pixel coords instead of centered Angstrom."""
        col_names = list(particles_table.getColumnNames())
        if particles_table.hasAllColumns(
                RelionStar.TOMO_PARTICLES_PIXEL_COORD_COLUMNS):
            return col_names

        centered = RelionStar.TOMO_PARTICLES_CENTERED_COORD_COLUMNS
        if not particles_table.hasAllColumns(centered):
            raise Exception(
                "Particles STAR file must contain either rlnCoordinateX/Y/Z or "
                "rlnCenteredCoordinateXAngst/YAngst/ZAngst columns."
            )

        insert_at = col_names.index(centered[0])
        new_cols = [c for c in col_names if c not in centered]
        for offset, pixel_col in enumerate(
                RelionStar.TOMO_PARTICLES_PIXEL_COORD_COLUMNS):
            new_cols.insert(insert_at + offset, pixel_col)
        return new_cols

    def _convertParticlesTableToWarp(self, particles_table, tomo_lookup, scale_factor):
        """Return a Warp-compatible particles table (voxels and tomostar names)."""
        out_cols = self._particlesOutputColumns(particles_table)
        out_table = Table(out_cols)
        src_cols = particles_table.getColumnNames()

        for row in particles_table:
            tomo_row = tomo_lookup[row.rlnTomoName]
            x, y, z = RelionStar.particleCoordsToPixel(row, tomo_row)
            row_values = {
                c: getattr(row, c)
                for c in src_cols
                if c not in RelionStar.TOMO_PARTICLES_CENTERED_COORD_COLUMNS
            }
            row_values.update({
                'rlnCoordinateX': x * scale_factor,
                'rlnCoordinateY': y * scale_factor,
                'rlnCoordinateZ': z * scale_factor,
                'rlnTomoName': self._warpTomoName(tomo_row),
            })
            out_table.addRowValues(**row_values)
        return out_table

    def _prepareParticlesRelionForWarp(self, particles_relion):
        """Prepare a Warp-compatible particles STAR for create_species."""
        if prepared := getattr(self, '_prepared_particles_relion', None):
            return prepared

        abs_path = self.project.join(particles_relion)
        particles_table = RelionStar.readTomoParticles(abs_path)
        

        self.log(
            "Preparing particles STAR for Warp: converting coordinates and "
            "mapping rlnTomoName to warp_tomostar file names."
        )
        tomo_lookup = self._collectTomoLookup(particles_table)

        # There might be a scale missmatch between the tomogram binning and the particles binning
        # because in WarpExportParticles, one can select a different output pixel size
        optics_table = StarFile.getTableFromFile('optics', abs_path)
        first_tomo_row = next(iter(tomo_lookup.values()))
        tomogram_binning = RelionStar.getTomoBinning(first_tomo_row)
        particles_binning = optics_table[0].rlnTomoSubtomogramBinning
        scale_factor = tomogram_binning / particles_binning


        converted_table = self._convertParticlesTableToWarp(
            particles_table, tomo_lookup, scale_factor)

        out_star = self.join(self.M, self.WARP_PARTICLES_STAR)
        all_tables = StarFile.getTablesDict(abs_path)
        with StarFile(out_star, 'w') as sf_out:
            sf_out.writeTimeStamp()
            for name, table in all_tables.items():
                if name == RelionStar.TOMO_PARTICLES_TABLE:
                    sf_out.writeTable(
                        name, converted_table, computeFormat='left')
                else:
                    sf_out.writeTable(name, table, computeFormat='left')

        self._prepared_particles_relion = self.toProjectPath(out_star)
        return self._prepared_particles_relion

    def _parse_sources_param(self):
        """Return source rows from the sources TableParam."""
        sources = self._args.get('sources', [])
        if isinstance(sources, str):
            from emwrap.base.job_form import _parse_table_param_value
            sources = _parse_table_param_value(sources)
        return sources if isinstance(sources, list) else []

    def _get_acquisition_input_star(self):
        if inputStar := super()._get_acquisition_input_star():
            return inputStar

        for source in self._parse_sources_param():
            if not isinstance(source, dict):
                continue
            warp_folder = str(source.get('warp_folder', '')).strip()
            if not warp_folder:
                continue
            fm = FolderManager(warp_folder)
            for name in self.ACQ_STAR_NAMES:
                star_path = fm.join(name)
                if os.path.exists(star_path):
                    self.log(
                        f"Using acquisition metadata from source "
                        f"'{source.get('name', warp_folder)}': {star_path}"
                    )
                    return star_path
        return None

    def _get_sources(self):
        """Return validated source rows from the sources TableParam."""
        sources = self._parse_sources_param()

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
        self._prepared_particles_relion = None
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
        self.mkdir(self.M, self.SOURCES)

        pop_arg = '--population'
        subargs = self.get_subargs('create_source', '--')
        subargs = {k: v for k, v in subargs.items()
                   if v is not None and str(v).strip() != ''}

        for source in sources:
            source_name = source['name']
            warp_folder = source['warp_folder']
            source_dir = self.join(self.M, self.SOURCES, source_name)
            self.mkdir(self.M, self.SOURCES, source_name)
            self.log(
                f"Importing source '{source_name}' into {self.M}/{self.SOURCES} "
                f"from previous WARP run: {warp_folder}"
            )
            self._importInputs(warp_folder, keys=self.SOURCE_IMPORT_KEYS,
                               dest=source_dir)

            pop_from_source = os.path.relpath(
                self.toProjectPath(self.join(pop_path)),
                self.toProjectPath(source_dir),
            )
            args = Args({
                'MTools': 'create_source',
                pop_arg: pop_from_source,
                '--name': source_name,
                '--processing_settings': self.TSS,
            })
            args.update(subargs)
            self.batch_execute('create_source', batch, args, call=True,
                               work_dir=source_dir)
            WarpPopulation(self.join(pop_path)).getSource(source_name)

        def _validate(key, value):
            path = self.project.join(value) if value else value
            if not value or not os.path.exists(path):
                raise Exception(f"Expected file '{key}' does not exist: {value}")
            return True

        original_particles_relion = str(
            self._args.get('create_species.particles_relion', '')).strip()
        prepared_particles_relion = None
        if original_particles_relion and _validate(
                'particles STAR', original_particles_relion):
            prepared_particles_relion = self._prepareParticlesRelionForWarp(
                original_particles_relion)

        # MTools create_species ${POPULATION} --name ... --diameter ... etc.
        args = Args({
            'MTools': 'create_species',
            pop_arg: pop_path,
        })

        subargs = self.get_subargs('create_species', '--')
        subargs = {k: v for k, v in subargs.items() if v is not None and str(v).strip() != ''}
        species_name = str(self._args.get('create_species.name', '')).strip()
        if not species_name:
            raise Exception("create_species.name is required.")

        if _validate('mask', subargs.get('--mask', '')):
            subargs['--mask'] = self.link(subargs['--mask'])

        if prepared_particles_relion:
            subargs['--particles_relion'] = self._warpParticlesStarPath()
            for i in range(1, 3):
                half = original_particles_relion.replace(
                    '_data.star', f'_half{i}_class001_unfil.mrc')
                if _validate(f'half{i} map', half):
                    subargs[f'--half{i}'] = self.link(half)

        extra = Args.fromString(self._args.get('extra_create_species', ''))
        args.update(subargs)
        args.update(extra)
        self.batch_execute('create_species', batch, args, call=True)
        population = WarpPopulation(self.join(pop_path))
        population.getSpecies(species_name)

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
