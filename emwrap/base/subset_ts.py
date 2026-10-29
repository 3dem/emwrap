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
import random
import shutil

from emtools.utils import Color
from emtools.jobs import NumericList
from emtools.metadata import StarFile, Table, RelionStar

from emwrap.base import ProcessingPipeline
from emwrap.warp.warp_subset import WarpSubsetTs


OUTPUT_NODE_LABELS = {
    'TiltSeriesMovies': 'TomogramGroupMetadata.star.emwrap.frameseries',
    'TiltSeries': 'TomogramGroupMetadata.star.emwrap.TiltSeries',
    'TiltSeriesAligned': 'TomogramGroupMetadata.star.emwrap.TiltSeriesAligned',
    'Tomograms': 'TomogramGroupMetadata.star.relion.tomo.Tomograms',
    'TomoParticles': 'TomogramGroupMetadata.star.relion.tomo.particles',
    'TomoCoordinates': 'TomogramGroupMetadata.star.emwrap.TomoCoordinates',
}

# Values for the 'mode' EnumParam: how the subset's tomogram names are picked.
MODE_EXPLICIT_NAMES = 0  # 'subset_tomo_names': space-separated rlnTomoName list.
MODE_MATCH_SET = 1       # 'subset_match_set': keep names also found in a 2nd set.
MODE_RANDOM = 2          # 'subset_random_count' (+ optional 'subset_random_seed').
MODE_RANDOM_PARTICLES = 3  # 'subset_random_particles' (+ optional 'subset_random_seed'),
                           # only for TomoCoordinates / TomoParticles inputs.

MODE_LABELS = ("0 (explicit names list), 1 (match a second set), "
               "2 (random selection of N tomograms) or 3 (random selection "
               "of M particles)")


class SubsetTsPipeline(ProcessingPipeline):
    name = 'emw-subset-ts'

    def __init__(self, args, output):
        ProcessingPipeline.__init__(self, args, output)
        self.inputSet = args['input_set']

        self.mode = self._parseMode(args.get('mode'))

        self.subsetNames = set()
        self.randomCount = None
        self.randomSeed = None
        self.matchSet = ''
        # Mode 3: number of particles to pick and, once picked, the indexes
        # (rows of the input particles table) of the selected ones.
        self.randomParticles = None
        self.particleIndexes = None

        if self.mode == MODE_EXPLICIT_NAMES:
            names = (args.get('subset_tomo_names') or '').replace(',', ' ')
            self.subsetNames = set(names.split())
        elif self.mode == MODE_MATCH_SET:
            self.matchSet = (args.get('subset_match_set') or '').strip()
        elif self.mode == MODE_RANDOM:
            self.randomCount = self._parseRandomCount(
                args.get('subset_random_count'), 'subset_random_count',
                'tomograms', 2)
            self.randomSeed = self._parseRandomSeed(args.get('subset_random_seed'))
        else:  # MODE_RANDOM_PARTICLES
            self.randomParticles = self._parseRandomCount(
                args.get('subset_random_particles'), 'subset_random_particles',
                'particles', 3)
            self.randomSeed = self._parseRandomSeed(args.get('subset_random_seed'))

        # 'exclude_tilts' only makes sense when the resulting tomogram names
        # are known ahead of the run: Mode 0 (explicit list) and Mode 1
        # (matched against a second set). In Modes 2 and 3 (random selection)
        # the names are not known until the job actually runs, so it's
        # ignored even if a stale value is still present in 'args'.
        self.excludedTiltsMap = (
            {} if self.mode in (MODE_RANDOM, MODE_RANDOM_PARTICLES)
            else self._parseExcludeTiltsParam(args))

    @staticmethod
    def _parseMode(value):
        """ Parse the 'mode' EnumParam value into an int: 0 (explicit
        names), 1 (match a second set), 2 (random selection of tomograms)
        or 3 (random selection of particles). """
        if value is None or str(value).strip() == '':
            return MODE_EXPLICIT_NAMES
        try:
            mode = int(value)
        except (TypeError, ValueError):
            raise Exception(
                f"Invalid 'mode' value: {value!r}. Expected {MODE_LABELS}.")
        if mode not in (MODE_EXPLICIT_NAMES, MODE_MATCH_SET, MODE_RANDOM,
                        MODE_RANDOM_PARTICLES):
            raise Exception(
                f"Invalid 'mode' value: {mode}. Expected {MODE_LABELS}.")
        return mode

    @staticmethod
    def _parseRandomCount(value, paramName, itemsLabel, mode):
        """ Parse 'paramName' ('subset_random_count' in Mode 2 or
        'subset_random_particles' in Mode 3) into a positive int. """
        if value is None or str(value).strip() == '':
            raise Exception(
                f"Missing or empty parameter '{paramName}'. Provide "
                f"the number of {itemsLabel} to randomly select (Mode {mode})."
            )
        try:
            count = int(value)
        except (TypeError, ValueError):
            raise Exception(
                f"Invalid '{paramName}' value: {value!r}. Expected "
                "a positive integer.")
        if count <= 0:
            raise Exception(
                f"'{paramName}' must be a positive integer, got {count}.")
        return count

    @staticmethod
    def _parseRandomSeed(value):
        """ Parse the optional 'subset_random_seed' (Modes 2 and 3) into an int,
        or None when left empty (non-reproducible random selection). """
        if value is None or str(value).strip() == '':
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            raise Exception(
                f"Invalid 'subset_random_seed' value: {value!r}. Expected "
                "an integer, or leave it empty.")

    @staticmethod
    def _parseTiltIds(text):
        """ Parse a string of tilt movie indices (rlnTomoTiltMovieIndex),
        separated by spaces and/or commas and optionally using ranges
        (e.g. '3, 5-7, 9' -> {3, 5, 6, 7, 9}), into a set of ints. """
        try:
            return set(NumericList.fromString(text))
        except ValueError as e:
            raise Exception(str(e))

    def _parseExcludeTiltsParam(self, args):
        """ Parse the 'exclude_tilts' TableParam (rows with 'tomoName' and
        'excludedTilts') into {tomoName: {excluded tilt ids}}. """
        raw = args.get('exclude_tilts', [])
        if isinstance(raw, str):
            from emwrap.base.job_form import _parse_table_param_value
            raw = _parse_table_param_value(raw)
        rows = raw if isinstance(raw, list) else []

        excludedTiltsMap = {}
        for row in rows:
            if not isinstance(row, dict):
                continue
            tomoName = str(row.get('tomoName', '')).strip()
            tiltsText = str(row.get('excludedTilts', '') or '').strip()
            if not tomoName or not tiltsText:
                continue
            try:
                ids = self._parseTiltIds(tiltsText)
            except Exception as e:
                raise Exception(
                    f"Invalid 'excludedTilts' for tomogram '{tomoName}': {e}")
            if ids:
                excludedTiltsMap.setdefault(tomoName, set()).update(ids)

        return excludedTiltsMap

    def _getTomogramsStar(self, inputStar):
        """Return the STAR file containing the global tomogram table."""
        if not RelionStar.isTomoOptimisationSet(inputStar):
            return inputStar

        optRow = RelionStar.readTomoOptimisationSet(inputStar)[0]
        tomoStar = optRow._asdict().get('rlnTomoTomogramsFile', '')
        if not tomoStar:
            raise Exception(
                f"Missing rlnTomoTomogramsFile in optimisation_set STAR "
                f"file: {inputStar}"
            )
        return tomoStar

    def _getParticlesStar(self, inputStar):
        """Return the particles STAR file of an optimisation_set STAR file."""
        optRow = RelionStar.readTomoOptimisationSet(inputStar)[0]
        ptsStar = optRow._asdict().get('rlnTomoParticlesFile', '')
        if not ptsStar:
            raise Exception(
                f"Missing rlnTomoParticlesFile in optimisation_set STAR file: {inputStar}"
            )
        return self._absPath(ptsStar)

    def _readGlobalTable(self, starPath):
        """Read and validate a RELION tomography global table."""
        table = StarFile.getTableFromFile('global', starPath)
        if not table:
            raise Exception(f"Could not read 'global' table from {starPath}")
        return table

    def _resolveAllTomoNames(self, inputStar):
        table = self._readGlobalTable(self._getTomogramsStar(inputStar))
        return {row.rlnTomoName for row in table}

    def _filterTiltSeriesStar(self, tomoName, tsStarPath, excludedIds):
        """ Read the per-tilt-series STAR file for 'tomoName', remove the
        rows whose 'rlnTomoTiltMovieIndex' is in 'excludedIds', write the
        result under this job's own output folder, and return its path
        (relative to the working dir) to use as the new
        'rlnTomoTiltSeriesStarFile' value. """
        tsTable = StarFile.getTableFromFile(tomoName, tsStarPath)
        if not tsTable:
            raise Exception(
                f"Could not read tilt-series table '{tomoName}' from {tsStarPath}")
        if not tsTable.hasColumn('rlnTomoTiltMovieIndex'):
            raise Exception(
                f"Tilt-series STAR file {tsStarPath} for '{tomoName}' has no "
                "'rlnTomoTiltMovieIndex' column, cannot exclude tilts from it.")

        filteredTs = Table(tsTable.getColumnNames())
        presentIds = set()
        for row in tsTable:
            tiltId = int(row.rlnTomoTiltMovieIndex)
            presentIds.add(tiltId)
            if tiltId not in excludedIds:
                filteredTs.addRow(row)

        if not len(filteredTs):
            raise Exception(
                f"Excluding tilts {sorted(excludedIds)} from '{tomoName}' would "
                f"leave no tilts in its tilt-series STAR file ({tsStarPath})."
            )

        if missing := (excludedIds - presentIds):
            self.log(Color.warn(
                f"WARNING: excludedTilts for '{tomoName}' includes tilt "
                f"index(es) not found in {tsStarPath}: {sorted(missing)}"))

        self.mkdir('tilt_series')
        relTsStar = os.path.join('tilt_series', f'{tomoName}.star')
        with StarFile(self.join(relTsStar), 'w') as sfOut:
            sfOut.writeTable(tomoName, filteredTs, computeFormat='left', timeStamp=True)

        self.log(f"Tomogram {Color.bold(tomoName)}: excluded "
                 f"{Color.red(len(excludedIds))} tilt(s), kept "
                 f"{Color.green(len(filteredTs))} / {Color.bold(len(tsTable))} "
                 f"in {Color.cyan(relTsStar)}")

        return self.fixOutputPath(relTsStar)

    def _copyTiltSeriesStar(self, tomoName, tsStarPath):
        """ Copy the (untouched) per-tilt-series STAR file for 'tomoName'
        as-is into this job's own 'tilt_series' output folder, and return
        its path (relative to the working dir) to use as the new
        'rlnTomoTiltSeriesStarFile' value. Used so that, once exclude_tilts
        is active, every tomogram in the output -- not only the ones with
        excluded tilts -- points to a tilt-series STAR file living in this
        same job folder. """
        self.mkdir('tilt_series')
        relTsStar = os.path.join('tilt_series', f'{tomoName}.star')
        shutil.copy2(tsStarPath, self.join(relTsStar))
        return self.fixOutputPath(relTsStar)

    def _copyMdoc(self, mdocFile):
        """ Copy 'mdocFile' into this job's own 'mdocs' folder, so it only
        contains the mdocs of the selected tilt series, and return the new
        path (relative to the working dir) to use as 'rlnTomoMdocFile'. """
        if not mdocFile or not os.path.exists(mdocFile):
            self.log(Color.warn(
                f"WARNING: mdoc file not found, keeping original path: {mdocFile}"))
            return mdocFile
        self.mkdir('mdocs')
        relMdoc = os.path.join('mdocs', os.path.basename(mdocFile))
        shutil.copy2(mdocFile, self.join(relMdoc))
        return self.fixOutputPath(relMdoc)

    def _resolveTomoNamesFromStar(self, starPath, paramLabel):
        """ Return the set of rlnTomoName values found in 'starPath'.
        Accepts the same kind of STAR files as 'input_set' (tilt_series.star,
        tomograms.star, optimisation_set.star), as well as any other STAR
        file that has at least one table with an 'rlnTomoName' column (e.g.
        a plain particles.star). Used by Mode 1 ('subset_match_set') to
        build the set of tomogram names to match against. """
        if not os.path.exists(starPath):
            raise Exception(f"{paramLabel} STAR file not found: {starPath}")

        lookupStar = (self._getTomogramsStar(starPath)
                      if RelionStar.isTomoOptimisationSet(starPath)
                      else starPath)

        tables = StarFile.getTablesDict(lookupStar)

        # Prefer the well-known 'global' (tilt_series/tomograms) or
        # 'particles' table names, falling back to any other table that
        # happens to carry an 'rlnTomoName' column.
        for preferred in ('global', 'particles'):
            table = tables.get(preferred)
            if table is not None and table.hasColumn('rlnTomoName'):
                return {row.rlnTomoName for row in table}

        for table in tables.values():
            if table.hasColumn('rlnTomoName'):
                return {row.rlnTomoName for row in table}

        raise Exception(
            f"Could not find a table with an 'rlnTomoName' column in "
            f"{paramLabel} STAR file: {lookupStar}")

    def _writeFilteredGlobalTable(self, inputStar, outputStar, subsetNames):
        inputTable = self._readGlobalTable(inputStar)

        if self.excludedTiltsMap and not inputTable.hasColumn('rlnTomoTiltSeriesStarFile'):
            raise Exception(
                f"exclude_tilts was provided, but {inputStar} has no "
                "'rlnTomoTiltSeriesStarFile' column to rewrite.")

        hasMdocs = inputTable.hasColumn('rlnTomoMdocFile')
        filtered = Table(inputTable.getColumnNames())
        for row in inputTable:
            if row.rlnTomoName not in subsetNames:
                continue
            if hasMdocs:
                row = row._replace(
                    rlnTomoMdocFile=self._copyMdoc(row.rlnTomoMdocFile))
            if excludedIds := self.excludedTiltsMap.get(row.rlnTomoName):
                newTsStar = self._filterTiltSeriesStar(
                    row.rlnTomoName, row.rlnTomoTiltSeriesStarFile, excludedIds)
                row = row._replace(rlnTomoTiltSeriesStarFile=newTsStar)
            elif self.excludedTiltsMap:
                # exclude_tilts is active for this job: even tomograms with
                # no excluded tilts get their tilt-series STAR file copied
                # here, so every row in the output points to a metadata
                # file living in this same job folder.
                newTsStar = self._copyTiltSeriesStar(
                    row.rlnTomoName, row.rlnTomoTiltSeriesStarFile)
                row = row._replace(rlnTomoTiltSeriesStarFile=newTsStar)
            filtered.addRow(row)

        if not len(filtered):
            raise Exception(
                f"No tomograms from {inputStar} matched the requested subset."
            )

        self.log(f"Writing {Color.green(len(filtered))} / "
                 f"{Color.bold(len(inputTable))} tomograms to {Color.cyan(outputStar)}")
        with StarFile(outputStar, 'w') as sfOut:
            sfOut.writeTable('global', filtered, computeFormat='left', timeStamp=True)
        return filtered

    def _writeFilteredParticlesTable(self, inputStar, outputStar, subsetNames):
        """ Copy all tables of 'inputStar' (general, optics...), keeping only
        the particles of the subset tomograms (and, in Mode 3, only the
        randomly selected ones, self.particleIndexes). """
        if not RelionStar.isTomoParticles(inputStar):
            raise Exception(
                f"{inputStar} is not a compliant tomography particles STAR file.")
        tables = StarFile.getTablesDict(inputStar)
        particlesTable = tables[RelionStar.TOMO_PARTICLES_TABLE]
        filtered = Table(particlesTable.getColumnNames())
        for i, row in enumerate(particlesTable):
            if self.particleIndexes is not None and i not in self.particleIndexes:
                continue
            if row.rlnTomoName in subsetNames:
                filtered.addRow(row)

        if not len(filtered):
            raise Exception(
                f"No particles from {inputStar} matched the requested subset."
            )

        self.log(f"Writing {Color.green(len(filtered))} / "
                 f"{Color.bold(len(particlesTable))} particles to {Color.cyan(outputStar)}")
        with StarFile(outputStar, 'w') as sfOut:
            sfOut.writeTimeStamp()
            for name, table in tables.items():
                if name == RelionStar.TOMO_PARTICLES_TABLE:
                    sfOut.writeTable(name, filtered, computeFormat='left')
                else:
                    sfOut.writeTable(name, table, singleRow=(name == 'general'))
        return filtered

    def _detectGlobalTableType(self, starPath, globalTable):
        if starPath.endswith('tomograms.star'):
            return 'Tomograms'

        if '_series' in os.path.basename(starPath) or starPath.endswith('tilt_series.star'):
            first = globalTable[0]
            tsStar = first.rlnTomoTiltSeriesStarFile
            tsTable = StarFile.getTableFromFile(first.rlnTomoName, tsStar)
            if tsTable.hasAllColumns(RelionStar.TOMO_ALIGNMENT_COLUMNS):
                return 'TiltSeriesAligned'
            if tsTable.hasColumn('rlnMicrographName'):
                return 'TiltSeries'
            if tsTable.hasAllColumns(RelionStar.TOMO_FRAME_SERIES_COLUMNS):
                return 'TiltSeriesMovies'

        raise Exception(
            f"Could not determine input type for {starPath}. "
            "Expected tilt_series.star or tomograms.star."
        )

    def _outputStarName(self, inputType):
        if inputType == 'Tomograms':
            return 'tomograms.star'
        return 'tilt_series.star'

    def _subsetGlobalInput(self, inputStar, subsetNames):
        inputTable = self._readGlobalTable(inputStar)

        inputType = self._detectGlobalTableType(inputStar, inputTable)
        outputStar = self.join(self._outputStarName(inputType))
        filtered = self._writeFilteredGlobalTable(inputStar, outputStar, subsetNames)

        outputNode = OUTPUT_NODE_LABELS[inputType]
        self.writeRelionOutputNodes([[outputStar, outputNode]])
        return inputType, len(filtered)

    def _absPath(self, path):
        return path if os.path.isabs(path) else os.path.join(self.workingDir, path)

    def _subsetOptimisationSet(self, inputStar, subsetNames):
        """ Subset a TomoCoordinates / TomoParticles optimisation set: its
        tomograms and their particles, both registered as outputs. """
        optRow = RelionStar.readTomoOptimisationSet(inputStar)[0]
        optValues = optRow._asdict()

        tomoStar = self._absPath(self._getTomogramsStar(inputStar))
        ptsStar = self._getParticlesStar(inputStar)

        outTomoStar = self.join('tomograms.star')
        outParticlesStar = self.join('particles.star')
        outOptimisationStar = self.join('optimisation_set.star')

        self._writeFilteredGlobalTable(tomoStar, outTomoStar, subsetNames)
        ptsTable = self._writeFilteredParticlesTable(
            ptsStar, outParticlesStar, subsetNames)

        # Same rule as the project outputs (ProjectData), plus Warp's ids
        if (ptsTable.hasColumn('rlnTomoParticleName')
                or ptsTable.hasColumn('rlnTomoParticleId')):
            inputType = 'TomoParticles'
        else:
            inputType = 'TomoCoordinates'

        optValues['rlnTomoTomogramsFile'] = self.fixOutputPath('tomograms.star')
        optValues['rlnTomoParticlesFile'] = self.fixOutputPath('particles.star')
        with StarFile(outOptimisationStar, 'w') as sfOut:
            sfOut.writeTable('optimisation_set', Table.fromDict(optValues),
                             computeFormat='left', timeStamp=True)

        self.writeRelionOutputNodes([
            [outOptimisationStar, OUTPUT_NODE_LABELS[inputType]],
            [outTomoStar, OUTPUT_NODE_LABELS['Tomograms']],
        ])
        return inputType, len(ptsTable)

    def _resolveSubsetNames(self, inputStar):
        """ Compute self.subsetNames according to the selected 'mode':
        Mode 0 (explicit list), Mode 1 (match names present in a second
        set), Mode 2 (random selection of N tomograms) or Mode 3 (random
        selection of M particles, and the tomograms they belong to). """
        if self.mode == MODE_EXPLICIT_NAMES:
            if not self.subsetNames and not self.excludedTiltsMap:
                raise Exception(
                    "Missing or empty parameter 'subset_tomo_names'. "
                    "Provide a space-separated list of rlnTomoName values (or, "
                    "to keep every tomogram and only exclude some tilts, leave "
                    "it empty and use 'exclude_tilts' instead)."
                )

            allNames = self._resolveAllTomoNames(inputStar)

            if not self.subsetNames:
                # No explicit subset requested, but exclude_tilts was: keep
                # every tomogram found in the input set.
                self.subsetNames = allNames
                self.log("No 'subset_tomo_names' provided; keeping all "
                         f"{Color.green(len(self.subsetNames))} tomogram(s) "
                         "found in the input set.")
            else:
                missing = sorted(self.subsetNames - allNames)

                if missing:
                    raise Exception(
                        "The following requested tomogram name(s) were not found "
                        f"in the input set: {', '.join(missing)}. "
                        "Available rlnTomoName values are: "
                        f"{', '.join(sorted(allNames))}"
                    )

        elif self.mode == MODE_MATCH_SET:
            if not self.matchSet:
                raise Exception(
                    "Missing parameter 'subset_match_set'. Provide a second "
                    "STAR file with an 'rlnTomoName' column to match against "
                    "(Mode 1)."
                )
            matchNames = self._resolveTomoNamesFromStar(
                self.matchSet, "'subset_match_set'")
            allNames = self._resolveAllTomoNames(inputStar)
            self.subsetNames = allNames & matchNames
            if not self.subsetNames:
                raise Exception(
                    "No tomograms in common between the input set and "
                    f"'subset_match_set' ({self.matchSet})."
                )
            self.log(f"Matched {Color.green(len(self.subsetNames))} / "
                     f"{Color.bold(len(allNames))} tomogram(s) present in "
                     f"the second set ({Color.cyan(self.matchSet)}, which "
                     f"has {Color.bold(len(matchNames))} rlnTomoName "
                     "value(s)).")

        elif self.mode == MODE_RANDOM:
            allNames = self._resolveAllTomoNames(inputStar)
            if self.randomCount > len(allNames):
                raise Exception(
                    f"'subset_random_count' ({self.randomCount}) is greater "
                    "than the number of tomograms available in the input "
                    f"set ({len(allNames)})."
                )
            rng = random.Random(self.randomSeed)
            self.subsetNames = set(rng.sample(sorted(allNames), self.randomCount))
            seedMsg = f", seed={self.randomSeed}" if self.randomSeed is not None else ""
            self.log(f"Randomly selected {Color.green(len(self.subsetNames))} / "
                     f"{Color.bold(len(allNames))} tomogram(s){seedMsg}.")

        else:  # MODE_RANDOM_PARTICLES
            self._resolveRandomParticles(inputStar)

    def _resolveRandomParticles(self, inputStar):
        """ Mode 3: randomly pick self.randomParticles particles of a
        TomoCoordinates / TomoParticles input (self.particleIndexes) and set
        self.subsetNames to the tomograms they belong to. """
        if not RelionStar.isTomoOptimisationSet(inputStar):
            msg = ("'Random selection of M particles' mode is only valid for "
                   "TomoCoordinates or TomoParticles inputs (optimisation_set "
                   f"STAR file), got: {inputStar}")
            self.log(Color.red(f"ERROR: {msg}"))
            raise Exception(msg)

        ptsStar = self._getParticlesStar(inputStar)
        if not RelionStar.isTomoParticles(ptsStar):
            raise Exception(
                f"{ptsStar} is not a compliant tomography particles STAR file.")
        particlesTable = StarFile.getTableFromFile(
            RelionStar.TOMO_PARTICLES_TABLE, ptsStar)
        allTomoNames = self._resolveAllTomoNames(inputStar)
        # Only the particles of tomograms present in the set can be written.
        candidates = [i for i, row in enumerate(particlesTable)
                      if row.rlnTomoName in allTomoNames]

        if self.randomParticles > len(candidates):
            raise Exception(
                f"'subset_random_particles' ({self.randomParticles}) is greater "
                "than the number of particles available in the input "
                f"set ({len(candidates)})."
            )
        rng = random.Random(self.randomSeed)
        self.particleIndexes = set(rng.sample(candidates, self.randomParticles))
        self.subsetNames = {particlesTable[i].rlnTomoName
                            for i in self.particleIndexes}
        seedMsg = f", seed={self.randomSeed}" if self.randomSeed is not None else ""
        self.log(f"Randomly selected {Color.green(self.randomParticles)} / "
                 f"{Color.bold(len(candidates))} particle(s){seedMsg}, from "
                 f"{Color.green(len(self.subsetNames))} / "
                 f"{Color.bold(len(allTomoNames))} tomogram(s).")

    # ------------------------------------------------------------------
    # The deselection: what each subset job decides its own way.
    # ------------------------------------------------------------------
    def _prepareDeselection(self):
        """ Set self.subsetNames (the tomograms to keep) and
        self.excludedTiltsMap ({tomoName: {movie indices}}, the tilts to
        leave out of them). Here from the form: the names by 'mode' and
        the 'exclude_tilts' table. Subclasses (TiltCurationPipeline)
        decide them their own way; the output is written the same way
        (_writeSubset). """
        self._resolveSubsetNames(self.inputSet)

        if self.excludedTiltsMap:
            summary = ', '.join(
                f"{name}: {sorted(ids)}"
                for name, ids in sorted(self.excludedTiltsMap.items())
            )
            self.log(
                f"Excluding tilts for "
                f"{Color.green(len(self.excludedTiltsMap))} "
                f"tomogram(s): {Color.cyan(summary)}"
            )

    # ------------------------------------------------------------------
    # The output: the same for every subset job.
    # ------------------------------------------------------------------
    def _writeSubset(self):
        """ Write the subset of self.inputSet given by self.subsetNames and
        self.excludedTiltsMap: the filtered STAR files (and RELION output
        node) and the same selection in the Warp state when the input job
        has one. """
        if not self.subsetNames:
            raise Exception("No tomograms left in the subset.")
        if unknown := sorted(set(self.excludedTiltsMap) - self.subsetNames):
            raise Exception(
                "exclude_tilts references tomogram name(s) not found in "
                f"the output subset: {', '.join(unknown)}"
            )

        self.log(f"Subset tomogram names ({Color.green(len(self.subsetNames))}): "
                 f"{Color.cyan(' '.join(sorted(self.subsetNames)))}")

        if RelionStar.isTomoOptimisationSet(self.inputSet):
            inputType, count = self._subsetOptimisationSet(
                self.inputSet, self.subsetNames)
        else:
            inputType, count = self._subsetGlobalInput(
                self.inputSet, self.subsetNames)

        if WarpSubsetTs.hasState(self.inputSet):
            warpSubset = WarpSubsetTs(self._args, self.outputDir)
            warpSubset.apply(self.subsetNames, self.excludedTiltsMap)

        self.writeInfo()

        self.log(
            f"Created {inputType} subset with "
            f"{Color.green(count)} item(s)."
        )

    def prerun(self):
        if not os.path.exists(self.inputSet):
            raise Exception(f"Input STAR file not found: {self.inputSet}")
        self.log(f"Input set: {Color.bold(self.inputSet)}")

        self._prepareDeselection()
        self._writeSubset()


if __name__ == '__main__':
    SubsetTsPipeline.main()
