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
import shutil

from emtools.utils import Color
from emtools.jobs import NumericList
from emtools.metadata import StarFile, Table, RelionStar

from emwrap.base import ProcessingPipeline


OUTPUT_NODE_LABELS = {
    'TiltSeriesMovies': 'TomogramGroupMetadata.star.emwrap.frameseries',
    'TiltSeries': 'TomogramGroupMetadata.star.emwrap.TiltSeries',
    'TiltSeriesAligned': 'TomogramGroupMetadata.star.emwrap.TiltSeriesAligned',
    'Tomograms': 'TomogramGroupMetadata.star.relion.tomo.Tomograms',
    'TomoParticles': 'TomogramGroupMetadata.star.relion.tomo.particles',
    'TomoCoordinates': 'TomogramGroupMetadata.star.emwrap.TomoCoordinates',
}


class SubsetTsPipeline(ProcessingPipeline):
    name = 'emw-subset-ts'

    def __init__(self, args, output):
        ProcessingPipeline.__init__(self, args, output)
        self.inputSet = args['input_set']
        self.subsetNames = set((args.get('subset_tomo_names') or '').split())
        self.excludedTiltsMap = self._parseExcludeTiltsParam(args)

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

    def _resolveAllTomoNames(self, inputStar):
        """ Return every rlnTomoName found in the relevant 'global' table
        of 'inputStar' (the tomograms/tilt-series table itself, or, for an
        optimisation_set, the tomograms table it points to). Used when no
        'subset_tomo_names' was provided but 'exclude_tilts' was, so the
        job keeps every tomogram and only applies the requested tilt
        exclusions. """
        if RelionStar.isTomoOptimisationSet(inputStar):
            optRow = RelionStar.readTomoOptimisationSet(inputStar)[0]
            tomoStar = optRow._asdict().get('rlnTomoTomogramsFile', '')
            if not tomoStar:
                raise Exception(
                    f"Missing rlnTomoTomogramsFile in optimisation_set STAR "
                    f"file: {inputStar}")
        else:
            tomoStar = inputStar

        table = StarFile.getTableFromFile('global', tomoStar)
        if not table:
            raise Exception(f"Could not read 'global' table from {tomoStar}")

        return {row.rlnTomoName for row in table}

    def _writeFilteredGlobalTable(self, inputStar, outputStar, subsetNames):
        inputTable = StarFile.getTableFromFile('global', inputStar)
        if not inputTable:
            raise Exception(f"Could not read 'global' table from {inputStar}")

        if self.excludedTiltsMap and not inputTable.hasColumn('rlnTomoTiltSeriesStarFile'):
            raise Exception(
                f"exclude_tilts was provided, but {inputStar} has no "
                "'rlnTomoTiltSeriesStarFile' column to rewrite.")

        filtered = Table(inputTable.getColumnNames())
        for row in inputTable:
            if row.rlnTomoName not in subsetNames:
                continue
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
        particlesTable = RelionStar.readTomoParticles(inputStar)
        filtered = Table(particlesTable.getColumnNames())
        for row in particlesTable:
            if row.rlnTomoName in subsetNames:
                filtered.addRow(row)

        if not len(filtered):
            raise Exception(
                f"No particles from {inputStar} matched the requested subset."
            )

        self.log(f"Writing {Color.green(len(filtered))} / "
                 f"{Color.bold(len(particlesTable))} particles to {Color.cyan(outputStar)}")
        with StarFile(outputStar, 'w') as sfOut:
            sfOut.writeTable('particles', filtered, computeFormat='left', timeStamp=True)
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
        inputTable = StarFile.getTableFromFile('global', inputStar)
        if not inputTable:
            raise Exception(f"Could not read 'global' table from {inputStar}")

        inputType = self._detectGlobalTableType(inputStar, inputTable)
        outputStar = self.join(self._outputStarName(inputType))
        filtered = self._writeFilteredGlobalTable(inputStar, outputStar, subsetNames)

        outputNode = OUTPUT_NODE_LABELS[inputType]
        self.writeRelionOutputNodes([[outputStar, outputNode]])
        return inputType, len(filtered)

    def _subsetOptimisationSet(self, inputStar, subsetNames):
        optRow = RelionStar.readTomoOptimisationSet(inputStar)[0]
        optValues = optRow._asdict()

        tomoStar = optValues.get('rlnTomoTomogramsFile', '')
        ptsStar = optValues.get('rlnTomoParticlesFile', '')
        if not tomoStar:
            raise Exception(
                f"Missing rlnTomoTomogramsFile in optimisation_set STAR file: {inputStar}"
            )
        if not ptsStar:
            raise Exception(
                f"Missing rlnTomoParticlesFile in optimisation_set STAR file: {inputStar}"
            )

        outTomoStar = self.join('tomograms.star')
        outParticlesStar = self.join('particles.star')
        outOptimisationStar = self.join('optimisation_set.star')

        self._writeFilteredGlobalTable(tomoStar, outTomoStar, subsetNames)
        ptsTable = self._writeFilteredParticlesTable(
            ptsStar, outParticlesStar, subsetNames)

        if ptsTable.hasColumn('rlnTomoParticleId'):
            inputType = 'TomoParticles'
        else:
            inputType = 'TomoCoordinates'

        optValues['rlnTomoTomogramsFile'] = self.fixOutputPath('tomograms.star')
        optValues['rlnTomoParticlesFile'] = self.fixOutputPath('particles.star')
        with StarFile(outOptimisationStar, 'w') as sfOut:
            sfOut.writeTable('optimisation_set', Table.fromDict(optValues),
                             computeFormat='left', timeStamp=True)

        outputNode = OUTPUT_NODE_LABELS[inputType]
        self.writeRelionOutputNodes([[outOptimisationStar, outputNode]])
        return inputType, len(ptsTable)

    def prerun(self):
        if not self.subsetNames and not self.excludedTiltsMap:
            raise Exception(
                "Missing or empty parameter 'subset_tomo_names'. "
                "Provide a space-separated list of rlnTomoName values (or, "
                "to keep every tomogram and only exclude some tilts, leave "
                "it empty and use 'exclude_tilts' instead)."
            )

        if not os.path.exists(self.inputSet):
            raise Exception(f"Input STAR file not found: {self.inputSet}")

        inputStar = self.inputSet

        if not self.subsetNames:
            # No explicit subset requested, but exclude_tilts was: keep
            # every tomogram found in the input set.
            self.subsetNames = self._resolveAllTomoNames(inputStar)
            self.log("No 'subset_tomo_names' provided; keeping all "
                     f"{Color.green(len(self.subsetNames))} tomogram(s) "
                     "found in the input set.")

        self.log(f"Input set: {Color.bold(self.inputSet)}")
        self.log(f"Subset tomogram names ({Color.green(len(self.subsetNames))}): "
                 f"{Color.cyan(' '.join(sorted(self.subsetNames)))}")

        if self.excludedTiltsMap:
            unknown = sorted(set(self.excludedTiltsMap) - self.subsetNames)
            if unknown:
                raise Exception(
                    "exclude_tilts references tomogram name(s) not found in "
                    f"the input set: {', '.join(unknown)}"
                )
            summary = ', '.join(
                f"{name}: {sorted(ids)}"
                for name, ids in sorted(self.excludedTiltsMap.items()))
            self.log(f"Excluding tilts for {Color.green(len(self.excludedTiltsMap))} "
                     f"tomogram(s): {Color.cyan(summary)}")

        if RelionStar.isTomoOptimisationSet(inputStar):
            inputType, count = self._subsetOptimisationSet(inputStar, self.subsetNames)
        else:
            inputType, count = self._subsetGlobalInput(inputStar, self.subsetNames)

        self.inputs = {'input_set': self.inputSet,
                       'subset_tomo_names': sorted(self.subsetNames),
                       'exclude_tilts': {name: sorted(ids) for name, ids in
                                        self.excludedTiltsMap.items()}}
        self.outputs = {'type': inputType, 'count': count}
        self.writeInfo()
        self.log(f"Created {inputType} subset with {Color.green(count)} item(s).")


if __name__ == '__main__':
    SubsetTsPipeline.main()
