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


import json
import os

from emtools.utils import Color
from emtools.metadata import StarFile, Table, RelionStar

from emwrap.base import ProcessingPipeline
from emwrap.base.job_form import _parse_multi_pointer_param_value
from emwrap.base.subset_ts import OUTPUT_NODE_LABELS


class MergeSetsPipeline(ProcessingPipeline):
    """Merge multiple tomography STAR sets of the same type into one output set."""

    name = 'emw-merge-sets'

    def __init__(self, args, output):
        ProcessingPipeline.__init__(self, args, output)
        self.inputSets = self._parseInputSets(args.get('input_sets'))

    @staticmethod
    def _parseInputSets(rawValue):
        """Parse the input_sets job parameter into a list of STAR file paths."""
        try:
            return _parse_multi_pointer_param_value(rawValue)
        except (ValueError, json.JSONDecodeError):
            return []

    def _detectGlobalTableType(self, starPath, globalTable):
        """Detect TiltSeriesMovies/TiltSeries/TiltSeriesAligned/Tomograms from a global table."""
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

    def _classifyInput(self, inputStar):
        """Classify an input STAR as global-table or optimisation-set and detect its type."""
        if RelionStar.isTomoOptimisationSet(inputStar):
            optRow = RelionStar.readTomoOptimisationSet(inputStar)[0]
            ptsStar = optRow.rlnTomoParticlesFile
            if not ptsStar:
                raise Exception(
                    f"Missing rlnTomoParticlesFile in optimisation_set STAR file: "
                    f"{inputStar}"
                )
            tomoStar = optRow.rlnTomoTomogramsFile
            if not tomoStar:
                raise Exception(
                    f"Missing rlnTomoTomogramsFile in optimisation_set STAR file: "
                    f"{inputStar}"
                )
            ptsTable = RelionStar.readTomoParticles(ptsStar)
            if ptsTable.hasColumn('rlnTomoParticleId'):
                inputType = 'TomoParticles'
            else:
                inputType = 'TomoCoordinates'
            return {
                'kind': 'optimisation',
                'inputType': inputType,
                'optimisationStar': inputStar,
                'particlesStar': ptsStar,
                'tomogramsStar': tomoStar,
            }

        globalTable = StarFile.getTableFromFile('global', inputStar)
        if not globalTable:
            raise Exception(f"Could not read 'global' table from {inputStar}")

        return {
            'kind': 'global',
            'inputType': self._detectGlobalTableType(inputStar, globalTable),
            'globalStar': inputStar,
        }

    @staticmethod
    def _outputStarName(inputType):
        """Return the output STAR filename for a global-table input type."""
        if inputType == 'Tomograms':
            return 'tomograms.star'
        return 'tilt_series.star'

    @staticmethod
    def _requireSameColumns(referenceColumns, table, context):
        """Raise if table columns differ from the reference column list."""
        columns = table.getColumnNames()
        if columns == referenceColumns:
            return

        missing = [c for c in referenceColumns if c not in columns]
        extra = [c for c in columns if c not in referenceColumns]
        details = []
        if missing:
            details.append(f"missing columns: {missing}")
        if extra:
            details.append(f"extra columns: {extra}")
        raise Exception(
            f"Incompatible columns in {context} ({'; '.join(details)})."
        )

    def _validateGlobalTables(self, inputStars):
        """Ensure all global tables share the same column schema."""
        referenceColumns = None
        for inputStar in inputStars:
            table = StarFile.getTableFromFile('global', inputStar)
            if not table:
                raise Exception(f"Could not read 'global' table from {inputStar}")
            if referenceColumns is None:
                referenceColumns = table.getColumnNames()
            else:
                self._requireSameColumns(
                    referenceColumns, table, f"'global' table in {inputStar}")

    def _validateTomogramPerTomoTables(self, tomoStars):
        """Ensure all per-tomogram tables in tomograms.star files share the same schema."""
        referenceColumns = None
        for tomoStar in tomoStars:
            for tableName, table in StarFile.getTablesDict(tomoStar).items():
                if tableName == 'global':
                    continue
                if referenceColumns is None:
                    referenceColumns = table.getColumnNames()
                else:
                    self._requireSameColumns(
                        referenceColumns, table,
                        f"table '{tableName}' in {tomoStar}")

    def _validateParticlesStars(self, particlesStars):
        """Ensure particles (and optional optics) tables are compatible across inputs."""
        referenceParticleColumns = None
        referenceOpticsColumns = None
        hasOptics = None

        for particlesStar in particlesStars:
            tables = StarFile.getTablesDict(particlesStar)
            particlesTable = tables.get('particles')
            if not particlesTable:
                raise Exception(
                    f"Could not read 'particles' table from {particlesStar}"
                )

            if referenceParticleColumns is None:
                referenceParticleColumns = particlesTable.getColumnNames()
            else:
                self._requireSameColumns(
                    referenceParticleColumns, particlesTable,
                    f"'particles' table in {particlesStar}")

            opticsTable = tables.get('optics')
            if opticsTable:
                if hasOptics is False:
                    raise Exception(
                        f"Incompatible particles STAR files: {particlesStar} has an "
                        "'optics' table but other inputs do not."
                    )
                hasOptics = True
                if referenceOpticsColumns is None:
                    referenceOpticsColumns = opticsTable.getColumnNames()
                else:
                    self._requireSameColumns(
                        referenceOpticsColumns, opticsTable,
                        f"'optics' table in {particlesStar}")
            elif hasOptics:
                raise Exception(
                    f"Incompatible particles STAR files: {particlesStar} is missing "
                    "an 'optics' table present in other inputs."
                )
            else:
                hasOptics = False

    def _validateMergeInputs(self, classified, inputType, kind):
        """Validate that all inputs can be merged (same columns and structure)."""
        if kind == 'optimisation':
            tomoStars = [item['tomogramsStar'] for item in classified]
            particlesStars = [item['particlesStar'] for item in classified]
            self._validateGlobalTables(tomoStars)
            self._validateTomogramPerTomoTables(tomoStars)
            self._validateParticlesStars(particlesStars)
            return

        globalStars = [item['globalStar'] for item in classified]
        self._validateGlobalTables(globalStars)
        if inputType == 'Tomograms':
            self._validateTomogramPerTomoTables(globalStars)

    @staticmethod
    def _particleRowKey(row, table):
        """Return a deduplication key for a particles or coordinates row."""
        for label in ('rlnParticleName', 'rlnTomoParticleName'):
            if table.hasColumn(label):
                return getattr(row, label)
        coordParts = []
        for label in (
                'rlnCoordinateX', 'rlnCoordinateY', 'rlnCoordinateZ',
                'rlnCenteredCoordinateXAngst', 'rlnCenteredCoordinateYAngst',
                'rlnCenteredCoordinateZAngst'):
            if table.hasColumn(label):
                coordParts.append(getattr(row, label))
        if coordParts:
            return (row.rlnTomoName, tuple(coordParts))
        return row.rlnTomoName

    def _mergeGlobalTables(self, inputStars, inputType):
        """Merge tilt_series.star or tomograms.star global tables, deduplicating by rlnTomoName."""
        merged = None
        seenTomoNames = set()
        skipped = 0
        perTomoTables = {}

        for inputStar in inputStars:
            inputTable = StarFile.getTableFromFile('global', inputStar)
            if not inputTable:
                raise Exception(f"Could not read 'global' table from {inputStar}")

            if merged is None:
                merged = Table(inputTable.getColumnNames())

            tableNames = StarFile.getTablesDict(inputStar).keys()

            for row in inputTable:
                tomoName = row.rlnTomoName
                if tomoName in seenTomoNames:
                    skipped += 1
                    continue
                seenTomoNames.add(tomoName)
                merged.addRow(row)

                if inputType == 'Tomograms':
                    for tableName in tableNames:
                        if tableName == 'global':
                            continue
                        if tableName == tomoName or tableName.endswith(tomoName):
                            perTomoTables[tomoName] = (inputStar, tableName)

        if not len(merged):
            raise Exception("No tomograms left after merging the input sets.")

        outputStar = self.join(self._outputStarName(inputType))
        self.log(f"Writing {Color.green(len(merged))} tomograms "
                 f"({Color.warn(skipped)} duplicate(s) skipped) to "
                 f"{Color.cyan(outputStar)}")

        with StarFile(outputStar, 'w') as sfOut:
            sfOut.writeTable('global', merged, computeFormat='left', timeStamp=True)
            if inputType == 'Tomograms':
                for tomoName in seenTomoNames:
                    if tomoName not in perTomoTables:
                        continue
                    sourceStar, tableName = perTomoTables[tomoName]
                    table = StarFile.getTableFromFile(tableName, sourceStar)
                    sfOut.writeTable(tableName, table, computeFormat='left')

        outputNode = OUTPUT_NODE_LABELS[inputType]
        self.writeRelionOutputNodes(
            [[self.fixOutputPath(self._outputStarName(inputType)), outputNode]])
        return len(merged)

    def _mergeTomogramsForOptimisation(self, tomoStars):
        """Merge tomograms.star files linked from optimisation_set inputs."""
        merged = None
        seenTomoNames = set()
        skipped = 0
        perTomoTables = {}

        for tomoStar in tomoStars:
            inputTable = StarFile.getTableFromFile('global', tomoStar)
            if not inputTable:
                raise Exception(f"Could not read 'global' table from {tomoStar}")

            if merged is None:
                merged = Table(inputTable.getColumnNames())

            tableNames = StarFile.getTablesDict(tomoStar).keys()

            for row in inputTable:
                tomoName = row.rlnTomoName
                if tomoName in seenTomoNames:
                    skipped += 1
                    continue
                seenTomoNames.add(tomoName)
                merged.addRow(row)

                for tableName in tableNames:
                    if tableName == 'global':
                        continue
                    if tableName == tomoName or tableName.endswith(tomoName):
                        perTomoTables[tomoName] = (tomoStar, tableName)

        if not len(merged):
            raise Exception("No tomograms left after merging the input sets.")

        outputStar = self.join('tomograms.star')
        self.log(f"Writing {Color.green(len(merged))} tomograms "
                 f"({Color.warn(skipped)} duplicate(s) skipped) to "
                 f"{Color.cyan(outputStar)}")

        with StarFile(outputStar, 'w') as sfOut:
            sfOut.writeTable('global', merged, computeFormat='left', timeStamp=True)
            for tomoName in seenTomoNames:
                if tomoName not in perTomoTables:
                    continue
                sourceStar, tableName = perTomoTables[tomoName]
                table = StarFile.getTableFromFile(tableName, sourceStar)
                sfOut.writeTable(tableName, table, computeFormat='left')

        return outputStar, len(merged), skipped

    def _mergeParticlesTables(self, particlesStars):
        """Merge particles.star files, remapping optics groups and deduplicating rows."""
        mergedParticles = None
        mergedOptics = None
        seenParticleKeys = set()
        skipped = 0
        nextOpticsGroupId = 0
        opticsNameMap = {}
        opticsIdMap = {}

        for particlesStar in particlesStars:
            tables = StarFile.getTablesDict(particlesStar)
            particlesTable = tables.get('particles')
            if not particlesTable:
                raise Exception(
                    f"Could not read 'particles' table from {particlesStar}"
                )

            if mergedParticles is None:
                mergedParticles = Table(particlesTable.getColumnNames())

            opticsTable = tables.get('optics')
            if opticsTable:
                if mergedOptics is None:
                    mergedOptics = Table(opticsTable.getColumnNames())
                    for row in opticsTable:
                        rowValues = row._asdict()
                        ogId = int(rowValues['rlnOpticsGroup'])
                        ogName = rowValues['rlnOpticsGroupName']
                        nextOpticsGroupId = max(nextOpticsGroupId, ogId)
                        mergedOptics.addRowValues(**rowValues)
                        opticsNameMap[ogName] = ogId
                        opticsIdMap[ogId] = ogId
                else:
                    for row in opticsTable:
                        rowValues = row._asdict()
                        ogName = rowValues['rlnOpticsGroupName']
                        ogId = int(rowValues['rlnOpticsGroup'])
                        if ogName in opticsNameMap:
                            opticsIdMap[ogId] = opticsNameMap[ogName]
                            continue
                        nextOpticsGroupId += 1
                        newName = ogName
                        opticsNameMap[ogName] = nextOpticsGroupId
                        opticsIdMap[ogId] = nextOpticsGroupId
                        rowValues['rlnOpticsGroup'] = nextOpticsGroupId
                        rowValues['rlnOpticsGroupName'] = newName
                        mergedOptics.addRowValues(**rowValues)

            for row in particlesTable:
                key = self._particleRowKey(row, particlesTable)
                if key in seenParticleKeys:
                    skipped += 1
                    continue
                seenParticleKeys.add(key)
                rowValues = row._asdict()
                if mergedOptics and 'rlnOpticsGroup' in rowValues:
                    ogId = int(rowValues['rlnOpticsGroup'])
                    if ogId in opticsIdMap:
                        rowValues['rlnOpticsGroup'] = opticsIdMap[ogId]
                mergedParticles.addRowValues(**rowValues)

        if not len(mergedParticles):
            raise Exception("No particles left after merging the input sets.")

        outputStar = self.join('particles.star')
        self.log(f"Writing {Color.green(len(mergedParticles))} particles "
                 f"({Color.warn(skipped)} duplicate(s) skipped) to "
                 f"{Color.cyan(outputStar)}")

        with StarFile(outputStar, 'w') as sfOut:
            sfOut.writeTimeStamp()
            if mergedOptics:
                sfOut.writeTable('optics', mergedOptics, computeFormat='left')
            sfOut.writeTable('particles', mergedParticles, computeFormat='left')

        return outputStar, len(mergedParticles), skipped

    def _mergeOptimisationInputs(self, classifiedInputs, inputType):
        """Merge optimisation_set inputs into tomograms, particles, and optimisation_set STAR files."""
        tomoStars = [item['tomogramsStar'] for item in classifiedInputs]
        particlesStars = [item['particlesStar'] for item in classifiedInputs]

        self._mergeTomogramsForOptimisation(tomoStars)
        self._mergeParticlesTables(particlesStars)

        outOptimisationStar = self.join('optimisation_set.star')
        optValues = {
            'rlnTomoTomogramsFile': self.fixOutputPath('tomograms.star'),
            'rlnTomoParticlesFile': self.fixOutputPath('particles.star'),
        }
        with StarFile(outOptimisationStar, 'w') as sfOut:
            sfOut.writeTable('optimisation_set', Table.fromDict(optValues),
                             computeFormat='left', timeStamp=True)

        outputNode = OUTPUT_NODE_LABELS[inputType]
        self.writeRelionOutputNodes(
            [[self.fixOutputPath('optimisation_set.star'), outputNode]])
        return len(StarFile.getTableFromFile('particles', self.join('particles.star')))

    def prerun(self):
        """Validate inputs, merge sets of the same type, and register Relion output nodes."""
        if len(self.inputSets) < 2:
            raise Exception(
                "Parameter 'input_sets' must contain at least two input STAR files."
            )

        for inputStar in self.inputSets:
            if not os.path.exists(inputStar):
                raise Exception(f"Input STAR file not found: {inputStar}")

        self.log(f"Merging {Color.green(len(self.inputSets))} input set(s):")
        for inputStar in self.inputSets:
            self.log(f"  - {Color.cyan(inputStar)}")

        classified = [self._classifyInput(inputStar) for inputStar in self.inputSets]
        inputTypes = {item['inputType'] for item in classified}
        if len(inputTypes) != 1:
            raise Exception(
                "All input sets must have the same type. Found: "
                f"{', '.join(sorted(inputTypes))}."
            )

        inputType = classified[0]['inputType']
        kind = classified[0]['kind']
        self._validateMergeInputs(classified, inputType, kind)

        if kind == 'optimisation':
            count = self._mergeOptimisationInputs(classified, inputType)
        else:
            globalStars = [item['globalStar'] for item in classified]
            count = self._mergeGlobalTables(globalStars, inputType)

        self.inputs = {'input_sets': self.inputSets}
        self.outputs = {'type': inputType, 'count': count}
        self.writeInfo()
        self.log(f"Created merged {inputType} set with {Color.green(count)} item(s).")


if __name__ == '__main__':
    MergeSetsPipeline.main()
