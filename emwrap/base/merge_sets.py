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

import numpy as np

from collections import OrderedDict

from emtools.utils import Color
from emtools.metadata import StarFile, Table, RelionStar

from emwrap.base import ProcessingPipeline
from emwrap.base.wizards import to_bool, to_float
from emwrap.base.job_form import _parse_multi_pointer_param_value
from emwrap.base.subset_ts import OUTPUT_NODE_LABELS


# Parts of the particles of merged sets (exclude_parts), with an overlap
# distance: 'overlap', the ones in more than one set, and 'set<N>', the ones
# only in the N-th set (1-based)
PART_OVERLAP = 'overlap'

# Particle columns written as 0 for a set without them (common_columns)
ORIGIN_COLUMNS = ('rlnOriginXAngst', 'rlnOriginYAngst', 'rlnOriginZAngst')


class MergeSetsPipeline(ProcessingPipeline):
    """Merge multiple tomography STAR sets of the same type into one output set.

    Particle / coordinate sets (optimisation sets) can be merged without
    their overlapping particles ('overlap_distance', Angstrom): a particle
    within the distance of one of an earlier set (same tomogram, positions
    with the shifts) is not added, the earlier one is.  The particles are
    then in parts, per tomogram: 'overlap' (in more than one set, from the
    first one) and 'set<N>' (only in the N-th set); 'exclude_parts' leaves
    some of them out (rows with 'tomoName', '*' for all, and 'parts').  With
    two sets A and B: set1 is A - B, set2 is B - A and overlap is A & B (as
    EMhub's 'Coordinates Distances' view of emw-coordinates-compare shows
    them, and merges them with its 'Merge' button).

    'edge_distance' (Angstrom) leaves out the particles closer than it to
    the tomogram box edges (before the overlaps).

    'common_columns' writes the particle columns of all sets (instead of
    requiring the same ones; a missing origin is 0), e.g. to merge picked
    coordinates with extracted particles (a TomoCoordinates set).
    """

    name = 'emw-merge-sets'

    def __init__(self, args, output):
        ProcessingPipeline.__init__(self, args, output)
        self.inputSets = self._parseInputSets(args.get('input_sets'))
        self.overlapDistance = to_float(args.get('overlap_distance'))
        if self.overlapDistance is not None and self.overlapDistance <= 0:
            self.overlapDistance = None
        self.excludeParts = self._parseExcludeParts(args.get('exclude_parts'))
        if self.excludeParts and not self.overlapDistance:
            raise Exception("'exclude_parts' needs an 'overlap_distance'.")
        self.commonColumns = to_bool(args.get('common_columns', False))
        self.edgeDistance = to_float(args.get('edge_distance'))
        if self.edgeDistance is not None and self.edgeDistance <= 0:
            self.edgeDistance = None

    def _parseExcludeParts(self, raw):
        """ The 'exclude_parts' TableParam (rows with 'tomoName', '*' for
        every tomogram, and 'parts': set<N> or overlap, separated by spaces
        or commas) as {(tomoName, part)}. """
        if isinstance(raw, str):
            from emwrap.base.job_form import _parse_table_param_value
            raw = _parse_table_param_value(raw) if raw.strip() else []
        nSets = len(self.inputSets)
        valid = {PART_OVERLAP} | {f'set{i}' for i in range(1, nSets + 1)}
        exclude = set()
        for row in raw if isinstance(raw, list) else []:
            if not isinstance(row, dict):
                continue
            tomoName = str(row.get('tomoName', '') or '').strip()
            parts = str(row.get('parts', '') or '').replace(',', ' ').split()
            if not tomoName or not parts:
                continue
            for part in parts:
                if part not in valid:
                    raise Exception(f"Invalid part {part!r} of {tomoName} in "
                                    f"'exclude_parts': one of {', '.join(sorted(valid))}")
                exclude.add((tomoName, part))
        return exclude

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

    @staticmethod
    def _mergeGeneralTables(particlesStars, tablesList, strict=True):
        """ Return the 'general' table shared by all particles STAR files (or None).

        The 'general' block holds set-level flags such as rlnTomoSubTomosAre2DStacks,
        which Relion needs to interpret the particle images correctly, so it must
        be carried over and be consistent across inputs.
        """
        refTable = refValues = refStar = None
        for particlesStar, tables in zip(particlesStars, tablesList):
            generalTable = tables.get('general')
            values = generalTable[0]._asdict() if generalTable else None
            if refStar is None:
                refTable, refValues, refStar = generalTable, values, particlesStar
            elif values != refValues:
                if not strict:  # Only if the same in every set
                    return None
                raise Exception(
                    f"Incompatible 'general' table in {particlesStar} "
                    f"({values}) vs {refStar} ({refValues})."
                )
        return refTable

    def _mergeParticlesTables(self, particlesStars, rowFilters=None,
                              dedup=True, columns=None, optics=True):
        """Merge particles.star files, renumbering optics groups and deduplicating rows.

        Optics groups are renumbered consecutively per input set. Group names are
        kept unless they clash with a name already used by a previous input
        (e.g. every WarpExport job names its groups opticsGroup1..N), in which
        case the group is renamed to opticsGroup<newId>.

        Options of subclasses (emw-coordinates-merge): rowFilters, per input,
        the rows (0-based) to keep (None: all); dedup=False keeps every row;
        columns, the particle columns written (those missing in an input
        are 0, e.g. its origin); optics=False drops the optics tables.
        """
        mergedParticles = None
        mergedOptics = None
        seenParticleKeys = set()
        skipped = 0
        nextOpticsGroupId = 0
        usedOpticsNames = set()
        usedInputs = []

        tablesList = [StarFile.getTablesDict(p) for p in particlesStars]
        generalTable = self._mergeGeneralTables(particlesStars, tablesList,
                                                strict=not columns)

        for particlesStar, tables in zip(particlesStars, tablesList):
            particlesTable = tables.get('particles')
            if not particlesTable:
                raise Exception(
                    f"Could not read 'particles' table from {particlesStar}"
                )

            if mergedParticles is None:
                mergedParticles = Table(columns or particlesTable.getColumnNames())

            # Optics group id mapping for this input only
            opticsIdMap = {}
            opticsTable = tables.get('optics') if optics else None
            if opticsTable:
                if mergedOptics is None:
                    mergedOptics = Table(opticsTable.getColumnNames())
                for row in opticsTable:
                    rowValues = row._asdict()
                    ogId = int(rowValues['rlnOpticsGroup'])
                    ogName = rowValues['rlnOpticsGroupName']
                    nextOpticsGroupId += 1
                    newName = ogName
                    if newName in usedOpticsNames:
                        newName = f"opticsGroup{nextOpticsGroupId}"
                        suffix = 1
                        while newName in usedOpticsNames:
                            newName = f"opticsGroup{nextOpticsGroupId}_{suffix}"
                            suffix += 1
                    usedOpticsNames.add(newName)
                    opticsIdMap[ogId] = nextOpticsGroupId
                    rowValues['rlnOpticsGroup'] = nextOpticsGroupId
                    rowValues['rlnOpticsGroupName'] = newName
                    mergedOptics.addRowValues(**rowValues)

            keep = rowFilters[len(usedInputs)] if rowFilters else None
            usedInputs.append(particlesStar)
            for i, row in enumerate(particlesTable):
                if keep is not None and i not in keep:
                    continue
                if dedup:
                    key = self._particleRowKey(row, particlesTable)
                    if key in seenParticleKeys:
                        skipped += 1
                        continue
                    seenParticleKeys.add(key)
                rowValues = row._asdict()
                if columns:
                    rowValues = {c: rowValues.get(c, 0) for c in columns}
                if mergedOptics and 'rlnOpticsGroup' in rowValues:
                    ogId = int(rowValues['rlnOpticsGroup'])
                    if ogId not in opticsIdMap:
                        raise Exception(
                            f"Particle in {particlesStar} references optics group "
                            f"{ogId} not defined in its 'optics' table."
                        )
                    rowValues['rlnOpticsGroup'] = opticsIdMap[ogId]
                mergedParticles.addRowValues(**rowValues)

        if not len(mergedParticles):
            raise Exception("No particles left after merging the input sets.")

        outputStar = self.join('particles.star')
        nOptics = len(mergedOptics) if mergedOptics else 0
        self.log(f"Writing {Color.green(len(mergedParticles))} particles "
                 f"({Color.warn(skipped)} duplicate(s) skipped, "
                 f"{Color.green(nOptics)} optics group(s)) to "
                 f"{Color.cyan(outputStar)}")

        with StarFile(outputStar, 'w') as sfOut:
            sfOut.writeTimeStamp()
            if generalTable:
                sfOut.writeTable('general', generalTable, singleRow=True)
            if mergedOptics:
                sfOut.writeTable('optics', mergedOptics, computeFormat='left')
            sfOut.writeTable('particles', mergedParticles, computeFormat='left')

        return outputStar, len(mergedParticles), skipped

    def _mergeOptimisationInputs(self, classifiedInputs, inputType, **particlesArgs):
        """Merge optimisation_set inputs into tomograms, particles, and
        optimisation_set STAR files (particlesArgs: options of
        _mergeParticlesTables). """
        tomoStars = [item['tomogramsStar'] for item in classifiedInputs]
        particlesStars = [item['particlesStar'] for item in classifiedInputs]

        self._mergeTomogramsForOptimisation(tomoStars)
        self._mergeParticlesTables(particlesStars, **particlesArgs)

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

    # ------------------------------------------------------------------
    # Overlaps and common columns of particle sets
    # ------------------------------------------------------------------
    def _overlapRows(self, classified):
        """ The rows (0-based) of each particles set to merge: not closer
        than the edge distance to the tomogram edges and, of the others,
        without the overlapping particles and the excluded parts; logs them. """
        from emwrap.base.coordinates_compare import (
            tomo_positions, tomo_geometry, tomo_edges, within_distance)

        positions = []
        nEdge = 0
        for item in classified:
            table = StarFile.getTableFromFile('particles', item['particlesStar'])
            tomograms = tomo_geometry(item['tomogramsStar'])
            pos = tomo_positions(table, tomograms)
            if edges := tomo_edges(pos, tomograms, self.edgeDistance):
                # Only the particles not near the edges (rows kept)
                for tomo, near in edges.items():
                    index, p, scores = pos[tomo]
                    pos[tomo] = (index[~near], p[~near], None)
                    nEdge += int(near.sum())
            positions.append(pos)
        if not self.overlapDistance:
            # Only the edges: every particle left
            return [{int(i) - 1 for p in pos.values() for i in p[0]}
                    for pos in positions], nEdge

        n = len(positions)
        keep = [set() for _ in range(n)]
        counts = OrderedDict((f'set{i + 1}', 0) for i in range(n))
        counts[PART_OVERLAP] = 0
        tomoNames = OrderedDict.fromkeys(t for p in positions for t in p)
        for tomo in tomoNames:
            sets = [p.get(tomo) for p in positions]
            nearEarlier = [None if s is None else np.zeros(len(s[0]), bool) for s in sets]
            nearLater = [None if s is None else np.zeros(len(s[0]), bool) for s in sets]
            for j in range(n):
                for k in range(j + 1, n):
                    if sets[j] is None or sets[k] is None:
                        continue
                    near = within_distance(sets[j][1], sets[k][1], self.overlapDistance)
                    if near.size:
                        nearLater[j] |= near.any(axis=1)
                        nearEarlier[k] |= near.any(axis=0)
            for k, s in enumerate(sets):
                if s is None:
                    continue
                rows = s[0] - 1  # 0-based
                # Near an earlier set: that one is added instead
                for part, mask in ((f'set{k + 1}', ~nearEarlier[k] & ~nearLater[k]),
                                   (PART_OVERLAP, ~nearEarlier[k] & nearLater[k])):
                    if ((tomo, part) in self.excludeParts
                            or ('*', part) in self.excludeParts):
                        continue
                    keep[k].update(int(i) for i in rows[mask])
                    counts[part] += int(mask.sum())

        self.log(f"Overlap distance: {Color.green(self.overlapDistance)} A, merged: "
                 + ', '.join(f"{part} {Color.green(c)}" for part, c in counts.items()))
        if self.excludeParts:
            self.log("Excluded parts: " + ' '.join(
                f"{t}:{p}" for t, p in sorted(self.excludeParts)))
        self.info['summary'] = dict(counts, overlap_distance=self.overlapDistance)
        return keep, nEdge

    def _commonColumns(self, particlesStars):
        """ (columns, optics) of common_columns: the particle columns of all
        sets (and the origin), and whether all have optics tables (else they
        are not written). """
        tables = [StarFile.getTablesDict(p) for p in particlesStars]
        cols = [t['particles'].getColumnNames() for t in tables]
        optics = all(t.get('optics') for t in tables)
        allCols = list(OrderedDict.fromkeys(c for cs in cols for c in cs))
        columns = [c for c in allCols
                   if c in ORIGIN_COLUMNS or all(c in cs for cs in cols)]
        if not optics and 'rlnOpticsGroup' in columns:
            columns.remove('rlnOpticsGroup')
        coords = [c for c in RelionStar.TOMO_PARTICLES_CENTERED_COORD_COLUMNS
                  if c not in columns]
        pixel = [c for c in RelionStar.TOMO_PARTICLES_PIXEL_COORD_COLUMNS
                 if c not in columns]
        if coords and pixel:
            raise Exception("The sets do not have the same coordinate columns.")
        if dropped := [c for c in allCols if c not in columns]:
            self.log(Color.warn("Columns not in every set, not written: "
                                f"{', '.join(dropped)}"))
        if not optics and any(t.get('optics') for t in tables):
            self.log(Color.warn("Optics tables not in every set, not written"))
        return columns, optics

    def _mergeParticleSets(self, classified):
        """ Merge optimisation sets with the overlap and columns options;
        returns (count, output type). """
        particlesStars = [item['particlesStar'] for item in classified]
        args = {}
        types = {item['inputType'] for item in classified}
        if self.commonColumns:
            columns, optics = self._commonColumns(particlesStars)
            args.update(columns=columns, optics=optics)
            tomoStars = [item['tomogramsStar'] for item in classified]
            self._validateGlobalTables(tomoStars)
            self._validateTomogramPerTomoTables(tomoStars)
            # Particles only if all are (their columns are then kept)
            inputType = types.pop() if len(types) == 1 else 'TomoCoordinates'
        else:
            if len(types) != 1:
                raise Exception("All input sets must have the same type (or use "
                                f"'common_columns'). Found: {', '.join(sorted(types))}.")
            inputType = types.pop()
            self._validateMergeInputs(classified, inputType, 'optimisation')
        if self.overlapDistance or self.edgeDistance:
            rowFilters, nEdge = self._overlapRows(classified)
            args.update(rowFilters=rowFilters)
            if self.edgeDistance:
                self.log(f"Edge distance: {Color.green(self.edgeDistance)} A, "
                         f"{Color.warn(nEdge)} particles near the edges left out")
                self.info.setdefault('summary', {}).update(
                    edge_distance=self.edgeDistance, near_edge=nEdge)
            if self.overlapDistance:
                # The overlaps replace the deduplication by particle name,
                # the names of particles of different sets may be the same
                args.update(dedup=False)
        return self._mergeOptimisationInputs(classified, inputType, **args), inputType

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
        if self.overlapDistance or self.edgeDistance or self.commonColumns:
            if not all(item['kind'] == 'optimisation' for item in classified):
                raise Exception("'overlap_distance', 'edge_distance' and "
                                "'common_columns' are only for TomoParticles / "
                                "TomoCoordinates sets.")
            count, inputType = self._mergeParticleSets(classified)
            self.writeInfo()
            self.log(f"Created merged {inputType} set with {Color.green(count)} item(s).")
            return
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

        self.writeInfo()
        self.log(f"Created merged {inputType} set with {Color.green(count)} item(s).")


if __name__ == '__main__':
    MergeSetsPipeline.main()
    
