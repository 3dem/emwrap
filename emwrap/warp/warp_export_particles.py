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
from collections import defaultdict

from emtools.utils import Color
from emtools.metadata import StarFile, Table, RelionStar
from emtools.jobs import Batch, Args

from .warp import WarpBasePipeline


WARP_TOMOGRAM_REQUIRED_COLUMNS = [
    'wrpTomostar',
    'rlnTomoReconstructedTomogram',
    'rlnTomoTiltSeriesPixelSize',
]

WARP_PARTICLE_OPTIONAL_COLUMNS = [
    'rlnAngleRot', 'rlnAngleTilt', 'rlnAnglePsi',
    'rlnLCCmax', 'rlnCutOff', 'rlnSearchStd',
    'rlnOriginXAngst', 'rlnOriginYAngst', 'rlnOriginZAngst',
]

WARP_COORD_COLUMNS = [
    'rlnCoordinateX', 'rlnCoordinateY', 'rlnCoordinateZ',
]

RELION5_CENTERED_COORD_COLUMNS = [
    'rlnCenteredCoordinateXAngst',
    'rlnCenteredCoordinateYAngst',
    'rlnCenteredCoordinateZAngst',
]

UNUSED_WARP_OUTPUTS = [
    'tomograms.star',
    'particles_tomograms.star',
    'dummy_tiltseries.mrc',
    'dummy_tiltseries.mrcs',
]


class WarpExportParticles(WarpBasePipeline):
    """Export 2D or 3D particle stacks from Warp tilt-series data."""
    name = 'emw-warp-export'

    def prerun(self):
        self._inputOptimisationSetRow = None
        self._inputTomogramsStar = None
        self._exportedParticleLookup = {}
        self._tomoLookup = {}

        if not self._register_output_only():
            self._export()
        else:
            self.log("Registering output only, skipping job execution.")

        self._output()

    def _export(self):
        tomoTable, particlesStar = self._resolveInputs()
        firstRow = tomoTable[0]

        self.log(f"Input tomograms: {Color.bold(self._inputTomogramsStar)}")
        self.log(f"Input particles: {Color.bold(particlesStar)}")
        self.log(f"Total input tomograms: {Color.green(len(tomoTable))}")

        particlesTable = self._readParticlesTable(particlesStar)
        self.log(f"Input number of particles: {Color.green(len(particlesTable))}")
        self.writeInfo()

        warpPath = self.project.join(firstRow.wrpTomostar)
        warpFolder = os.path.dirname(os.path.dirname(warpPath))

        total_pts, total_tomograms = self._buildAllCoordinates(particlesTable, tomoTable)
        self.log(f"Exporting {Color.green(total_pts)} particles "
                 f"from {Color.green(total_tomograms)} tomograms")

        # Import inputs except tomostar, that might come from a different folder
        self._importInputs(warpFolder, keys=['fs', 'fss', 'ts', 'tss'])
        self.mkdir('Particles')

        batch = Batch(id=self.name, path=self.path)
        subargs = self.get_subargs("ts_export_particles")
        outStar = "particles.star"
        args = Args({
            'WarpTools': "ts_export_particles",
            "--settings": self.TSS,
        })
        args.update(subargs)
        args.update({
            "--input_star": "all_coordinates.star",
            "--coords_angpix": RelionStar.getTomoPixelSize(firstRow),
            "--output_star": outStar,
            "--output_processing": "Particles",
            f"--{self._args['ts_export_type']}": ""  # 2d or 3d
        })
        if self.gpuList:
            args['--device_list'] = self.gpuList

        self.log("Running ts_export_particles.")
        self.batch_execute('ts_export_particles', batch, args)

    def _output(self):
        """Output the results."""
        self.log("Generating output files.")
        batch = Batch(id=self.name, path=self.path)

        # Rename Warp output STAR files that use the particles_ prefix
        for suffix in ['optimisation_set', 'particles']:
            fn = self.join(f'particles_{suffix}.star')
            if os.path.exists(fn):
                os.rename(fn, fn.replace('particles_', ''))

        ptsFn = self.join('particles.star')
        iosFn = self.join('optimisation_set.star')

        if os.path.exists(ptsFn):
            ptsTable = StarFile.getTableFromFile('particles', ptsFn)
            if ptsTable and len(ptsTable):
                self._postprocessParticlesStar(ptsFn)
            else:
                self.log(
                    "WARNING: particles.star has an empty particles table; "
                    "skipping post-processing. Check input coordinates and Warp export logs."
                )

        self._removeUnusedWarpOutputs()
        self._writeFilteredTomogramsStar()
        self._writeOptimisationSet()

        outputNodes = [[iosFn, 'TomogramGroupMetadata.star.relion.tomo.particles']]
        self.writeRelionOutputNodes(outputNodes)
        self.updateBatchInfo(batch)

    @staticmethod
    def _normalizeTomoName(name):
        base = os.path.basename(str(name))
        if base.endswith('.tomostar'):
            return base[:-len('.tomostar')]
        return base

    def _validateTomogramsStar(self, tomoStar):
        if not self.project.exists(tomoStar):
            raise Exception(f"Input tomograms STAR file not found: {tomoStar}")

        tomoTable = StarFile.getTableFromFile('global', self.project.join(tomoStar))
        if not tomoTable:
            raise Exception(f"Could not read 'global' table from {tomoStar}")

        columns = tomoTable.getColumnNames()
        if 'rlnCoordinatesMetadata' in columns:
            raise Exception(
                f"{tomoStar} looks like a PyTom tomograms.star file. "
                "Expected tomograms.star from a Warp reconstruction run."
            )

        missing = [c for c in WARP_TOMOGRAM_REQUIRED_COLUMNS if c not in columns]
        if missing:
            raise Exception(
                f"{tomoStar} is missing required Warp reconstruction columns: "
                f"{missing}"
            )
        return tomoTable

    def _resolveCoordinatesInput(self):
        """Resolve coordinates input to particles STAR and optional linked tomograms."""
        inCoords = self._args.get('input_coordinates', '')
        if not inCoords:
            raise Exception("Missing required parameter 'input_coordinates'.")

        if not self.project.exists(inCoords):
            raise Exception(f"Input coordinates STAR file not found: {inCoords}")

        absCoords = self.project.join(inCoords)
        if RelionStar.isTomoOptimisationSet(absCoords):
            row = RelionStar.readTomoOptimisationSet(absCoords)[0]
            particlesStar = row.rlnTomoParticlesFile
            if not self.project.exists(particlesStar):
                raise Exception(
                    f"Particles STAR file '{particlesStar}' referenced in "
                    f"{inCoords} was not found."
                )

            optRow = row._asdict()
            tomogramsStar = optRow.get('rlnTomoTomogramsFile') or ''
            if tomogramsStar and not self.project.exists(tomogramsStar):
                raise Exception(
                    f"Tomograms STAR file '{tomogramsStar}' referenced in "
                    f"{inCoords} was not found."
                )
            return particlesStar, optRow, tomogramsStar or None

        if RelionStar.isTomoParticles(absCoords):
            return inCoords, None, None

        raise Exception(
            f"{inCoords} is not a compliant tomography optimisation_set or "
            "particles STAR file."
        )

    def _ensureInputsResolved(self):
        if self._inputTomogramsStar is None:
            self._resolveInputs()

    def _resolveInputs(self):
        particlesStar, optRow, _tomogramsFromOpt = self._resolveCoordinatesInput()
        self._inputOptimisationSetRow = optRow

        tomogramsStar = self._args.get('input_tomograms', '')
        if not tomogramsStar:
            raise Exception(
                "Missing required parameter 'input_tomograms'. "
                "Provide tomograms.star from a Warp CTF/reconstruction (emw-warp-ctfrec) job."
            )
        self._inputTomogramsStar = tomogramsStar

        tomoTable = self._validateTomogramsStar(self._inputTomogramsStar)
        return tomoTable, particlesStar

    def _readParticlesTable(self, particlesStar):
        absPath = self.project.join(particlesStar)
        if not RelionStar.isTomoParticles(absPath):
            raise Exception(
                f"Particles STAR file {particlesStar} is not a compliant "
                "tomography particles STAR file."
            )
        return RelionStar.readTomoParticles(absPath)

    def _buildTomoLookup(self, tomoTable):
        lookup = {}
        for row in tomoTable:
            lookup[self._normalizeTomoName(row.rlnTomoName)] = row
        return lookup

    def _reconstructedTomoSize(self, tomoRow, axis):
        """Return reconstructed tomogram size in pixels along X/Y/Z."""
        return float(getattr(tomoRow, axis)) / RelionStar.getTomoBinning(tomoRow)

    def _centeredAngstToPixel(self, centeredAngst, tomoRow, axis):
        """Convert Relion centered Angstrom coords to Warp pixel coordinates.

        Coordinates must refer to the binned reconstructed tomogram grid
        (rlnTomoSize / binning), not the unbinned tomogram dimensions.
        """
        return centeredAngst / RelionStar.getTomoPixelSize(tomoRow) + self._reconstructedTomoSize(tomoRow, axis) / 2

    def _pixelToCenteredAngst(self, pixel, tomoRow, axis):
        """Convert Warp pixel coordinates back to Relion centered Angstrom coords."""
        return (float(pixel) - self._reconstructedTomoSize(tomoRow, axis) / 2) * RelionStar.getTomoPixelSize(tomoRow)

    def _inputCenteredCoords(self, inputRow, tomoRow):
        """Return Relion 5 centered Angstrom coordinates from an input particle row."""
        if 'rlnCenteredCoordinateXAngst' in inputRow:
            return {
                col: inputRow[col]
                for col in RELION5_CENTERED_COORD_COLUMNS
            }
        return {
            'rlnCenteredCoordinateXAngst': self._pixelToCenteredAngst(
                inputRow['rlnCoordinateX'], tomoRow, 'rlnTomoSizeX'),
            'rlnCenteredCoordinateYAngst': self._pixelToCenteredAngst(
                inputRow['rlnCoordinateY'], tomoRow, 'rlnTomoSizeY'),
            'rlnCenteredCoordinateZAngst': self._pixelToCenteredAngst(
                inputRow['rlnCoordinateZ'], tomoRow, 'rlnTomoSizeZ'),
        }

    def _particleToWarpRow(self, row, tomoRow):
        """Build a Warp-compatible particle row.

        Warp ts_export_particles requires rlnCoordinateX/Y/Z in pixel units at
        --coords_angpix. Relion 5 rlnCenteredCoordinate*Angst values are
        converted to the reconstructed tomogram pixel grid.
        """
        values = row._asdict()

        if 'rlnCenteredCoordinateXAngst' in values:
            x = self._centeredAngstToPixel(float(values['rlnCenteredCoordinateXAngst']),
                                           tomoRow, 'rlnTomoSizeX')
            y = self._centeredAngstToPixel(float(values['rlnCenteredCoordinateYAngst']),
                                           tomoRow, 'rlnTomoSizeY')
            z = self._centeredAngstToPixel(float(values['rlnCenteredCoordinateZAngst']),
                                           tomoRow, 'rlnTomoSizeZ')
        elif 'rlnCoordinateX' in values:
            x = float(values['rlnCoordinateX'])
            y = float(values['rlnCoordinateY'])
            z = float(values['rlnCoordinateZ'])
        else:
            raise Exception(
                "Particles STAR file must contain either rlnCoordinateX/Y/Z or "
                "rlnCenteredCoordinateXAngst/YAngst/ZAngst columns."
            )

        warpRow = {
            'rlnCoordinateX': x,
            'rlnCoordinateY': y,
            'rlnCoordinateZ': z,
            'rlnTomoName': os.path.basename(tomoRow.wrpTomostar),
        }
        for col in WARP_PARTICLE_OPTIONAL_COLUMNS:
            if col in values:
                warpRow[col] = values[col]
        return warpRow

    def _iterExportedParticles(self, particlesTable, tomoTable):
        """Yield particles that will be exported, in Warp export order."""
        particlesMin = int(self._args['filters.particles_min'])
        particlesMax = int(self._args['filters.particles_max'])
        tomoLookup = self._buildTomoLookup(tomoTable)
        byTomo = defaultdict(list)
        missingLogged = set()

        for row in particlesTable:
            tomoKey = self._normalizeTomoName(row.rlnTomoName)
            if tomoKey not in tomoLookup:
                if tomoKey not in missingLogged:
                    self.log(
                        f"Ignoring particles with missing rlnTomoName: {row.rlnTomoName}"
                    )
                    missingLogged.add(tomoKey)
                continue
            byTomo[tomoKey].append(row)

        particleIds = defaultdict(int)
        for tomoKey, rows in byTomo.items():
            n = len(rows)
            if n < particlesMin or n > particlesMax:
                self.log(f"Skipping tomogram {tomoKey} with {n} particles")
                continue

            tomoRow = tomoLookup[tomoKey]
            for row in rows:
                particleIds[tomoKey] += 1
                yield tomoKey, tomoRow, row, particleIds[tomoKey]

    def _ensureExportedParticleLookup(self):
        """Build input-particle lookup keyed by (tomoName, particleId)."""
        if self._exportedParticleLookup:
            return

        tomoTable, particlesStar = self._resolveInputs()
        particlesTable = self._readParticlesTable(particlesStar)
        self._tomoLookup = self._buildTomoLookup(tomoTable)

        for tomoKey, tomoRow, row, particleId in self._iterExportedParticles(
                particlesTable, tomoTable):
            self._exportedParticleLookup[(tomoKey, particleId)] = row._asdict()

    def _buildAllCoordinates(self, particlesTable, tomoTable):
        """Build Warp input coordinates and copy required tomostar files."""
        outStarFile = self.join('all_coordinates.star')
        self.log(f"Writing output star file: {Color.bold(outStarFile)}")
        self._tomoLookup = self._buildTomoLookup(tomoTable)
        self._exportedParticleLookup = {}

        totalPts = 0
        totalTomograms = 0
        outTM = self.mkdir(self.TM)
        copiedTomostars = set()
        currentTomo = None

        with StarFile(outStarFile, 'w') as sfOut:
            newTable = None

            for tomoKey, tomoRow, row, particleId in self._iterExportedParticles(
                    particlesTable, tomoTable):
                if tomoKey != currentTomo:
                    currentTomo = tomoKey
                    totalTomograms += 1
                    tomostar = self.project.join(tomoRow.wrpTomostar)
                    if tomostar not in copiedTomostars:
                        dst = os.path.join(outTM, os.path.basename(tomostar))
                        if not os.path.exists(dst):
                            shutil.copy(tomostar, dst)
                        copiedTomostars.add(tomostar)

                self._exportedParticleLookup[(tomoKey, particleId)] = row._asdict()
                warpRow = self._particleToWarpRow(row, tomoRow)
                if newTable is None:
                    newTable = Table(list(warpRow.keys()))
                    sfOut.writeTimeStamp()
                    sfOut.writeHeader('particles', newTable)
                sfOut.writeRowValues(warpRow)
                totalPts += 1

        if totalPts == 0:
            raise Exception("No particles left to export after filtering.")

        return totalPts, totalTomograms

    def _collectExportedTomoNames(self, particlesTable, tomoTable):
        """Return normalized tomogram names kept after the particle-count filter."""
        return {
            tomoKey
            for tomoKey, _tomoRow, _row, _particleId in self._iterExportedParticles(
                particlesTable, tomoTable)
        }

    def _writeFilteredTomogramsStar(self):
        """Write tomograms.star subset for tomograms that passed the particle filter."""
        self._ensureInputsResolved()
        tomoTable, particlesStar = self._resolveInputs()
        particlesTable = self._readParticlesTable(particlesStar)
        subsetNames = self._collectExportedTomoNames(particlesTable, tomoTable)

        inputStar = self.project.join(self._inputTomogramsStar)
        inputTable = StarFile.getTableFromFile('global', inputStar)
        if not inputTable:
            raise Exception(
                f"Could not read 'global' table from {self._inputTomogramsStar}"
            )

        filtered = Table(inputTable.getColumnNames())
        for row in inputTable:
            if self._normalizeTomoName(row.rlnTomoName) in subsetNames:
                filtered.addRow(row)

        if not len(filtered):
            raise Exception(
                "No tomograms from the input tomograms.star matched tomograms "
                "with exported particles."
            )

        outputStar = self.join('tomograms.star')
        self.log(
            f"Writing {Color.green(len(filtered))} / "
            f"{Color.bold(len(inputTable))} tomograms to {Color.cyan(outputStar)}"
        )

        with StarFile(inputStar) as sfIn:
            tableNames = sfIn.getTableNames()

        with StarFile(outputStar, 'w') as sfOut:
            sfOut.writeTable('global', filtered, computeFormat='left', timeStamp=True)
            for tableName in tableNames:
                if tableName == 'global':
                    continue
                if self._normalizeTomoName(tableName) not in subsetNames:
                    continue
                table = StarFile.getTableFromFile(tableName, inputStar)
                sfOut.writeTable(tableName, table, computeFormat='left')

        return outputStar

    def _writeOptimisationSet(self):
        iosFn = self.join('optimisation_set.star')

        self._ensureInputsResolved()

        if self._inputOptimisationSetRow:
            values = dict(self._inputOptimisationSetRow)
        else:
            values = {}

        values['rlnTomoParticlesFile'] = self.fixOutputPath('particles.star')
        values['rlnTomoTomogramsFile'] = self.fixOutputPath('tomograms.star')

        with StarFile(iosFn, 'w') as sf:
            sf.writeTable('optimisation_set', Table.fromDict(values), timeStamp=True)

    def _particlesOutputColumns(self, warpColumns):
        """Build output column order with Relion 5 centered coordinates."""
        outputColumns = []
        centeredAdded = False
        for col in warpColumns:
            if col in WARP_COORD_COLUMNS:
                continue
            outputColumns.append(col)
            if col == 'rlnTomoName' and not centeredAdded:
                outputColumns.extend(RELION5_CENTERED_COORD_COLUMNS)
                centeredAdded = True
        if not centeredAdded:
            outputColumns.extend(RELION5_CENTERED_COORD_COLUMNS)
        return outputColumns

    def _postprocessParticlesStar(self, ptsFn):
        """Fix paths, tomogram names, and restore Relion 5 centered coordinates."""
        self._ensureExportedParticleLookup()
        pathLabels = ['rlnImageName', 'rlnCtfImage']
        starFnOut = ptsFn.replace('.star', '_fixed.star')

        with StarFile(ptsFn) as sf:
            with StarFile(starFnOut, 'w') as sfOut:
                sfOut.writeTimeStamp()
                for tn in sf.getTableNames():
                    table = sf.getTable(tn, guessType=False)

                    if tn == 'particles':
                        outputColumns = self._particlesOutputColumns(
                            table.getColumnNames()
                        )
                        newTable = Table(outputColumns)
                        for row in table:
                            rowDict = row._asdict()
                            tomoKey = self._normalizeTomoName(rowDict['rlnTomoName'])
                            rowDict['rlnTomoName'] = tomoKey

                            particleId = int(rowDict.get('rlnTomoParticleId', 0))
                            inputRow = self._exportedParticleLookup.get(
                                (tomoKey, particleId)
                            )
                            if inputRow:
                                tomoRow = self._tomoLookup[tomoKey]
                                rowDict.update(
                                    self._inputCenteredCoords(inputRow, tomoRow)
                                )
                            else:
                                self.log(
                                    f"WARNING: no input particle for "
                                    f"{tomoKey} particle {particleId}"
                                )

                            for col in WARP_COORD_COLUMNS:
                                rowDict.pop(col, None)
                            for label in pathLabels:
                                if label in rowDict:
                                    rowDict[label] = self.join(rowDict[label])

                            newTable.addRowValues(**rowDict)
                    else:
                        newTable = table

                    singleRow = len(newTable) == 1
                    sfOut.writeTable(
                        tn, newTable, computeFormat='right', singleRow=singleRow
                    )
        shutil.move(starFnOut, ptsFn)

    def _removeUnusedWarpOutputs(self):
        """Remove Warp outputs that are not needed downstream."""
        for fn in UNUSED_WARP_OUTPUTS:
            path = self.join(fn)
            if os.path.exists(path):
                os.remove(path)
                self.log(f"Removed unused Warp output: {Color.bold(fn)}")


if __name__ == '__main__':
    WarpExportParticles.main()
