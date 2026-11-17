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
import time
from collections import OrderedDict

import numpy as np

from emtools.utils import Color, Pretty
from emtools.metadata import StarFile, Table, RelionStar

from emwrap.base import ProcessingPipeline


# Max number of float64 distances computed at once (rows x cols of a block),
# ~64 MB, so a very large tomogram does not need the full matrix in memory.
_BLOCK_SIZE = 8 * 1024 * 1024


class CoordinatesComparePipeline(ProcessingPipeline):
    """ Compute the distances between the coordinates (or particles) of two
    tomography sets, tomogram by tomogram.

    Input: a TomoCoordinates or TomoParticles set (optimisation_set.star or
    particles.star) and a second set to compare to, or the input set itself
    (to find duplicates).  Only the coordinates in the same tomogram
    (rlnTomoName) are compared.  The positions are in Angstrom and include
    the refined shifts:

        pos = rlnCenteredCoordinate(X/Y/Z)Angst - rlnOrigin(X/Y/Z)Angst

    (pixel coordinates, rlnCoordinate(X/Y/Z), are converted to Angstrom
    with the tomogram pixel size from the tomograms.star file).

    Outputs:
        distances.star: table 'distances', one row per pair within the
            maximum distance (all pairs if not set): rlnTomoName,
            emwIndex1, emwIndex2 (1-based rows in the particles table of
            set 1 and set 2) and emwDistanceAngst.  When comparing a set to
            itself, only pairs with emwIndex1 < emwIndex2 are written.
        nearest.star: the particles table of set 1 (plus its 'general' and
            'optics' tables) with: emwNearestIndex and
            emwNearestDistanceAngst (closest coordinate in set 2, or 0 and
            -1 if the tomogram is not in set 2) and emwNeighbors (number of
            coordinates in set 2 within the maximum distance).
        distances.npz (optional): per tomogram, the full float32 distance
            matrix (key = rlnTomoName) and the 1-based rows of set 1 and set
            2 ('<tomo>.index1', '<tomo>.index2').
    """
    name = 'emw-coordinates-compare'
    OUTPUT_TYPE = 'TomogramGroupMetadata.star.emwrap.CoordinatesDistances'
    OUTPUT_STAR = 'distances.star'
    NEAREST_STAR = 'nearest.star'
    OUTPUT_NPZ = 'distances.npz'

    def __init__(self, args, output):
        ProcessingPipeline.__init__(self, args, output)
        self.inputSet = (args.get('input_set') or '').strip()
        self.compareSelf = self._bool(args.get('compare_self', False))
        self.secondSet = (args.get('second_set') or '').strip()
        self.maxDistance = self._float(args.get('max_distance'), 'max_distance')
        self.writeNpz = self._bool(args.get('write_npz', False))

    # ------------------------------------------------------------------
    # Params
    # ------------------------------------------------------------------
    @staticmethod
    def _bool(value):
        """ A form boolean, which may come as text ('false', '0', 'no'). """
        if isinstance(value, str):
            return value.strip().lower() not in ('false', '0', 'no', '')
        return bool(value)

    @staticmethod
    def _float(value, name):
        """ A positive float, or None for an empty (or 0) value. """
        if value is None or str(value).strip() == '':
            return None
        try:
            v = float(value)
        except ValueError:
            raise Exception(f"Invalid value for '{name}': {value}")
        if v < 0:
            raise Exception(f"'{name}' can not be negative: {value}")
        return v or None

    # ------------------------------------------------------------------
    # Input
    # ------------------------------------------------------------------
    def _loadSet(self, inputStar, label):
        """ Read a TomoCoordinates / TomoParticles set.

        Returns a dict with the particles STAR file, its tables, and the
        positions (Angstrom) grouped by tomogram:
            {tomoName: (indexes (1-based rows), Nx3 float64 array)}
        """
        if not inputStar:
            raise Exception(f"Missing {label}.")
        # Paths are relative to the project folder (also the ones inside
        # the optimisation_set.star file)
        inputFile = self.projectFile(inputStar)
        if not os.path.exists(inputFile):
            raise Exception(f"{label} not found: {inputStar}")

        tomoStar = None
        if RelionStar.isTomoOptimisationSet(inputFile):
            optRow = RelionStar.readTomoOptimisationSet(inputFile)[0]
            particlesStar = self.projectFile(optRow.rlnTomoParticlesFile)
            if tomoStar := getattr(optRow, 'rlnTomoTomogramsFile', None):
                tomoStar = self.projectFile(tomoStar)
        elif RelionStar.isTomoParticles(inputFile):
            particlesStar = inputFile
        else:
            raise Exception(
                f"{label} ({inputStar}) is not a TomoCoordinates or "
                "TomoParticles set (optimisation_set.star or particles.star).")

        if not RelionStar.isTomoParticles(particlesStar):
            raise Exception(
                f"{label}: {particlesStar} is not a tomography particles "
                "STAR file (needs rlnTomoName and coordinate columns).")

        tables = StarFile.getTablesDict(particlesStar)
        table = tables['particles']
        positions = self._positions(table, tomoStar, label)

        self.log(f"{label}: {Color.cyan(inputStar)}, "
                 f"{Color.green(len(table))} coordinates in "
                 f"{Color.green(len(positions))} tomograms")

        return {
            'input': inputStar,
            'particlesStar': particlesStar,
            'tables': tables,
            'positions': positions
        }

    @staticmethod
    def _column(table, name, default=0.0):
        """ Column values as a float64 array (default if missing). """
        if table.hasColumn(name):
            return np.array(table.getColumnValues(name), dtype=np.float64)
        return np.full(len(table), default, dtype=np.float64)

    def _positions(self, table, tomoStar, label):
        """ Return {tomoName: (indexes, Nx3 positions in Angstrom)}. """
        tomoNames = table.getColumnValues('rlnTomoName')
        origin = np.stack([self._column(table, f'rlnOrigin{a}Angst')
                           for a in 'XYZ'], axis=1)

        if table.hasAllColumns(RelionStar.TOMO_PARTICLES_CENTERED_COORD_COLUMNS):
            coords = np.stack(
                [self._column(table, c)
                 for c in RelionStar.TOMO_PARTICLES_CENTERED_COORD_COLUMNS],
                axis=1)
        else:
            # Pixel coordinates (in the reconstructed tomogram), they need
            # the tomogram pixel size to be in Angstrom
            if not tomoStar or not os.path.exists(tomoStar):
                raise Exception(
                    f"{label} has pixel coordinates (rlnCoordinateX/Y/Z): a "
                    "tomograms.star file (from an optimisation_set.star) is "
                    "needed for the tomogram pixel sizes.")
            pixelSizes = {row.rlnTomoName: RelionStar.getTomoPixelSize(row)
                          for row in StarFile.getTableFromFile('global', tomoStar)}
            missing = set(tomoNames) - set(pixelSizes)
            if missing:
                raise Exception(
                    f"{label}: tomograms not found in {tomoStar}: "
                    f"{', '.join(sorted(missing)[:10])}")
            coords = np.stack(
                [self._column(table, c)
                 for c in RelionStar.TOMO_PARTICLES_PIXEL_COORD_COLUMNS],
                axis=1)
            coords *= np.array([pixelSizes[t] for t in tomoNames])[:, None]

        coords -= origin

        groups = OrderedDict()
        for i, tomoName in enumerate(tomoNames):
            groups.setdefault(tomoName, []).append(i)

        return OrderedDict(
            (tomoName, (np.array(rows) + 1, coords[rows]))
            for tomoName, rows in groups.items())

    # ------------------------------------------------------------------
    # Distances
    # ------------------------------------------------------------------
    @staticmethod
    def _distanceBlocks(pos1, pos2):
        """ Yield (rowStart, block) with the distances of a block of rows of
        pos1 to all pos2, so memory is bounded for large tomograms. """
        n2 = max(len(pos2), 1)
        step = max(1, _BLOCK_SIZE // n2)
        sq2 = np.einsum('ij,ij->i', pos2, pos2)
        for start in range(0, len(pos1), step):
            p1 = pos1[start:start + step]
            sq1 = np.einsum('ij,ij->i', p1, p1)
            # |a - b|^2 = |a|^2 + |b|^2 - 2 a.b (fast, matrix product)
            d2 = sq1[:, None] + sq2[None, :] - 2.0 * (p1 @ pos2.T)
            np.maximum(d2, 0.0, out=d2)  # Rounding can give tiny negatives
            yield start, np.sqrt(d2, out=d2)

    def _compareTomo(self, idx1, pos1, idx2, pos2):
        """ Compare the coordinates of one tomogram.

        Returns (pairs, nearestIndex, nearestDist, neighbors, matrix):
            pairs: (i1, i2, dist) arrays of the pairs to write (1-based rows).
            nearest*/neighbors: per coordinate of set 1.
            matrix: the full float32 matrix (only if writing the npz file).
        """
        n1, n2 = len(pos1), len(pos2)
        nearestIndex = np.zeros(n1, dtype=np.int64)
        nearestDist = np.full(n1, -1.0)
        neighbors = np.zeros(n1, dtype=np.int64)
        matrix = np.empty((n1, n2), dtype=np.float32) if self.writeNpz else None
        pairs1, pairs2, pairsD = [], [], []

        for start, d in self._distanceBlocks(pos1, pos2):
            rows = np.arange(start, start + len(d))
            if matrix is not None:
                matrix[start:start + len(d)] = d
            if self.compareSelf:
                # Skip i == j and keep each pair once (i < j)
                d[np.arange(len(d)), rows] = np.inf
                keep = rows[:, None] < np.arange(n2)[None, :]
            else:
                keep = np.ones(d.shape, dtype=bool)

            if n2:
                j = np.argmin(d, axis=1)
                dmin = d[np.arange(len(d)), j]
                valid = np.isfinite(dmin)
                nearestIndex[rows[valid]] = idx2[j[valid]]
                nearestDist[rows[valid]] = dmin[valid]

            within = (d <= self.maxDistance) if self.maxDistance else np.isfinite(d)
            neighbors[rows] = within.sum(axis=1)

            r, c = np.nonzero(within & keep)
            pairs1.append(idx1[rows[r]])
            pairs2.append(idx2[c])
            pairsD.append(d[r, c])

        def _cat(arrays, dtype):
            return np.concatenate(arrays) if arrays else np.empty(0, dtype=dtype)

        pairs = (_cat(pairs1, np.int64), _cat(pairs2, np.int64),
                 _cat(pairsD, np.float64))
        return pairs, nearestIndex, nearestDist, neighbors, matrix

    # ------------------------------------------------------------------
    # Output
    # ------------------------------------------------------------------
    def _writeDistances(self, results):
        """ Write the pairs to distances.star (fast, column-wise lines). """
        outStar = self.join(self.OUTPUT_STAR)
        columns = ['rlnTomoName', 'emwIndex1', 'emwIndex2', 'emwDistanceAngst']
        with open(outStar, 'w') as f:
            f.write(f"\n# Created by {self.name} at {Pretty.now()}\n\n"
                    "data_distances\n\nloop_\n")
            for i, c in enumerate(columns):
                f.write(f"_{c} #{i + 1}\n")
            for tomoName, (i1, i2, d) in results:
                if len(d):
                    lines = [f"{tomoName}\t{a}\t{b}\t{x:.3f}\n"
                             for a, b, x in zip(i1.tolist(), i2.tolist(),
                                                d.tolist())]
                    f.writelines(lines)
            f.write('\n')
        return outStar

    def _writeNearest(self, set1, nearestIndex, nearestDist, neighbors):
        """ Write set 1 particles with the nearest neighbour columns. """
        tables = set1['tables']
        table = tables['particles']
        newColumns = ['emwNearestIndex', 'emwNearestDistanceAngst', 'emwNeighbors']
        columns = [c for c in table.getColumnNames() if c not in newColumns]
        outTable = Table(columns + newColumns)
        for i, row in enumerate(table):
            values = {c: getattr(row, c) for c in columns}
            values['emwNearestIndex'] = int(nearestIndex[i])
            values['emwNearestDistanceAngst'] = round(float(nearestDist[i]), 3)
            values['emwNeighbors'] = int(neighbors[i])
            outTable.addRowValues(**values)

        outStar = self.join(self.NEAREST_STAR)
        with StarFile(outStar, 'w') as sfOut:
            sfOut.writeTimeStamp()
            if general := tables.get('general'):
                sfOut.writeTable('general', general, singleRow=True)
            if optics := tables.get('optics'):
                sfOut.writeTable('optics', optics, computeFormat='left')
            sfOut.writeTable('particles', outTable, computeFormat='left')
        return outStar

    # ------------------------------------------------------------------
    # Job
    # ------------------------------------------------------------------
    def prerun(self):
        set1 = self._loadSet(self.inputSet, 'Input set')
        if self.compareSelf:
            set2 = set1
            self.log("Comparing the input set to itself (duplicates)")
        else:
            set2 = self._loadSet(self.secondSet, 'Second set')

        maxDistStr = (f"{self.maxDistance} A" if self.maxDistance
                      else "none (all pairs)")
        self.log(f"Maximum distance: {Color.green(maxDistStr)}")

        n1 = len(set1['tables']['particles'])
        nearestIndex = np.zeros(n1, dtype=np.int64)
        nearestDist = np.full(n1, -1.0)
        neighbors = np.zeros(n1, dtype=np.int64)
        results = []
        npzArrays = {}
        nCompared = 0
        t0 = time.time()

        for tomoName, (idx1, pos1) in set1['positions'].items():
            if tomoName not in set2['positions']:
                continue
            idx2, pos2 = set2['positions'][tomoName]
            pairs, nIdx, nDist, nNeigh, matrix = self._compareTomo(
                idx1, pos1, idx2, pos2)
            rows = idx1 - 1
            nearestIndex[rows] = nIdx
            nearestDist[rows] = nDist
            neighbors[rows] = nNeigh
            results.append((tomoName, pairs))
            nCompared += len(pos1) * len(pos2)
            if matrix is not None:
                npzArrays[tomoName] = matrix
                npzArrays[f'{tomoName}.index1'] = idx1.astype(np.int32)
                npzArrays[f'{tomoName}.index2'] = idx2.astype(np.int32)

        tCompute = time.time() - t0
        nPairs = sum(len(p[2]) for _, p in results)
        self.log(f"Compared {Color.green(len(results))} tomograms, "
                 f"{Color.green(nCompared)} distances in {tCompute:.3f} s")

        t0 = time.time()
        outStar = self._writeDistances(results)
        self.log(f"Wrote {Color.green(nPairs)} pairs to {Color.cyan(outStar)} "
                 f"({time.time() - t0:.2f} s)")
        nearestStar = self._writeNearest(set1, nearestIndex, nearestDist, neighbors)
        self.log(f"Wrote nearest neighbours to {Color.cyan(nearestStar)}")

        outputs = {
            'type': 'CoordinatesDistances',
            'star': self.fixOutputPath(self.OUTPUT_STAR),
            'nearest': self.fixOutputPath(self.NEAREST_STAR)
        }
        if self.writeNpz:
            t0 = time.time()
            npzFile = self.join(self.OUTPUT_NPZ)
            np.savez(npzFile, **npzArrays)
            self.log(f"Wrote distance matrices to {Color.cyan(npzFile)} "
                     f"({time.time() - t0:.2f} s)")
            outputs['npz'] = self.fixOutputPath(self.OUTPUT_NPZ)

        if self.maxDistance:
            nDup = int((neighbors > 0).sum())
            self.log(f"{Color.warn(nDup)} of {n1} coordinates have at least "
                     f"one neighbour within {self.maxDistance} A")

        self.outputs = outputs
        self.info['summary'] = {
            'tomograms': len(results),
            'coordinates': n1,
            'pairs': nPairs,
            'max_distance': self.maxDistance,
            'with_neighbors': int((neighbors > 0).sum())
        }
        self.writeRelionOutputNodes(
            [[self.fixOutputPath(self.OUTPUT_STAR), self.OUTPUT_TYPE]])
        self.writeInfo()


if __name__ == '__main__':
    CoordinatesComparePipeline.main()
