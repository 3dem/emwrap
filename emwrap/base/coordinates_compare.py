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

from emtools.utils import Color
from emtools.metadata import StarFile, RelionStar

from emwrap.base import ProcessingPipeline
from emwrap.base.wizards import to_bool, to_float


# Max number of float64 distances computed at once (rows x cols of a block),
# ~64 MB, so a very large tomogram does not need the full matrix in memory.
_BLOCK_SIZE = 8 * 1024 * 1024

# Score of the coordinates (the first column found), to keep the best one of
# the duplicates: picking scores first, then the refinement one
SCORE_COLUMNS = ('rlnLCCmax', 'rlnAutopickFigureOfMerit', 'rlnScore',
                 'rlnConfidence', 'rlnMaxValueProbDistribution')


def duplicates(matrix, distance, scores=None):
    """ The duplicates of a tomogram compared to itself (bool array of
    its coordinates), with the (n, n) distance matrix.

    Without scores, as Relion's 'remove duplicates': i is a duplicate if a
    later coordinate (j > i) is within the distance.  With scores, from the
    highest score down, a coordinate is kept and the ones within the
    distance of it are duplicates (on equal scores, the later one is kept).
    """
    near = np.asarray(matrix) <= distance
    n = near.shape[0]
    if scores is None:
        return np.triu(near, k=1).any(axis=1)
    scores = np.nan_to_num(np.asarray(scores, dtype=np.float64), nan=-np.inf)
    dup = np.zeros(n, dtype=bool)
    # lexsort: last key first, score descending, then index descending
    for i in np.lexsort((-np.arange(n), -scores)):
        if not dup[i]:
            # None of i's neighbours was kept (i would be a duplicate)
            dup |= near[i]
            dup[i] = False
    return dup



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

    Output: distances.npz, per tomogram in both sets, the full float32
    distance matrix (key = rlnTomoName; rows: set 1, columns: set 2, in the
    order of their particles tables), the 1-based rows of set 1 and set 2
    in their particles tables ('<tomo>.index1', '<tomo>.index2') and, when
    the set is compared to itself and it has one of SCORE_COLUMNS, the
    score of its coordinates ('<tomo>.score', to keep the best of the
    duplicates, see duplicates()).  EMhub
    reads it to show the results at any distance (its 'Coordinates
    Distances' view); the maximum distance is the one it shows first.

    The job registers no output node: its results are not a set to use as
    input of other jobs (EMhub shows them as a job view, a 'fake' output).
    The inputs and the maximum distance are in info.json ('inputs').
    """
    name = 'emw-coordinates-compare'
    OUTPUT_NPZ = 'distances.npz'

    def __init__(self, args, output):
        ProcessingPipeline.__init__(self, args, output)
        self.inputSet = (args.get('input_set') or '').strip()
        self.compareSelf = to_bool(args.get('compare_self', False))
        self.secondSet = (args.get('second_set') or '').strip()
        self.maxDistance = to_float(args.get('max_distance'))
        if not self.maxDistance or self.maxDistance <= 0:
            raise Exception("'max_distance' must be a distance > 0 (Angstrom), "
                            f"got: {args.get('max_distance')!r}")

    # ------------------------------------------------------------------
    # Input
    # ------------------------------------------------------------------
    def _loadSet(self, inputStar, label):
        """ Read a TomoCoordinates / TomoParticles set.

        Returns {'size': coordinates, 'scoreColumn': score column or None,
        'positions': {tomoName: (indexes (1-based rows), Nx3 float64
        array of positions in Angstrom, scores or None)}}
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

        table = StarFile.getTableFromFile('particles', particlesStar)
        scoreColumn = next((c for c in SCORE_COLUMNS if table.hasColumn(c)), None)
        positions = self._positions(table, tomoStar, label, scoreColumn)

        self.log(f"{label}: {Color.cyan(inputStar)}, "
                 f"{Color.green(len(table))} coordinates in "
                 f"{Color.green(len(positions))} tomograms, score: "
                 f"{Color.cyan(scoreColumn or 'none')}")

        return {'size': len(table), 'scoreColumn': scoreColumn,
                'positions': positions}

    @staticmethod
    def _column(table, name, default=0.0):
        """ Column values as a float64 array (default if missing). """
        if table.hasColumn(name):
            return np.array(table.getColumnValues(name), dtype=np.float64)
        return np.full(len(table), default, dtype=np.float64)

    def _positions(self, table, tomoStar, label, scoreColumn):
        """ Return {tomoName: (indexes, Nx3 positions in Angstrom,
        scores or None)}. """
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
        scores = self._column(table, scoreColumn) if scoreColumn else None

        groups = OrderedDict()
        for i, tomoName in enumerate(tomoNames):
            groups.setdefault(tomoName, []).append(i)

        return OrderedDict(
            (tomoName, (np.array(rows) + 1, coords[rows],
                        None if scores is None else scores[rows]))
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

    def _distanceMatrix(self, pos1, pos2):
        """ float32 (n1, n2) distances (Angstrom) of a tomogram. """
        matrix = np.empty((len(pos1), len(pos2)), dtype=np.float32)
        for start, d in self._distanceBlocks(pos1, pos2):
            matrix[start:start + len(d)] = d
        return matrix

    def _withNeighbors(self, matrix, scores):
        """ Coordinates of set 1 with another one (of set 2) within the
        maximum distance, for the job summary: the duplicates when the set
        is compared to itself. """
        if self.compareSelf:
            return int(duplicates(matrix, self.maxDistance, scores).sum())
        within = matrix <= self.maxDistance
        return int(within.any(axis=1).sum()) if within.size else 0

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

        self.log(f"Maximum distance: {Color.green(self.maxDistance)} A")
        self.inputs = {
            'input_set': self.inputSet,
            'second_set': '' if self.compareSelf else self.secondSet,
            'compare_self': self.compareSelf,
            'max_distance': self.maxDistance,
        }

        npzArrays = {}
        nTomos = nCompared = nNeighbors = 0
        t0 = time.time()
        for tomoName, (idx1, pos1, scores) in set1['positions'].items():
            if tomoName not in set2['positions']:
                continue
            idx2, pos2, _ = set2['positions'][tomoName]
            if not self.compareSelf:
                scores = None
            matrix = self._distanceMatrix(pos1, pos2)
            nNeighbors += self._withNeighbors(matrix, scores)
            nTomos += 1
            nCompared += matrix.size
            npzArrays[tomoName] = matrix
            npzArrays[f'{tomoName}.index1'] = idx1.astype(np.int32)
            npzArrays[f'{tomoName}.index2'] = idx2.astype(np.int32)
            if scores is not None:
                npzArrays[f'{tomoName}.score'] = scores.astype(np.float32)
        self.log(f"Compared {Color.green(nTomos)} tomograms, "
                 f"{Color.green(nCompared)} distances in {time.time() - t0:.3f} s")

        t0 = time.time()
        npzFile = self.join(self.OUTPUT_NPZ)
        np.savez(npzFile, **npzArrays)
        self.log(f"Wrote distance matrices to {Color.cyan(npzFile)} "
                 f"({time.time() - t0:.2f} s)")
        if self.compareSelf:
            self.log(f"{Color.warn(nNeighbors)} of {set1['size']} coordinates are "
                     f"duplicates at {self.maxDistance} A (kept the "
                     + (f"highest {set1['scoreColumn']})" if set1['scoreColumn']
                        else "last one, no score)"))
        else:
            self.log(f"{Color.warn(nNeighbors)} of {set1['size']} coordinates have "
                     f"at least one neighbour within {self.maxDistance} A")

        # A file of the job, not an output node (see the class docstring)
        self.outputs = {'npz': self.fixOutputPath(self.OUTPUT_NPZ)}
        self.info['summary'] = {
            'tomograms': nTomos,
            'coordinates': set1['size'],
            'max_distance': self.maxDistance,
            'with_neighbors': nNeighbors,
            'score_column': set1['scoreColumn'] if self.compareSelf else None,
        }
        self.writeRelionOutputNodes([])
        self.writeInfo()


if __name__ == '__main__':
    CoordinatesComparePipeline.main()
