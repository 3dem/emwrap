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
    return duplicates_near(np.asarray(matrix) <= distance, scores)


def duplicates_near(near, scores=None):
    """ duplicates() with the (n, n) bool matrix of the coordinates within
    the distance of each other. """
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



def score_column(table):
    """ The first of SCORE_COLUMNS in the particles table, None if none. """
    return next((c for c in SCORE_COLUMNS if table.hasColumn(c)), None)


def column(table, name, default=0.0):
    """ Column values as a float64 array (default if missing). """
    if table.hasColumn(name):
        return np.array(table.getColumnValues(name), dtype=np.float64)
    return np.full(len(table), default, dtype=np.float64)


def tomo_positions(table, tomograms=None, scoreColumn=None):
    """ The positions (Angstrom, from the tomogram center, with the shifts)
    of the coordinates of a particles table, by tomogram:
        {tomoName: (indexes (1-based rows), Nx3 float64 array, scores or None)}

        pos = rlnCenteredCoordinate(X/Y/Z)Angst - rlnOrigin(X/Y/Z)Angst

    tomograms (tomo_geometry) is needed for pixel coordinates
    (rlnCoordinateX/Y/Z, in the reconstructed tomogram): times its pixel
    size, minus half of its size. """
    tomoNames = table.getColumnValues('rlnTomoName')
    origin = np.stack([column(table, f'rlnOrigin{a}Angst') for a in 'XYZ'], axis=1)

    if table.hasAllColumns(RelionStar.TOMO_PARTICLES_CENTERED_COORD_COLUMNS):
        coords = np.stack([column(table, c)
                           for c in RelionStar.TOMO_PARTICLES_CENTERED_COORD_COLUMNS],
                          axis=1)
    else:
        if not tomograms:
            raise Exception("Pixel coordinates (rlnCoordinateX/Y/Z) need the "
                            "tomogram pixel sizes (a tomograms.star file).")
        if missing := set(tomoNames) - set(tomograms):
            raise Exception("Tomograms without a pixel size: "
                            f"{', '.join(sorted(missing)[:10])}")
        coords = np.stack([column(table, c)
                           for c in RelionStar.TOMO_PARTICLES_PIXEL_COORD_COLUMNS],
                          axis=1)
        coords *= np.array([tomograms[t]['pixel_size'] for t in tomoNames])[:, None]
        coords -= np.array([tomograms[t]['size'] for t in tomoNames]) / 2

    coords -= origin
    scores = column(table, scoreColumn) if scoreColumn else None

    groups = OrderedDict()
    for i, tomoName in enumerate(tomoNames):
        groups.setdefault(tomoName, []).append(i)

    return OrderedDict(
        (tomoName, (np.array(rows) + 1, coords[rows],
                    None if scores is None else scores[rows]))
        for tomoName, rows in groups.items())


def tomo_geometry(tomoStar):
    """ {tomoName: {'pixel_size', 'size'}} of a tomograms.star file: the
    pixel size of the reconstructed tomogram and its size (X, Y, Z) in
    Angstrom (rlnTomoSize* are in tilt series pixels), None if unknown. """
    geometry = {}
    for row in StarFile.getTableFromFile('global', tomoStar):
        size = None
        if all(hasattr(row, f'rlnTomoSize{a}') for a in 'XYZ'):
            size = (np.array([float(getattr(row, f'rlnTomoSize{a}')) for a in 'XYZ'])
                    * float(getattr(row, 'rlnTomoTiltSeriesPixelSize', 0)))
        geometry[row.rlnTomoName] = {'pixel_size': RelionStar.getTomoPixelSize(row),
                                     'size': size}
    return geometry


def edge_distances(pos, size):
    """ Distance (Angstrom) of each position (from the tomogram center) to
    the nearest edge of the tomogram box (size: X, Y, Z in Angstrom),
    negative outside of it. """
    return (np.asarray(size) / 2 - np.abs(pos)).min(axis=1) if len(pos) else np.zeros(0)


def near_edge(edges, edge):
    """ The coordinates closer than edge (Angstrom) to the tomogram box
    edges (edges: edge_distances), none without an edge (None or 0). """
    edges = np.asarray(edges)
    return edges < edge if edge else np.zeros(len(edges), bool)


def distance_blocks(pos1, pos2):
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


def distance_matrix(pos1, pos2):
    """ float32 (n1, n2) distances (Angstrom) of a tomogram. """
    matrix = np.empty((len(pos1), len(pos2)), dtype=np.float32)
    for start, d in distance_blocks(pos1, pos2):
        matrix[start:start + len(d)] = d
    return matrix


def within_distance(pos1, pos2, distance):
    """ (n1, n2) bool matrix of the coordinates of pos1 within the distance
    of those of pos2 (no distance matrix kept in memory). """
    near = np.empty((len(pos1), len(pos2)), dtype=bool)
    for start, d in distance_blocks(pos1, pos2):
        near[start:start + len(d)] = d <= distance
    return near


def tomo_edges(positions, tomograms, edge):
    """ {tomoName: near_edge mask} of tomo_positions, with the tomogram
    sizes of tomograms (tomo_geometry); None without an edge. """
    if not edge:
        return None
    edges = {}
    for tomo, (_, pos, _) in positions.items():
        size = (tomograms or {}).get(tomo, {}).get('size')
        if size is None:
            raise Exception(f"No size of tomogram {tomo} (rlnTomoSizeX/Y/Z in "
                            "its tomograms.star) to know its edges.")
        edges[tomo] = near_edge(edge_distances(pos, size), edge)
    return edges


def remove_duplicates(table, distance=None, tomograms=None, edge=None):
    """ The rows (0-based) of a particles table that are not closer than
    edge (Angstrom) to the tomogram box edges, and of the others, not
    duplicates at the distance (duplicates(), with its score if it has
    one), computing the distances.  Returns (kept rows, score column or
    None, near the edges, duplicates). """
    scoreColumn = score_column(table)
    positions = tomo_positions(table, tomograms, scoreColumn)
    edges = tomo_edges(positions, tomograms, edge)
    kept, nEdge, nDup = [], 0, 0
    for tomo, (index, pos, scores) in positions.items():
        valid = ~edges[tomo] if edges else np.ones(len(pos), bool)
        index, pos = index[valid], pos[valid]
        nEdge += int((~valid).sum())
        if distance:
            dup = duplicates_near(within_distance(pos, pos, distance),
                                  None if scores is None else scores[valid])
            index = index[~dup]
            nDup += int(dup.sum())
        kept.extend(int(i) - 1 for i in index)
    return kept, scoreColumn, nEdge, nDup


def cross_sets(matrix, distance):
    """ (a_in_b, b_in_a) bool arrays of a tomogram of two sets, with the
    (nA, nB) distance matrix: the coordinates of A with one of B within the
    distance (A & B; the other ones are A - B), and those of B with one of A
    (the other ones are B - A). """
    near = np.asarray(matrix) <= distance
    if not near.size:
        return np.zeros(near.shape[0], bool), np.zeros(near.shape[1], bool)
    return near.any(axis=1), near.any(axis=0)


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
    duplicates, see duplicates()).  When the tomogram sizes are known
    (tomograms.star of an optimisation set), the distance (Angstrom) of the
    coordinates of every tomogram of set 1 and set 2 to the tomogram box
    edges ('<tomo>.edge1', '<tomo>.edge2'; only edge1 for the set compared
    to itself, see edge_distances), to leave out those near the edges.  EMhub
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
        scoreColumn = score_column(table)
        tomograms = (tomo_geometry(tomoStar)
                     if tomoStar and os.path.exists(tomoStar) else None)
        positions = self._positions(table, tomograms, label, scoreColumn)
        # Distance of each coordinate to the tomogram box edges (if the
        # tomogram sizes are known), for EMhub's 'Near edge' set
        edges = {}
        for tomo, (_, pos, _) in positions.items():
            size = (tomograms or {}).get(tomo, {}).get('size')
            if size is not None:
                edges[tomo] = edge_distances(pos, size)

        self.log(f"{label}: {Color.cyan(inputStar)}, "
                 f"{Color.green(len(table))} coordinates in "
                 f"{Color.green(len(positions))} tomograms, score: "
                 f"{Color.cyan(scoreColumn or 'none')}")

        return {'size': len(table), 'scoreColumn': scoreColumn,
                'positions': positions, 'edges': edges}

    def _positions(self, table, tomograms, label, scoreColumn):
        """ Return {tomoName: (indexes, Nx3 positions in Angstrom,
        scores or None)}, see tomo_positions. """
        if (not tomograms and not table.hasAllColumns(
                RelionStar.TOMO_PARTICLES_CENTERED_COORD_COLUMNS)):
            # Pixel coordinates (in the reconstructed tomogram), they need
            # the tomogram pixel size to be in Angstrom
            raise Exception(
                f"{label} has pixel coordinates (rlnCoordinateX/Y/Z): a "
                "tomograms.star file (from an optimisation_set.star) is "
                "needed for the tomogram pixel sizes.")
        try:
            return tomo_positions(table, tomograms, scoreColumn)
        except Exception as e:
            raise Exception(f"{label}: {e}")

    # ------------------------------------------------------------------
    # Distances
    # ------------------------------------------------------------------
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
            matrix = distance_matrix(pos1, pos2)
            nNeighbors += self._withNeighbors(matrix, scores)
            nTomos += 1
            nCompared += matrix.size
            npzArrays[tomoName] = matrix
            npzArrays[f'{tomoName}.index1'] = idx1.astype(np.int32)
            npzArrays[f'{tomoName}.index2'] = idx2.astype(np.int32)
            if scores is not None:
                npzArrays[f'{tomoName}.score'] = scores.astype(np.float32)
        # Edge distances of every tomogram of each set (also those not in
        # the other set)
        for key, s in (('edge1', set1), ('edge2', set2)):
            if key == 'edge2' and self.compareSelf:
                break
            for tomoName, edges in s['edges'].items():
                npzArrays[f'{tomoName}.{key}'] = edges.astype(np.float32)
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
