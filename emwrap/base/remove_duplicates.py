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

from emtools.utils import Color
from emtools.metadata import RelionStar

from emwrap.base import ProcessingPipeline
from emwrap.base.wizards import to_float
from emwrap.base.subset_ts import SubsetTsPipeline, MODE_EXPLICIT_NAMES
from emwrap.base.coordinates_compare import duplicates


class RemoveDuplicatesPipeline(SubsetTsPipeline):
    """ Remove the duplicate coordinates (or particles) of a set, from the
    distances of an emw-coordinates-compare job that compared the set to
    itself (its distances.npz, all the distances), at any distance (the
    job's maximum one by default).

    The duplicates are the ones of coordinates_compare.duplicates(): of the
    coordinates within the distance of each other, the one with the highest
    score is kept (the score the compare job stored, see its SCORE_COLUMNS),
    or the last one (as Relion's 'remove duplicates') without scores.

    Output: the subset of the compared set without the duplicates, as
    emw-subset-ts writes it (SubsetTsPipeline._writeSubset): its
    optimisation_set.star, particles.star and tomograms.star.
    """
    name = 'emw-remove-duplicates'

    def __init__(self, args, output):
        ProcessingPipeline.__init__(self, args, output)
        self.inputDistances = (args.get('input_distances') or '').strip()
        if os.path.isdir(self.inputDistances):
            self.inputDistances = os.path.join(self.inputDistances, 'distances.npz')
        if not os.path.exists(self.inputDistances):
            raise Exception(f"Distances file not found: {self.inputDistances!r} "
                            "(distances.npz of an emw-coordinates-compare job)")
        inputs = self._compareInputs(self.inputDistances)
        if not inputs.get('compare_self'):
            raise Exception(f"{self.inputDistances} is not of a set compared to "
                            "itself: there are no duplicates to remove.")
        self.maxDistance = to_float(inputs.get('max_distance'))
        self.distance = to_float(args.get('distance')) or self.maxDistance
        if not self.distance or self.distance <= 0:
            raise Exception("'distance' must be a distance > 0 (Angstrom), "
                            f"got: {args.get('distance')!r}")

        # The subset is of the compared set (also for WarpSubsetTs, that
        # reads it from the args), with every tomogram and the particles
        # that are not duplicates (self.particleIndexes)
        self.inputSet = self._args['input_set'] = inputs['input_set']
        if not RelionStar.isTomoOptimisationSet(self.inputSet):
            raise Exception(f"The compared set ({self.inputSet}) is not an "
                            "optimisation_set.star: its subset can not be made.")
        self.mode = MODE_EXPLICIT_NAMES
        self.subsetNames = set()
        self.excludedTiltsMap = {}
        self.randomCount = self.randomSeed = self.randomParticles = None
        self.matchSet = self.excludeSet = ''
        self.particleIndexes = None

    @staticmethod
    def _compareInputs(npzFile):
        """ The inputs of the compare job (its info.json). """
        infoFile = os.path.join(os.path.dirname(npzFile), 'info.json')
        if not os.path.exists(infoFile):
            raise Exception(f"Missing {infoFile} of the emw-coordinates-compare job")
        with open(infoFile) as f:
            inputs = (json.load(f) or {}).get('inputs') or {}
        if not inputs.get('input_set'):
            raise Exception(f"No 'input_set' in {infoFile}")
        return inputs

    def _prepareDeselection(self):
        """ Every tomogram, and the particles that are not duplicates. """
        self.subsetNames = self._resolveAllTomoNames(self.inputSet)
        kept, total, withScores = set(), 0, 0
        with np.load(self.inputDistances) as npz:
            for key in npz.files:
                if '.' in key:  # <tomo>.index1 / .index2 / .score
                    continue
                index = npz[f'{key}.index1']
                scores = npz[f'{key}.score'] if f'{key}.score' in npz.files else None
                withScores += scores is not None
                dup = duplicates(npz[key], self.distance, scores)
                kept.update(int(i) - 1 for i in index[~dup])  # 0-based rows
                total += len(index)
        self.particleIndexes = kept
        self.log(f"Distance: {Color.green(self.distance)} A, removing "
                 f"{Color.warn(total - len(kept))} duplicates of {total} "
                 f"coordinates (kept the "
                 + ("highest score)" if withScores else "last one, no scores)"))
        self.inputs = {'input_distances': self.inputDistances,
                       'distance': self.distance, 'input_set': self.inputSet}
        self.info['summary'] = {'distance': self.distance, 'coordinates': total,
                                'duplicates': total - len(kept), 'kept': len(kept),
                                'by_score': bool(withScores)}


if __name__ == '__main__':
    RemoveDuplicatesPipeline.main()
