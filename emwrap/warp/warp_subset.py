# **************************************************************************
# *
# * Authors:     Daniel Marchan Torres (danielmarchan3@gmail.com)
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

from emtools.utils import Color
from emtools.jobs import Batch
from emtools.metadata import StarFile, RelionStar

from .warp import WarpBasePipeline


class WarpSubsetTs(WarpBasePipeline):
    """ Synchronize the Warp state found next to an input tilt-series or
    tomograms STAR file with a subset of tomograms (and excluded tilts).

    It is not a standalone job: it is instantiated by a subset job with the
    same args and output folder, and applied through ``apply``.
    """
    name = 'emw-warp-subset-ts'

    def __init__(self, args, output):
        WarpBasePipeline.__init__(self, args, output)
        self.inputSet = self._args['input_set']
        self.inputFolder = os.path.dirname(self.inputSet)
        self.stateKeys = set()
        self.hasFrames = False
        self.hasTilts = False

    def _get_acquisition_input_star(self):
        return self._args.get('input_set')

    @classmethod
    def hasState(cls, inputSet):
        """ Return True if Warp state is found next to ``inputSet``. """
        return bool(cls.detectState(os.path.dirname(inputSet)))

    def apply(self, subsetNames, excludedTiltsMap=None):
        """ Import the Warp state into this job folder and deselect the
        frame-series and tilt-series items that are not part of the subset.

        Must be called with the ORIGINAL input metadata, before any
        per-tomogram tilt-series STAR paths are rewritten.
        """
        excludedTiltsMap = excludedTiltsMap or {}

        # Warp population/m/ workflows require separate handling.
        if RelionStar.isTomoOptimisationSet(self.inputSet):
            raise Exception(
                "Warp state was detected next to the input optimisation "
                "set, but Warp population/m/ workflows are not currently "
                "supported by this subset job."
            )

        self._importState(subsetNames)
        plan = self._buildSelectionPlan(subsetNames, excludedTiltsMap)
        self._applySelectionPlan(plan)

    def _importState(self, subsetNames):
        """ Import mutable Warp state detected next to the input STAR. """
        sourceKeys = self.detectState(self.inputFolder)
        self.hasFrames, self.hasTilts = self.validateState(sourceKeys)

        self.log(
            f"Detected Warp state in previous job folder: "
            f"{Color.cyan(self.inputFolder)}", flush=True
        )

        copyKeys = set(sourceKeys)

        # warp_tomostar controls which tilt series WarpTools discovers for
        # commands such as ts_ctf and ts_reconstruct. Do not copy/link the full
        # upstream folder; create a private filtered folder instead.
        hasTomostarState = 'tm' in copyKeys
        copyKeys.discard('tm')

        if copyKeys:
            self._importInputs(self.inputFolder, keys=copyKeys, mutable=True)

        if hasTomostarState:
            self.copyTomostars(self.inputFolder, self, subsetNames)

        self.stateKeys = set(sourceKeys)

        self.log(
            "Imported Warp state: "
            + Color.cyan(', '.join(sorted(self.stateKeys)))
        )

    def _buildSelectionPlan(self, subsetNames, excludedTiltsMap):
        """ Build frame-series and tilt-series deselections for Warp. """
        globalTable = StarFile.getTableFromFile('global', self.inputSet)
        if not globalTable:
            raise Exception(
                f"Could not read 'global' table from {self.inputSet}")

        allNames = {row.rlnTomoName for row in globalTable}
        removedNames = allNames - set(subsetNames)
        plan = {'frames': set(), 'tiltseries': set()}

        if excludedTiltsMap and self.hasTilts:
            raise Exception(
                "Individual tilt exclusions cannot currently be synchronized "
                "with an existing Warp tilt-series state. WarpTools "
                "change_selection supports frame-series items and complete "
                "tilt-series items, but not an individual tilt inside an "
                "already-created warp_tiltseries state. Use exclude_tilts "
                "before the Warp tilt-series state is created."
            )

        if self.hasTilts:
            plan['tiltseries'].update(
                self.tiltSeriesSelectionPath(name) for name in removedNames
            )

        if not self.hasFrames or not (removedNames or excludedTiltsMap):
            return plan

        if not globalTable.hasColumn('rlnTomoTiltSeriesStarFile'):
            raise Exception(
                f"Warp frame-series synchronization requires "
                f"'rlnTomoTiltSeriesStarFile', but {self.inputSet} "
                "does not contain that column."
            )

        for tomoRow in globalTable:
            tomoName = tomoRow.rlnTomoName
            removeWholeTomo = tomoName in removedNames
            excludedIds = excludedTiltsMap.get(tomoName, set())

            if not removeWholeTomo and not excludedIds:
                continue

            tsStar = tomoRow.rlnTomoTiltSeriesStarFile
            tsTable = StarFile.getTableFromFile(
                tomoName, tsStar, guessType=False
            )
            if not tsTable:
                raise Exception(
                    f"Could not read tilt-series table '{tomoName}' "
                    f"from {tsStar} while preparing Warp subset."
                )

            if not tsTable.hasColumn('rlnMicrographMovieName'):
                raise Exception(
                    f"Tilt-series STAR file {tsStar} for '{tomoName}' has no "
                    "'rlnMicrographMovieName' column."
                )

            if excludedIds and not tsTable.hasColumn('rlnTomoTiltMovieIndex'):
                raise Exception(
                    f"Tilt-series STAR file {tsStar} for '{tomoName}' has no "
                    "'rlnTomoTiltMovieIndex' column."
                )

            for tiltRow in tsTable:
                deselect = removeWholeTomo
                if excludedIds:
                    tiltId = int(tiltRow.rlnTomoTiltMovieIndex)
                    deselect = deselect or tiltId in excludedIds

                if deselect:
                    plan['frames'].add(
                        self.frameSelectionPath(tiltRow.rlnMicrographMovieName)
                    )

        return plan

    def _writeSelectionList(self, fileName, paths):
        """ Write a WarpTools --input_data text file and return its path. """
        with open(self.join(fileName), 'w') as fh:
            for path in sorted(set(paths)):
                fh.write(str(path) + '\n')
        return fileName

    def _applySelectionPlan(self, plan):
        """ Apply planned Warp deselections to this job's private state. """
        jobs = (
            ('frames', self.FSS,
             'warp_deselect_frames.txt', 'frame-series'),
            ('tiltseries', self.TSS,
             'warp_deselect_tiltseries.txt', 'tilt-series'),
        )

        if not any(plan.values()):
            self.log(
                "Warp state preserved; no additional Warp items need "
                "to be deselected."
            )
            return

        batch = Batch(id=self.name, path=self.path)

        for key, settings, fileName, label in jobs:
            paths = plan[key]
            if not paths:
                continue

            inputList = self._writeSelectionList(fileName, paths)
            self.log(
                f"Deselecting {Color.red(len(paths))} Warp {label} item(s)."
            )

            args = self.changeSelectionArgs(settings, inputList, deselect=True)
            self.batch_execute(f'change_selection_{key}', batch, args)
