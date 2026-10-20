# **************************************************************************
# *
# * Authors:     Daniel Marchan (danielmarchan3@gmail.com)
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

"""
RELION 3D class selection: keep the particles of the chosen classes of a
Class3D job (like the Subset selection job of the Relion GUI, but choosing the
classes by number). Uses relion_star_handler:

  --select rlnClassNumber --minval N --maxval N   (one run per class)
  --combine                                       (when several classes)
"""

import os
import shlex

from emtools.utils import Color
from emtools.jobs import Batch, Args, NumericList
from emtools.metadata import StarFile, Table

from .relion_base import RelionBasePipeline


class RelionSubsetSelection(RelionBasePipeline):
    """ Select the particles of some 3D classes from a Class3D job. """
    name = 'emw-relion-subset-selection'
    PREFIX = 'relion_subset_selection'
    OPTIMISER_SUFFIXES = ('_optimiser.star', '_optimisation_set.star')
    PROGRAM = 'relion_star_handler'

    def _dataStar(self):
        optimiser = self._args.get(f'{self.PREFIX}.optimiser')
        if not optimiser or not str(optimiser).strip():
            raise Exception("Input optimiser STAR file is required.")
        optimiser = str(optimiser).strip()
        suffix = next((s for s in self.OPTIMISER_SUFFIXES if optimiser.endswith(s)), None)
        if suffix is None:
            raise Exception(f"Expecting a file ending in {' or '.join(self.OPTIMISER_SUFFIXES)}, got '{optimiser}'.")

        # The particles are in the _data.star file of the same iteration
        self._iterPrefix = optimiser[:-len(suffix)]
        data = self._iterPrefix + '_data.star'
        dataAbs = data if os.path.isabs(data) else os.path.join(self.workingDir, data)
        if not os.path.exists(dataAbs):
            raise Exception(f"Particles file '{data}' does not exist (resolved: {dataAbs}).")
        return dataAbs

    def _writeOptimisationSet(self):
        """ Copy the Class3D optimisation set, replacing the particles file. """
        src = self._iterPrefix + '_optimisation_set.star'
        srcAbs = src if os.path.isabs(src) else os.path.join(self.workingDir, src)
        values = {}
        if os.path.exists(srcAbs):
            with open(srcAbs) as f:
                for line in f:
                    parts = shlex.split(line)
                    if len(parts) == 2 and parts[0].startswith('_rln'):
                        values[parts[0][1:]] = parts[1]
        else:
            self.log(f"WARNING: {src} not found, optimisation set will only have particles.")

        values['rlnTomoParticlesFile'] = self.fixOutputPath('particles.star')
        iosFn = self.join('optimisation_set.star')
        with StarFile(iosFn, 'w') as sf:
            sf.writeTable('optimisation_set', Table.fromDict(values), timeStamp=True)
        return iosFn

    def _classes(self):
        try:
            classes = sorted(set(NumericList.fromString(
                self._args.get(f'{self.PREFIX}.classes', ''))))
        except ValueError as e:
            raise Exception(f"Invalid classes: {e}")
        if not classes:
            raise Exception("Select at least one class (e.g. '1,3-4').")
        return classes

    def _run(self, batch, args):
        cmd = Args({self.PROGRAM: ''})
        cmd.update(args)
        returncode = self.batch_execute(self.PROGRAM, batch, cmd)
        if returncode or batch.error:
            raise Exception(f"{self.PROGRAM} failed. Check {self.join('run.out')}.")

    def prerun(self):
        classes = self._classes()
        batch = Batch(id=self.name, path=self.outputDir)
        dataLocal = batch.link(self._dataStar())
        self.log(f"Particles: {Color.bold(dataLocal)}")
        self.log(f"Selected classes: {Color.green(classes)}")

        output = 'particles.star'
        partial = []
        for c in classes:
            out = output if len(classes) == 1 else f'class{c:03d}.star'
            self._run(batch, Args({
                '--i': dataLocal, '--o': out,
                '--select': 'rlnClassNumber', '--minval': c, '--maxval': c
            }))
            partial.append(out)

        if len(classes) > 1:
            self._run(batch, Args({
                '--combine': '', '--i': ' '.join(partial), '--o': output
            }))
            for fn in partial:
                os.remove(batch.join(fn))

        outStar = batch.join(output)
        if not os.path.exists(outStar):
            raise Exception(f"{self.PROGRAM} did not produce output. Check {self.join('run.out')}.")

        try:
            n = len(StarFile.getTableFromFile('particles', outStar))
            self.log(f"Selected particles: {Color.green(n)}")
        except Exception:
            pass

        iosFn = self._writeOptimisationSet()
        self.writeRelionOutputNodes([[iosFn, 'TomogramGroupMetadata.star.relion.tomo.particles']])
        self.updateBatchInfo(batch)


if __name__ == '__main__':
    RelionSubsetSelection.main()
