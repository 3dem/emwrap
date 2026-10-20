# **************************************************************************
# *
# * Authors:     Daniel Marchan (danielmarchan3@gmail.com)
# *              J.M. de la Rosa Trevin (delarosatrevin@gmail.com)
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
Keep the particles of the chosen classes of a 3D classification (like the
Subset selection job of the Relion GUI, but choosing the classes by number).
The particles are filtered by 'rlnClassNumber' directly from the STAR file.

The input can be a particles STAR file (e.g. Class3D/job017/run_it025_data.star),
producing only the filtered particles.star, or an optimisation_set.star, also
producing a new optimisation_set.star pointing to the filtered particles.
"""

import os
from collections import Counter

from emtools.utils import Color
from emtools.jobs import NumericList
from emtools.metadata import StarFile, Table, RelionStar

from emwrap.base import ProcessingPipeline


OUTPUT_NODE_LABELS = {
    'TomoParticles': 'TomogramGroupMetadata.star.relion.tomo.particles',
    'Particles': 'ParticleGroupMetadata.star.relion',
}


class SubsetClassesPipeline(ProcessingPipeline):
    """ Select the particles assigned to some 3D classes. """
    name = 'emw-subset-classes'

    def __init__(self, args, output):
        ProcessingPipeline.__init__(self, args, output)
        self.inputSet = (args.get('input_set') or '').strip()
        self.classes = self._parseClasses(args.get('classes'))

    @staticmethod
    def _parseClasses(value):
        """ Parse the class numbers to keep (e.g. '1,3-4' -> [1, 3, 4]). """
        try:
            classes = sorted(set(NumericList.fromString(str(value or ''))))
        except ValueError as e:
            raise Exception(f"Invalid 'classes' value: {e}")
        if not classes:
            raise Exception("Select at least one class (e.g. '1,3-4').")
        return classes

    def _absPath(self, path):
        return path if os.path.isabs(path) else os.path.join(self.workingDir, path)

    def _writeParticles(self, inputStar, outputStar):
        """ Copy all tables from 'inputStar', keeping only the rows of the
        'particles' table that belong to the selected classes. """
        tables = StarFile.getTablesDict(inputStar)
        particles = tables.get('particles')
        if not particles:
            raise Exception(f"Could not read 'particles' table from {inputStar}")
        if not particles.hasColumn('rlnClassNumber'):
            raise Exception(f"No 'rlnClassNumber' column in {inputStar}")

        selected = set(self.classes)
        counts = Counter()
        filtered = Table(particles.getColumnNames())
        for row in particles:
            c = int(row.rlnClassNumber)
            counts[c] += 1
            if c in selected:
                filtered.addRow(row)

        for c in self.classes:
            self.log(f"Class {Color.bold(c)}: {Color.green(counts[c])} particles")
        if missing := sorted(selected - set(counts)):
            self.log(Color.warn(
                f"WARNING: no particles assigned to class(es) {missing}. "
                f"Available classes: {sorted(counts)}"))
        if not len(filtered):
            raise Exception("No particles left for the selected classes.")

        self.log(f"Writing {Color.green(len(filtered))} / "
                 f"{Color.bold(len(particles))} particles to {Color.cyan(outputStar)}")
        with StarFile(outputStar, 'w') as sfOut:
            sfOut.writeTimeStamp()
            for name, table in tables.items():
                if name == 'particles':
                    sfOut.writeTable(name, filtered)
                else:
                    sfOut.writeTable(name, table, singleRow=(name == 'general'))
        return len(filtered)

    def _writeOptimisationSet(self, optValues, outputStar):
        """ Copy the optimisation set, replacing the particles file. """
        optValues = dict(optValues)
        optValues['rlnTomoParticlesFile'] = self.fixOutputPath('particles.star')
        with StarFile(outputStar, 'w') as sfOut:
            sfOut.writeTable('optimisation_set', Table.fromDict(optValues),
                             computeFormat='left', timeStamp=True)

    def prerun(self):
        if not self.inputSet:
            raise Exception("Missing parameter 'input_set'.")
        inputStar = self._absPath(self.inputSet)
        if not os.path.exists(inputStar):
            raise Exception(f"Input STAR file not found: {inputStar}")

        optValues = None
        if RelionStar.isTomoOptimisationSet(inputStar):
            optValues = RelionStar.readTomoOptimisationSet(inputStar)[0]._asdict()
            dataStar = self._absPath(optValues['rlnTomoParticlesFile'])
            if not os.path.exists(dataStar):
                raise Exception(f"Particles file '{dataStar}' from the "
                                f"optimisation set does not exist.")
        else:
            dataStar = inputStar

        self.log(f"Particles: {Color.bold(dataStar)}")
        self.log(f"Selected classes: {Color.green(self.classes)}")

        outParticles = self.join('particles.star')
        count = self._writeParticles(dataStar, outParticles)

        if optValues:
            outputType = 'TomoParticles'
            outputStar = self.join('optimisation_set.star')
            self._writeOptimisationSet(optValues, outputStar)
        else:
            outputType = 'Particles'
            outputStar = outParticles

        self.writeRelionOutputNodes([[outputStar, OUTPUT_NODE_LABELS[outputType]]])

        self.inputs = {
            'input_set': self.inputSet,
            'classes': self.classes
        }
        self.outputs = {
            'type': outputType,
            'count': count
        }
        self.writeInfo()

        self.log(f"Created {outputType} subset with {Color.green(count)} particle(s).")


if __name__ == '__main__':
    SubsetClassesPipeline.main()
