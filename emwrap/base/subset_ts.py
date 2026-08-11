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

from emtools.utils import Color
from emtools.metadata import StarFile, Table, RelionStar

from emwrap.base import ProcessingPipeline


OUTPUT_NODE_LABELS = {
    'TiltSeriesMovies': 'TomogramGroupMetadata.star.emwrap.frameseries',
    'TiltSeries': 'TomogramGroupMetadata.star.emwrap.TiltSeries',
    'TiltSeriesAligned': 'TomogramGroupMetadata.star.emwrap.TiltSeriesAligned',
    'Tomograms': 'TomogramGroupMetadata.star.relion.tomo.Tomograms',
    'TomoParticles': 'TomogramGroupMetadata.star.relion.tomo.particles',
    'TomoCoordinates': 'TomogramGroupMetadata.star.emwrap.TomoCoordinates',
}


class SubsetTsPipeline(ProcessingPipeline):
    name = 'emw-subset-ts'

    def __init__(self, args, output):
        ProcessingPipeline.__init__(self, args, output)
        self.inputSet = args['input_set']
        self.subsetNames = set((args.get('subset_tomo_names') or '').split())

    def _writeFilteredGlobalTable(self, inputStar, outputStar, subsetNames):
        inputTable = StarFile.getTableFromFile('global', inputStar)
        if not inputTable:
            raise Exception(f"Could not read 'global' table from {inputStar}")

        filtered = Table(inputTable.getColumnNames())
        for row in inputTable:
            if row.rlnTomoName in subsetNames:
                filtered.addRow(row)

        if not len(filtered):
            raise Exception(
                f"No tomograms from {inputStar} matched the requested subset."
            )

        self.log(f"Writing {Color.green(len(filtered))} / "
                 f"{Color.bold(len(inputTable))} tomograms to {Color.cyan(outputStar)}")
        with StarFile(outputStar, 'w') as sfOut:
            sfOut.writeTable('global', filtered, computeFormat='left', timeStamp=True)
        return filtered

    def _writeFilteredParticlesTable(self, inputStar, outputStar, subsetNames):
        particlesTable = RelionStar.readTomoParticles(inputStar)
        filtered = Table(particlesTable.getColumnNames())
        for row in particlesTable:
            if row.rlnTomoName in subsetNames:
                filtered.addRow(row)

        if not len(filtered):
            raise Exception(
                f"No particles from {inputStar} matched the requested subset."
            )

        self.log(f"Writing {Color.green(len(filtered))} / "
                 f"{Color.bold(len(particlesTable))} particles to {Color.cyan(outputStar)}")
        with StarFile(outputStar, 'w') as sfOut:
            sfOut.writeTable('particles', filtered, computeFormat='left', timeStamp=True)
        return filtered

    def _detectGlobalTableType(self, starPath, globalTable):
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

    def _outputStarName(self, inputType):
        if inputType == 'Tomograms':
            return 'tomograms.star'
        return 'tilt_series.star'

    def _subsetGlobalInput(self, inputStar, subsetNames):
        inputTable = StarFile.getTableFromFile('global', inputStar)
        if not inputTable:
            raise Exception(f"Could not read 'global' table from {inputStar}")

        inputType = self._detectGlobalTableType(inputStar, inputTable)
        outputStar = self.join(self._outputStarName(inputType))
        filtered = self._writeFilteredGlobalTable(inputStar, outputStar, subsetNames)

        outputNode = OUTPUT_NODE_LABELS[inputType]
        self.writeRelionOutputNodes([[outputStar, outputNode]])
        return inputType, len(filtered)

    def _subsetOptimisationSet(self, inputStar, subsetNames):
        optRow = RelionStar.readTomoOptimisationSet(inputStar)[0]
        optValues = optRow._asdict()

        tomoStar = optValues.get('rlnTomoTomogramsFile', '')
        ptsStar = optValues.get('rlnTomoParticlesFile', '')
        if not tomoStar:
            raise Exception(
                f"Missing rlnTomoTomogramsFile in optimisation_set STAR file: {inputStar}"
            )
        if not ptsStar:
            raise Exception(
                f"Missing rlnTomoParticlesFile in optimisation_set STAR file: {inputStar}"
            )

        outTomoStar = self.join('tomograms.star')
        outParticlesStar = self.join('particles.star')
        outOptimisationStar = self.join('optimisation_set.star')

        self._writeFilteredGlobalTable(tomoStar, outTomoStar, subsetNames)
        ptsTable = self._writeFilteredParticlesTable(
            ptsStar, outParticlesStar, subsetNames)

        if ptsTable.hasColumn('rlnTomoParticleId'):
            inputType = 'TomoParticles'
        else:
            inputType = 'TomoCoordinates'

        optValues['rlnTomoTomogramsFile'] = self.fixOutputPath('tomograms.star')
        optValues['rlnTomoParticlesFile'] = self.fixOutputPath('particles.star')
        with StarFile(outOptimisationStar, 'w') as sfOut:
            sfOut.writeTable('optimisation_set', Table.fromDict(optValues),
                             computeFormat='left', timeStamp=True)

        outputNode = OUTPUT_NODE_LABELS[inputType]
        self.writeRelionOutputNodes([[outOptimisationStar, outputNode]])
        return inputType, len(ptsTable)

    def prerun(self):
        if not self.subsetNames:
            raise Exception(
                "Missing or empty parameter 'subset_tomo_names'. "
                "Provide a space-separated list of rlnTomoName values."
            )

        if not os.path.exists(self.inputSet):
            raise Exception(f"Input STAR file not found: {self.inputSet}")

        inputStar = self.inputSet

        self.log(f"Input set: {Color.bold(self.inputSet)}")
        self.log(f"Subset tomogram names ({Color.green(len(self.subsetNames))}): "
                 f"{Color.cyan(' '.join(sorted(self.subsetNames)))}")

        if RelionStar.isTomoOptimisationSet(inputStar):
            inputType, count = self._subsetOptimisationSet(inputStar, self.subsetNames)
        else:
            inputType, count = self._subsetGlobalInput(inputStar, self.subsetNames)

        self.inputs = {'input_set': self.inputSet,
                       'subset_tomo_names': sorted(self.subsetNames)}
        self.outputs = {'type': inputType, 'count': count}
        self.writeInfo()
        self.log(f"Created {inputType} subset with {Color.green(count)} item(s).")


if __name__ == '__main__':
    SubsetTsPipeline.main()
