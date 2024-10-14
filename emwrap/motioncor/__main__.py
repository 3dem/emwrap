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

"""

"""

import os
import subprocess
import shutil
import sys
import json
import argparse
from pprint import pprint

from emtools.utils import Color, Timer, Path
from emtools.jobs import ProcessingPipeline, BatchManager
from emtools.metadata import Table, Column, StarFile, StarMonitor


class McPipeline(ProcessingPipeline):
    """ Pipeline specific to Motioncor processing. """
    def __init__(self, args):
        ProcessingPipeline.__init__(self, os.getcwd(), args['output_dir'])

        self.program = args.get('motioncor_path',
                                os.environ.get('MOTIONCOR_PATH', None))
        self.extraArgs = args.get('motioncor_args', '')
        self.gpuList = args['gpu_list'].split()
        self.outputMicDir = self.join('Micrographs')
        self.inputStar = args['input_star']
        self.batchSize = args.get('batch_size', 32)
        self.optics = None
        self._outputPrefix = "output/aligned_"
        self._outputDict = {
            'rlnCtfPowerSpectrum': '_Ctf.mrc',
            'rlnMicrographName': '.mrc',
            'rlnMicrographMetadata': '_Ctf.txt',
            'rlnOpticsGroup': None
        }

    def _build(self):
        def _movie_filename(row):
            return row.rlnMicrographMovieName

        monitor = StarMonitor(self.inputStar, 'movies',
                              _movie_filename, timeout=30)

        batchMgr = BatchManager(self.batchSize, monitor.newItems(), self.outputDir,
                                itemFileNameFunc=_movie_filename)
        #

        g = self.addGenerator(batchMgr.generate)
        outputQueue = None
        print(f"Creating {len(self.gpuList)} processing threads.")
        for gpu in self.gpuList:
            p = self.addProcessor(g.outputQueue,
                                  self.get_motioncor_proc(gpu),
                                  outputQueue=outputQueue)
            outputQueue = p.outputQueue

        self.addProcessor(outputQueue, self._output)

    def motioncor(self, gpu, batch):
        batch_dir = batch['path']

        def _path(p):
            return os.path.join(batch_dir, p)

        os.mkdir(_path('output'))
        os.mkdir(_path('log'))

        logFn = _path('motioncor_log.txt')
        args = [self.program]

        ext = Path.getExt(batch['items'][0].rlnMicrographMovieName)
        extLower = ext.lower()

        if extLower.startswith('.tif'):
            inArg = '-InTiff'
        elif extLower.startswith('.mrc'):
            inArg = '-InMrc'
        elif extLower.startswith('.eer'):
            inArg = '-InEer'
        else:
            raise Exception(f"Unsupported movie format: {ext}")

        # Load acquisition parameters from the optics table
        acq = self.optics[0]
        ps = acq.rlnMicrographOriginalPixelSize
        voltage = acq.rlnVoltage
        cs = acq.rlnSphericalAberration

        opts = f"{inArg} ./ -OutMrc {self._outputPrefix} -InSuffix {ext} "
        opts += f"-Serial 1  -Gpu {gpu} -LogDir log/ "
        opts += f"-PixSize {ps} -kV {voltage} -Cs {cs} "
        opts += self.extraArgs
        args.extend(opts.split())
        batchStr = Color.cyan("BATCH_%02d" % batch['index'])
        t = Timer()

        print(f">>> {batchStr}: Running {Color.green(self.program)} {Color.bold(opts)}")

        with open(logFn, 'w') as logFile:
            subprocess.call(args, cwd=batch_dir, stderr=logFile, stdout=logFile)

        print(f">>> {batchStr}: Done! Elapsed: {t.getToc()}. "
              f"Log file: {Color.bold(logFn)}")

        return batch

    def get_motioncor_proc(self, gpu):
        def _motioncor(batch):
            return self.motioncor(gpu, batch)

        return _motioncor

    def _output(self, batch):
        """
        Movies/20170629_00021_frameImage.tiff

        56956944 Sep 23 11:08 aligned_20170629_00021_frameImage.mrc
         1049600 Sep 23 11:08 aligned_20170629_00022_frameImage_Ctf.mrc
             293 Sep 23 11:08 aligned_20170629_00022_frameImage_Ctf.txt

======================== movies.star =============================================
# version 50001

data_optics

loop_
_rlnOpticsGroupName #1
_rlnOpticsGroup #2
_rlnMtfFileName #3
_rlnMicrographOriginalPixelSize #4
_rlnVoltage #5
_rlnSphericalAberration #6
_rlnAmplitudeContrast #7
opticsGroup1            1 mtf_k2_200kV.star     0.885000   200.000000     1.400000     0.100000


# version 50001

data_movies

loop_
_rlnMicrographMovieName #1
_rlnOpticsGroup #2
Movies/20170629_00021_frameImage.tiff            1
Movies/20170629_00022_frameImage.tiff            1
Movies/20170629_00023_frameImage.tiff            1
Movies/20170629_00024_frameImage.tiff            1



======================== corrected_micrographs.star ============================

# version 50001

data_optics

loop_
_rlnOpticsGroupName #1
_rlnOpticsGroup #2
_rlnMtfFileName #3
_rlnMicrographOriginalPixelSize #4
_rlnVoltage #5
_rlnSphericalAberration #6
_rlnAmplitudeContrast #7
_rlnMicrographPixelSize #8
opticsGroup1            1 mtf_k2_200kV.star     0.885000   200.000000     1.400000     0.100000     0.885000


# version 50001

data_micrographs

loop_
_rlnCtfPowerSpectrum #1
_rlnMicrographName #2
_rlnMicrographMetadata #3
_rlnOpticsGroup #4
_rlnAccumMotionTotal #5
_rlnAccumMotionEarly #6
_rlnAccumMotionLate #7
MotionCorr/job002/Movies/20170629_00021_frameImage_PS.mrc MotionCorr/job002/Movies/20170629_00021_frameImage.mrc MotionCorr/job002/Movies/20170629_00021_frameImage.star            1    16.429035     2.504605    13.924432
MotionCorr/job002/Movies/20170629_00022_frameImage_PS.mrc MotionCorr/job002/Movies/20170629_00022_frameImage.mrc MotionCorr/job002/Movies/20170629_00022_frameImage.star            1    19.556374     2.480002    17.076372
MotionCorr/job002/Movies/20170629_00023_frameImage_PS.mrc MotionCorr/job002/Movies/20170629_00023_frameImage.mrc MotionCorr/job002/Movies/20170629_00023_frameImage.star            1    17.542337     1.940450    15.601886
MotionCorr/job002/Movies/20170629_00024_frameImage_PS.mrc MotionCorr/job002/Movies/20170629_00024_frameImage.mrc MotionCorr/job002/Movies/20170629_00024_frameImage.star            1    18.102179     1.725132    16.377047
MotionCorr/job002/Movies/20170629_00025_frameImage_PS.mrc MotionCorr/job002/Movies/20170629_00025_frameImage.mrc MotionCorr/job002/Movies/20170629_00025_frameImage.star            1    24.124382     3.573240    20.551142
MotionCorr/job002/Movies/20170629_00026_frameImage_PS.mrc MotionCorr/job002/Movies/20170629_00026_frameImage.mrc MotionCorr/job002/Movies/20170629_00026_frameImage.star            1    13.147140     1.832367    11.314774
MotionCorr/job002/Movies/20170629_00027_frameImage_PS.mrc MotionCorr/job002/Movies/20170629_00027_frameImage.mrc MotionCorr/job002/Movies/20170629_00027_frameImage.star            1    15.070049     1.434088    13.635961
MotionCorr/job002/Movies/20170629_00028_frameImage_PS.mrc MotionCorr/job002/Movies/20170629_00028_frameImage.mrc MotionCorr/job002/Movies/20170629_00028_frameImage.star            1    13.784786     1.098232    12.686556

        """
        batch_dir = batch['path']
        def _path(p):
            return os.path.join(batch_dir, p)

        def _move(p):
            pp = _path(p)
            if os.path.exists(pp):
                return self.relpath(shutil.move(pp, self.outputMicDir))
            else:
                return 'None'

        for item in batch['items']:
            base = Path.removeBaseExt(item.rlnMicrographMovieName)
            self._outSf.writeRowValues([
                _move(f"{self._outputPrefix}{base}{'_Ctf.mrc'}"),
                _move(f"{self._outputPrefix}{base}{'.mrc'}"),
                item.rlnOpticsGroup])

            # for newExt in zip(self._outputExts, ):
            #     newFile = _path(f"{self._outputPrefix}{base}{newExt}")
            #     shutil.move(newFile, self.outputMicDir)

        # FIXME: Check what we want to move to output
        #os.system(f'mv {batch_dir}/* {self.outputDir}/ && rm -rf {batch_dir}')
        return batch

    def _writeMicrographsTableHeader(self):
        self.micTable = Table(columns=list(self._outputDict.keys()))
        self._outSf.writeHeader('micrographs', self.micTable)
        
    def run(self):
        with StarFile(self.inputStar) as sf:
            self.optics = sf.getTable('optics')

        # Create a new optics table adding pixelSize
        cols = list(self.optics.getColumns())
        cols.append(Column('rlnMicrographPixelSize', type=float))
        self.newOptics = Table(columns=cols)

        for row in self.optics:
            d = row._asdict()
            d['rlnMicrographPixelSize'] = row.rlnMicrographOriginalPixelSize  #FIXME incorrect with binning
            self.newOptics.addRowValues(**d)

        if not os.path.exists(self.outputMicDir):
            os.mkdir(self.outputMicDir)

        self._build()
        print(f"Batch size: {self.batchSize}")

        self._outFile = open(self.join('corrected_micrographs.star'), 'w')  #FIXME improve for continue
        self._outSf = StarFile(self._outFile)
        self._outSf.writeTable('optics', self.newOptics)


        ProcessingPipeline.run(self)

        self._outSf.close()

#
#
# def motioncor():
#     argsDict = {
#         '-Throw': 0 if self.isEER else (frame0 - 1),
#         '-Trunc': 0 if self.isEER else (numbOfFrames - frameN),
#         '-Patch': f"{self.patchX} {self.patchY}",
#         '-MaskCent': f"{self.cropOffsetX} {self.cropOffsetY}",
#         '-MaskSize': f"{cropDimX} {cropDimY}",
#         '-FtBin': self.binFactor.get(),
#         '-Tol': self.tol.get(),
#         '-PixSize': inputMovies.getSamplingRate(),
#         '-kV': inputMovies.getAcquisition().getVoltage(),
#         '-Cs': 0,
#         '-OutStack': 1 if self.doSaveMovie else 0,
#         '-Gpu': '%(GPU)s',
#         '-SumRange': "0.0 0.0",  # switch off writing out DWS,
#         '-LogDir': './'
#         # '-FmRef': 0
#     }


def main():
    p = argparse.ArgumentParser(prog='emw-motioncor')
    p.add_argument('--json',
                   help="Input all arguments through this JSON file. "
                        "The other arguments will be ignored. ")
    p.add_argument('--in_movies', '-i')
    p.add_argument('--output', '-o')
    p.add_argument('--motioncor_path', '-p')
    p.add_argument('--motioncor_args', '-a', default='')
    p.add_argument('--batch_size', '-b', type=int, default=8)
    p.add_argument('--j', help="Just to ignore the threads option from Relion")
    p.add_argument('--gpu')

    args = p.parse_args()

    if args.json:
        raise Exception("JSON input not yet implemented.")
    else:
        argsDict = {
            'input_star': args.in_movies,
            'output_dir': args.output,
            'motioncor_path': args.motioncor_path,
            'motioncor_args': args.motioncor_args,
            'gpu_list': args.gpu,
            'batch_size': args.batch_size
        }
        mc = McPipeline(argsDict)
        mc.run()


if __name__ == '__main__':
    main()
