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
from emtools.metadata import Table, Column, StarFile, StarMonitor, TextFile

from .motioncor import Motioncor


# def _optics_to_acq(t):
#     r = t[0]
#     print("Optics row:", r)
#     return {
#         'pixel_size': r.rlnMicrographOriginalPixelSize,
#         'voltage': r.rlnVoltage,
#         'cs': r.rlnSphericalAberration,
#         'amplitud_constrast': r.rlnAmplitudeContrast
#     }


class McPipeline(ProcessingPipeline):
    """ Pipeline specific to Motioncor processing. """
    def __init__(self, args):
        ProcessingPipeline.__init__(self, **args)

        self.gpuList = args['gpu_list'].split()
        self.outputMicDir = self.join('Micrographs')
        self.inputStar = args['input_star']
        self.batchSize = args.get('batch_size', 32)
        self.optics = None
        with StarFile(self.inputStar) as sf:
            self.optics = sf.getTable('optics')

        self._outputDict = {
            'rlnMicrographName': '.mrc',
            'rlnCtfPowerSpectrum': '_Ctf.mrc',
            'rlnMicrographMetadata': '_Ctf.txt',
            'rlnOpticsGroup': None
        }

        o = self.optics[0]  # shortcut
        acq_args = (f" -PixSize {o.rlnMicrographOriginalPixelSize}"
                    f" -kV {o.rlnVoltage} -Cs {o.rlnSphericalAberration} ")

        mc_args = args.get('motioncor_args', '') + acq_args
        self.mc = Motioncor(mc_args, **args)

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

    def get_motioncor_proc(self, gpu):
        def _motioncor(batch):
            return self.mc.process_batch(gpu, batch)
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

        def _opath(p):
            return self.relpath(os.path.join(self.outputMicDir, p))

        def _path(p):
            return os.path.join(batch_dir, p)

        def _move(p):
            pp = _path(p)
            if os.path.exists(pp):
                return self.relpath(shutil.move(pp, self.outputMicDir))
            else:
                return 'None'


        """
# version 50001

data_general

_rlnImageSizeX                                     3710
_rlnImageSizeY                                     3838
_rlnImageSizeZ                                       24
_rlnMicrographMovieName                    Movies/20170629_00021_frameImage.tiff
_rlnMicrographGainName                     Movies/gain.mrc
_rlnMicrographBinning                          1.000000
_rlnMicrographOriginalPixelSize                0.885000
_rlnMicrographDoseRate                         1.277000
_rlnMicrographPreExposure                      0.000000
_rlnVoltage                                  200.000000
_rlnMicrographStartFrame                              1
_rlnMotionModelVersion                                1
 

# version 50001

data_global_shift

loop_ 
_rlnMicrographFrameNumber #1 
_rlnMicrographShiftX #2 
_rlnMicrographShiftY #3 
           1     0.000000     0.000000 
           2     0.963387     -0.81697 
           3     1.956197     -1.56231 
           4     2.545505     -2.11229 
           5     3.127461     -2.74669 
           6     3.711705     -3.40940 
           7     4.203554     -3.95097 
           8     4.749740     -4.73250 
           9     5.122874     -5.25944 
          10     5.564512     -5.48017 
          11     6.173563     -6.31221 
          12     6.658707     -6.61978 
          13     7.333168     -7.13502 
          14     7.748536     -7.60218 
          15     8.064422     -7.91611 
          16     8.631406     -8.56053 
          17     8.696014     -9.14832 
          18     9.040505     -9.36561 
          19     9.615537     -9.77509 
          20     9.834720    -10.07185 
          21    10.181879    -10.60601 
          22    10.589931    -10.85237 
          23    11.129778    -11.29847 
          24    11.276052    -11.53595 
        """

        for item in batch['items']:
            movName = item.rlnMicrographMovieName
            base = Path.removeBaseExt(movName)
            oBase = self.mc.outputPrefix + base
            logBase = 'log/' + base
            oMicFn = _move(f"{oBase}.mrc")
            oStarFn = _opath(Path.replaceBaseExt(oMicFn, ".star"))
            with StarFile(oStarFn, 'w') as sf:
                self._writeMicGeneralTable(sf,  # FIXME update other values
                                           rlnMicrographMovieName=movName)
                self._writeMicGlobal(sf, _path(f"{logBase}-Patch-Full.log"),
                                     _path(f"{logBase}-Patch-Frame.log"))
            values = [
                oMicFn,
                oStarFn,
                1,  # fixme: optics group
                -999.0, -999.0, -999.0  # fixme motion
            ]
            # External/job006/241015-115244_01_20b2a132/log/20170629_00023_frameImage-Patch-Full.log
            if self.mc.version >= 3:
                values.append(_move(f"{oBase}_Ctf.mrc"))
                ctfTxt = _path(f"{oBase}_Ctf.txt")
                # It should be only one line in the CTF txt file
                for line in TextFile.stripLines(ctfTxt):
                    ctfValues = [float(v) for v in line.split()]
                del ctfValues[3]  # remove Phase shift
                ctfValues.insert(2, abs(ctfValues[0] - ctfValues[1]))  # Add astigmatism
                """
                # defocus 1 [A]; #3 - defocus 2; #4 - azimuth of astig; #5 - additional phase shift [radian]; #6 - cross correlation; #7 - spacing (in Angstroms) up to which CTF rings were fit successfully
   9174.97    8825.42    77.98     0.00    0.24444   10.0000
                """
                values.extend(ctfValues)
            self._outSf.writeRowValues(values)

        return batch

    def _writeMicrographsTableHeader(self):
        columns = [
            'rlnMicrographName',
            'rlnMicrographMetadata',
            'rlnOpticsGroup',
            'rlnAccumMotionTotal',
            'rlnAccumMotionEarly',
            'rlnAccumMotionLate'
        ]
        if self.mc.version >= 3:
            columns.extend([
                'rlnCtfImage',
                'rlnDefocusU',
                'rlnDefocusV',
                'rlnCtfAstigmatism',
                'rlnDefocusAngle',
                'rlnCtfFigureOfMerit',
                'rlnCtfMaxResolution'])

        self.micTable = Table(columns=columns)
        self._outSf.writeHeader('micrographs', self.micTable)
        self._outSf.flush()

    def _writeMicGeneralTable(self, sf, **kwargs):
        dRow = {
            'rlnImageSizeX': 3710,
            'rlnImageSizeY': 3838,
            'rlnImageSizeZ': 24,
            'rlnMicrographMovieName': '',
            'rlnMicrographGainName': 'Movies/gain.mrc',
            'rlnMicrographBinning': 1.0,
            'rlnMicrographOriginalPixelSize': 0.885,
            'rlnMicrographDoseRate': 1.277,
            'rlnMicrographPreExposure': 0.0,
            'rlnVoltage': 200.0,
            'rlnMicrographStartFrame': 1,
            'rlnMotionModelVersion': 1
        }
        dRow.update(kwargs)
        sf.writeSingleRow('general', dRow)

    def _writeMicGlobal(self, sf, globalShiftsFn, localShiftsFn):
        cols = ["rlnMicrographFrameNumber",
                "rlnMicrographShiftX",
                "rlnMicrographShiftY"]

        def _table_from_file(tableName, fn, i=None):
            if not os.path.exists(fn):
                return

            t = Table(columns=cols)
            sf.writeHeader(tableName, t)
            for line in TextFile.stripLines(fn):
                values = line.split()[:i] if i else line.split()
                sf.writeRowValues(values)

        _table_from_file('global_shift', globalShiftsFn)
        cols.insert(1, 'rlnCoordinateX')
        cols.insert(2, 'rlnCoordinateY')
        _table_from_file('local_shift', localShiftsFn, i=-1)

    def prerun(self):
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

        if self.mc.version >= 3:
            outName = 'mc3_micrographs_ctf.star'
        else:
            outName = 'mc2_micrographs.star'
        self._outFn = self.join(outName)
        self._outFile = open(self._outFn, 'w')  #FIXME improve for continue
        self._outSf = StarFile(self._outFile)
        self._outSf.writeTable('optics', self.newOptics)
        self._writeMicrographsTableHeader()
        # Write Relion-compatible nodes files
        """
        data_output_nodes
loop_
_rlnPipeLineNodeName #1
_rlnPipeLineNodeType #2
External/job006/coords_suffix_topaz.star            2 
        """
        with StarFile(self.join('RELION_OUTPUT_NODES.star'), 'w') as sf:
            t = Table(columns=['rlnPipeLineNodeName', 'rlnPipeLineNodeType'])
            t.addRowValues(self._outFn, 1)
            sf.writeTable('output_nodes', t)

        with open(self.join('job.json'), 'w') as f:
            json.dump({
                'inputs': [self.inputStar],
                'outputs': [self._outFn]
            }, f)

    def postrun(self):
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
