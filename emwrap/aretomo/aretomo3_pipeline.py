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
import shutil
import csv

#Plots and reading mrc
# import mrcfile
# import numpy as np

from emtools.utils import Color, FolderManager
from emtools.image import Image
from emtools.jobs import TsStarBatchManager
from emtools.metadata import StarFile, Acquisition, Table, Mdoc

from emwrap.base import ProcessingPipeline
from .aretomo3 import AreTomo3

# TODO: 'rlnTiltSeriesAligned' is not aligned is the normal tilt series unaligned

def _fix_mdoc_subframe_paths(mdocPath, destPath, relDir='.'):
    """
    Read mdocPath, rewrite every SubFramePath line so it points to the
    basename of the original movie file prefixed with relDir.
    relDir: relative path (from AreTomo3's working directory) where the
            movie files actually live, e.g. '.' if mdoc and movies are
            siblings in the same batch folder, or 'tmp_mdoc' if movies
            live in a subfolder relative to where AreTomo3 is invoked.
    """
    mdoc = Mdoc.parse(mdocPath)
    # Let's write a local MDOC file with fixed filenames
    for _, section in mdoc.zvalues:
        section['SubFramePath'] = f'./{Mdoc.getSubFrameBase(section)}' 

    mdoc.write(destPath)


class AreTomo3Pipeline(ProcessingPipeline):
    """ Pipeline specific to AreTomo3 preprocessing. """
    name = 'emw-aretomo3'

    def __init__(self, args, output):
        ProcessingPipeline.__init__(self, args, output) # self._args is defined here
        gpus = self._args.get('gpus', '')
        if gpus is not None and gpus != '':
            gpus = str(gpus).strip()
        else:
            gpus = ''
        self.gpuList = self.get_gpu_list(gpus) if gpus else []
        self.outputTomDir = 'tomograms'
        self.outputTsDir = 'tilt_series'
        self.acq = self.loadAcquisition()
        self.inputLen = 0
        self.inputGain = self.acq.get('gain', None)
        self._allResults = {}  # tsName -> result dict, accumulated by _output
        self.inputTsTable = None  # set in prerun via _getInputTsTable
        self.inputTs = None
        self.registerOnly = self._register_output_only() # DEBUG flag

    
    # -------- Simple configuration / column definitions --------
    def _aligned_tilt_series_extra_cols(self):
        return [
            'rlnTomoTiltSeriesPixelSize',
            'rlnTomoTiltSeriesStarFile',
            'rlnTiltSeriesAligned',
            'rlnTiltSeriesAlignedOdd',
            'rlnTiltSeriesAlignedEvn',
            'rlnTomoMdocFile',
        ]

    def _individual_tilt_series_extra_cols(self):
        return [
            # Motion correction / movie outputs
            'rlnCtfPowerSpectrum',
            'rlnMicrographNameEven',
            'rlnMicrographNameOdd',
            'rlnMicrographName',
            'rlnMicrographMetadata',
            'rlnAccumMotionTotal',
            'rlnAccumMotionEarly',
            'rlnAccumMotionLate',
            # CTF estimation
            'rlnCtfImage',
            'rlnDefocusU',
            'rlnDefocusV',
            'rlnCtfAstigmatism',
            'rlnDefocusAngle',
            'rlnCtfFigureOfMerit',
            'rlnCtfMaxResolution',
            'rlnCtfIceRingDensity',
            # Tilt-series alignment
            'rlnTomoXTilt',
            'rlnTomoYTilt',
            'rlnTomoZRot',
            'rlnTomoXShiftAngst',
            'rlnTomoYShiftAngst',
            'rlnCtfScalefactor',
        ]

    def _tomogram_extra_cols(self):
        return [
            'rlnTomoReconstructedTomogram',
            'rlnTomoTomogramBinning',
            'rlnTomoSizeX',
            'rlnTomoSizeY',
            'rlnTomoSizeZ',
            'rlnTomoReconstructedTomogramHalf1',
            'rlnTomoReconstructedTomogramHalf2',
        ]
    
    
    # -------- Small generic helpers -------------
    @staticmethod
    def _get_first_binning_value(value):
        if value is None:
            return 1.0

        text = str(value).strip()
        if not text:
            return 1.0

        first_value = text.split()[0]
        try:
            return float(first_value)
        except ValueError:
            return 1.0

    def newTargetTsPs(self, inputPs):
        # Motion correction binning is applied to the tilt series before
        # reconstruction, so the effective pixel size becomes the input
        # pixel size multiplied by the McBin value.
        binning = self._args.get('aretomo3.McBin', 1)
        binning = int(binning) if binning not in ('', None) else 1
        newPx = inputPs * float(binning) if binning > 0 else inputPs
        return newPx

    def newTargetTomPs(self, inputPs):
        tsPs = self.newTargetTsPs(inputPs)
        tomBinning = self._get_first_binning_value(self._args.get('aretomo3.AtBin', ''))
        newPx = tsPs * tomBinning if tomBinning > 0 else tsPs
        return newPx
    
    
    # ----- Input/output folder helpers --------
    def _getInputTsTable(self):
        """ Read input star file and return the 'global' table. """
        inputStar = self._args['input_tiltseries']
        with StarFile(inputStar) as sf:
            t = sf.getTable('global')
            self.inputLen = len(t)  # Let's update the inputLen property
            return t
        return None
    
    def _getOutputTsFolder(self, tsName):
        return FolderManager(self.join(self.outputTsDir, tsName))

    def _getOutputTomFolder(self, tsName):
        return FolderManager(self.join(self.outputTomDir, tsName))

    def _copy_mdoc_to_output(self, tsName, result):
        mdocFile = result.get('rlnTomoMdocFile', None)

        if not mdocFile or not os.path.exists(mdocFile):
            return None

        mdocsFm = FolderManager(self.join('mdocs'))
        mdocsFm.create()
        dstMdocFile = mdocsFm.join(f'{tsName}.mdoc')

        if os.path.abspath(mdocFile) != os.path.abspath(dstMdocFile):
            shutil.copy2(mdocFile, dstMdocFile)

        return dstMdocFile

    def write_ts_table(self, tableName, table, starFile):
        self.log(f"Writing: {starFile}")
        with StarFile(starFile, 'w') as sfOut:
            sfOut.writeTable(tableName, table, computeFormat='left', timeStamp=True)
    
    
    # -------- Parsers for AreTomo3 output files --------------
    def _read_tilt_angle_mapping(self, mappingFile):
        """Read AreTomo3 *_TLT.txt file.
        Expected columns:
            1. tilt angle
            2. micrograph acquisition index, 1-based
            3. optional value currently ignored
        Returns:
            dict[int, float]: {micrograph_index_1_based: tilt_angle}
        """
        tiltAnglesByIndex = {}

        if not mappingFile or not os.path.exists(mappingFile):
            return tiltAnglesByIndex

        with open(mappingFile) as f:
            for lineNo, line in enumerate(f, start=1):
                line = line.strip()

                if not line or line.startswith('#'):
                    continue

                parts = line.split()
                if len(parts) < 2:
                    continue

                try:
                    tiltAngle = float(parts[0])
                    micrographIndex = int(parts[1])
                except ValueError:
                    self.log(
                        f"WARNING: Could not parse tilt mapping line {lineNo}: {line}"
                    )
                    continue

                tiltAnglesByIndex[micrographIndex] = tiltAngle

        return tiltAnglesByIndex

    def _read_ctf_estimation_file(self, ctfFile):
        """Read AreTomo3 *_CTF.txt file.

        Expected columns:
            1. micrograph number, 1-based
            2. defocus 1 [A]
            3. defocus 2 [A]
            4. astigmatism azimuth [deg]
            5. additional phase shift [radian]
            6. cross correlation
            7. CTF fit resolution [A]
            8. dfHand

        Returns:
            dict[int, dict]: {micrograph_index_1_based: ctf_values}
        """
        ctfByIndex = {}

        if not ctfFile or not os.path.exists(ctfFile):
            return ctfByIndex

        with open(ctfFile) as f:
            for lineNo, line in enumerate(f, start=1):
                line = line.strip()

                if not line or line.startswith('#'):
                    continue

                parts = line.split()
                if len(parts) < 8:
                    self.log(
                        f"WARNING: Could not parse CTF line {lineNo}: "
                        f"expected 8 columns, got {len(parts)}"
                    )
                    continue

                try:
                    micrographIndex = int(parts[0])
                    defocusU = float(parts[1])
                    defocusV = float(parts[2])
                    defocusAngle = float(parts[3])
                    phaseShiftRad = float(parts[4])
                    ctfScore = float(parts[5])
                    ctfMaxResolution = float(parts[6])
                    dfHand = int(parts[7])
                except ValueError:
                    self.log(
                        f"WARNING: Could not parse CTF line {lineNo}: {line}"
                    )
                    continue

                ctfByIndex[micrographIndex] = {
                    'rlnDefocusU': defocusU,
                    'rlnDefocusV': defocusV,
                    'rlnCtfAstigmatism': abs(defocusU - defocusV),
                    'rlnDefocusAngle': defocusAngle,
                    'rlnCtfFigureOfMerit': ctfScore,
                    'rlnCtfMaxResolution': ctfMaxResolution,
                    # Not currently in your columns, but useful if you add them later.
                    'at3PhaseShiftRad': phaseShiftRad,
                    'at3DfHand': dfHand,
                }

        return ctfByIndex

    def _read_aretomo3_alignment_file(self, alnFile, pixelSize):
        """Read AreTomo3 .aln file.
        Returns:
            {
                'alphaOffset': float or '',
                'betaOffset': float or '',
                'thickness': float or '',
                'global': {
                    sec_1_based: {
                        'sec': int,
                        'rot': float,
                        'gmag': float,
                        'txPix': float,
                        'tyPix': float,
                        'txAngst': float,
                        'tyAngst': float,
                        'smean': float,
                        'sfit': float,
                        'scale': float,
                        'base': float,
                        'tilt': float,
                    }
                },
                'local': {
                    sec_1_based: [
                        {
                            'patch': int,
                            'x': float,
                            'y': float,
                            'dxPix': float,
                            'dyPix': float,
                            'dxAngst': float,
                            'dyAngst': float,
                            'score': float,
                        },
                        ...
                    ]
                }
            }
        """
        alignment = {
            'alphaOffset': '',
            'betaOffset': '',
            'thickness': '',
            'global': {},
            'local': {},
        }

        if not alnFile or not os.path.exists(alnFile):
            return alignment

        inLocalAlignment = False

        with open(alnFile) as f:
            for lineNo, line in enumerate(f, start=1):
                rawLine = line
                line = line.strip()

                if not line:
                    continue

                if line.startswith('#'):
                    if 'AlphaOffset' in line:
                        try:
                            alignment['alphaOffset'] = float(line.split('=')[1].strip())
                        except Exception:
                            self.log(f"WARNING: Could not parse AlphaOffset in {alnFile}:{lineNo}")

                    elif 'BetaOffset' in line:
                        try:
                            alignment['betaOffset'] = float(line.split('=')[1].strip())
                        except Exception:
                            self.log(f"WARNING: Could not parse BetaOffset in {alnFile}:{lineNo}")

                    elif 'Thickness' in line:
                        try:
                            alignment['thickness'] = float(line.split('=')[1].strip())
                        except Exception:
                            self.log(f"WARNING: Could not parse Thickness in {alnFile}:{lineNo}")

                    elif line.startswith('# Local Alignment'):
                        inLocalAlignment = True

                    continue

                parts = line.split()

                try:
                    if not inLocalAlignment:
                        # Global alignment:
                        # SEC ROT GMAG TX TY SMEAN SFIT SCALE BASE TILT
                        if len(parts) < 10:
                            self.log(
                                f"WARNING: Expected 10 global alignment columns "
                                f"in {alnFile}:{lineNo}, got {len(parts)}"
                            )
                            continue

                        sec = int(parts[0])
                        txPix = float(parts[3])
                        tyPix = float(parts[4])

                        alignment['global'][sec] = {
                            'sec': sec,
                            'rot': float(parts[1]),
                            'gmag': float(parts[2]),
                            'txPix': txPix,
                            'tyPix': tyPix,
                            'txAngst': txPix * pixelSize,
                            'tyAngst': tyPix * pixelSize,
                            'smean': float(parts[5]),
                            'sfit': float(parts[6]),
                            'scale': float(parts[7]),
                            'base': float(parts[8]),
                            'tilt': float(parts[9]),
                        }

                    else:
                        # Local alignment:
                        # SEC PATCH X Y DX DY SCORE
                        if len(parts) < 7:
                            self.log(
                                f"WARNING: Expected 7 local alignment columns "
                                f"in {alnFile}:{lineNo}, got {len(parts)}"
                            )
                            continue

                        secZeroBased = int(parts[0])
                        sec = secZeroBased + 1
                        patch = int(parts[1])
                        dxPix = float(parts[4])
                        dyPix = float(parts[5])

                        alignment['local'].setdefault(sec, []).append({
                            'sec': sec,
                            'patch': patch,
                            'x': float(parts[2]),
                            'y': float(parts[3]),
                            'dxPix': dxPix,
                            'dyPix': dyPix,
                            'dxAngst': dxPix * pixelSize,
                            'dyAngst': dyPix * pixelSize,
                            'score': float(parts[6]),
                        })

                except ValueError:
                    self.log(f"WARNING: Could not parse alignment line {lineNo}: {rawLine.rstrip()}")

        return alignment

    
    # ----- Relion metadata conversion helpers -----------
    def _setAretomo3Params(self, tiltDict, result, micrographIndex=None, tiltAnglesByIndex=None, ctfByIndex=None,  alignmentByIndex=None):
        """Convert AreTomo3 per-tilt-series outputs into Relion per-tilt labels.
        This function is the central place for parsing AreTomo3 outputs such as:
        - *_TLT.txt
        - *.aln
        - *_CTF.txt
        - *_CTF.mrc
        - motion-correction outputs

        and mapping them into Relion labels in the individual TS_NAME.star file.
        """

        tiltAnglesByIndex = tiltAnglesByIndex or {}
        ctfByIndex = ctfByIndex or {}
        alignmentByIndex = alignmentByIndex or {
            'alphaOffset': '',
            'betaOffset': '',
            'thickness': '',
            'global': {},
            'local': {},
        }

        def setMotionCorrectionLabels():
            # TODO: parse motion-correction outputs when available.
            tiltDict.update({
                'rlnCtfPowerSpectrum':'',
                'rlnMicrographNameEven': '',
                'rlnMicrographNameOdd': '',
                'rlnMicrographName': '',
                'rlnMicrographMetadata': '',
                'rlnAccumMotionTotal': '',
                'rlnAccumMotionEarly': '',
                'rlnAccumMotionLate': '',
            })

        def setCTFEstimationLabels():
            ctfValues = ctfByIndex.get(micrographIndex, {})
            tiltDict.update({
                'rlnCtfImage': result.get('rlnCtfImage', ''),
                'rlnDefocusU': ctfValues.get('rlnDefocusU', ''),
                'rlnDefocusV': ctfValues.get('rlnDefocusV', ''),
                'rlnCtfAstigmatism': ctfValues.get('rlnCtfAstigmatism', ''),
                'rlnDefocusAngle': ctfValues.get('rlnDefocusAngle', ''),
                'rlnCtfFigureOfMerit': ctfValues.get('rlnCtfFigureOfMerit', ''),
                'rlnCtfMaxResolution': ctfValues.get('rlnCtfMaxResolution', ''),
                'rlnCtfIceRingDensity': '',
            })

        def setTiltSeriesAlignmentLabels():
            alnValues = alignmentByIndex.get('global', {}).get(micrographIndex, {})
            tiltDict.update({
            # Convention note:
            # AreTomo3 .aln has one TILT value per projection.
            # In many RELION tomo tables this corresponds to rlnTomoXTilt,
            # while rlnTomoYTilt is often empty/0 unless a separate Y tilt
            # estimate is available.
            'rlnTomoXTilt': alnValues.get('tilt', ''), #TODO: I am not sure this is the correct interpretation
            'rlnTomoYTilt': '',
            'rlnTomoZRot': alnValues.get('rot', ''),
            # AreTomo3 TX/TY are pixels; converted to Angstroms in parser.
            'rlnTomoXShiftAngst': alnValues.get('txAngst', ''),
            'rlnTomoYShiftAngst': alnValues.get('tyAngst', ''),
            'rlnCtfScalefactor': '' # alnValues.get('scale', ''), TODO: Not sure this is from here, I think this should be introduced by tomogram reconstruction
        })

        setMotionCorrectionLabels()
        setCTFEstimationLabels()
        setTiltSeriesAlignmentLabels()

        return tiltDict
    
    def _write_individual_tilt_series_star(self, tsName, tsRow, result, newTsPs):
        inputStarFile = tsRow.rlnTomoTiltSeriesStarFile
        idvTsTable = StarFile.getTableFromFile(tsName, inputStarFile)

        extraCols = self._individual_tilt_series_extra_cols()
        outputCols = idvTsTable.getColumnNames() + [
            c for c in extraCols
            if c not in idvTsTable.getColumnNames()
        ]

        newIdvTsTable = Table(outputCols)
        tiltAnglesByIndex = self._read_tilt_angle_mapping(
            result.get('at3MappingFile', None))

        ctfByIndex = self._read_ctf_estimation_file(
            result.get('at3TomoCtfFile', None))

        alignmentByIndex = self._read_aretomo3_alignment_file(
            result.get('at3TomoAlignmentFile', None),
            newTsPs
        )

        # Checks rejection of tilt images
        if tiltAnglesByIndex and len(tiltAnglesByIndex) != len(idvTsTable):
            self.log(
                f"WARNING: {tsName}: AreTomo3 mapping has {len(tiltAnglesByIndex)} "
                f"entries but STAR has {len(idvTsTable)} rows."
            )

        for micrographIndex, tiltRow in enumerate(idvTsTable, start=1):
            tiltDict = tiltRow._asdict()
            
            tiltDict = self._setAretomo3Params(
                tiltDict, 
                result, 
                micrographIndex=micrographIndex,
                tiltAnglesByIndex=tiltAnglesByIndex,
                ctfByIndex=ctfByIndex, 
                alignmentByIndex=alignmentByIndex
                )

            newIdvTsTable.addRowValues(**tiltDict)

        idvTsStarFile = self.join(self.outputTsDir, f'{tsName}.star')
        self.write_ts_table(tsName, newIdvTsTable, idvTsStarFile)

        return idvTsStarFile

    def _build_aligned_ts_row(self, tsRow, result, newTsPs, idvTsStarFile, dstMdocFile):
        tsDict = tsRow._asdict()
        tsDict.update({
            'rlnTomoTiltSeriesPixelSize': newTsPs,
            'rlnTomoTiltSeriesStarFile': idvTsStarFile,
            'rlnTiltSeriesAligned': result.get('rlnTiltSeriesAligned', ''),
            'rlnTiltSeriesAlignedOdd': result.get('rlnTiltSeriesAlignedOdd', ''),
            'rlnTiltSeriesAlignedEvn': result.get('rlnTiltSeriesAlignedEvn', ''),
            'rlnTomoMdocFile': dstMdocFile or '',
        })

        return tsDict

    def _build_tomogram_row(self, tsRow, result, tomDims):
        tomBinning = self._get_first_binning_value(
            self._args.get('aretomo3.AtBin', '')
        )
        tomDict = tsRow._asdict()

        tomDict.update({
            'rlnTomoReconstructedTomogram': result.get('rlnTomoReconstructedTomogram', ''),
            'rlnTomoTomogramBinning': tomBinning,
            'rlnTomoSizeX': tomDims[0],
            'rlnTomoSizeY': tomDims[1],
            'rlnTomoSizeZ': tomDims[2],
            # In your result dict these are currently named rlnTomoNameOdd/Evn,
            # but the tomograms.star columns you chose are Half1/Half2.
            'rlnTomoReconstructedTomogramHalf1': result.get('rlnTomoNameOdd', ''),
            'rlnTomoReconstructedTomogramHalf2': result.get('rlnTomoNameEvn', ''),
        })

        return tomDict

    
    # ------- Output registration ------------
    def _registerAretomo3CsvOutputs(self):
        metricsRows = []
        metricsHeader = None

        timestampRows = []
        timestampHeader = None

        for tsName, result in self._allResults.items():
            if 'error' in result:
                continue

            metricsCsv = result.get('at3MetricsCsv')
            if metricsCsv and os.path.exists(metricsCsv):
                header, rows = self._read_csv_rows(metricsCsv)
                if metricsHeader is None:
                    metricsHeader = header
                metricsRows.extend(rows)

            timestampCsv = result.get('at3TimeStampCsv')
            if timestampCsv and os.path.exists(timestampCsv):
                header, rows = self._read_csv_rows(timestampCsv)
                if timestampHeader is None:
                    timestampHeader = header
                timestampRows.extend(rows)

        metricsOut = self.join('TiltSeries_Metrics.csv')
        timestampOut = self.join('TiltSeries_TimeStamp.csv')

        self._write_csv_rows(metricsOut, metricsHeader, metricsRows)   
        self._write_csv_rows(timestampOut, timestampHeader, timestampRows)

    def _read_csv_rows(self, csvPath):
        with open(csvPath, newline='') as f:
            reader = csv.DictReader(f)
            return reader.fieldnames, list(reader)

    def _write_csv_rows(self, csvPath, fieldnames, rows):
        if not fieldnames or not rows:
            return False

        with open(csvPath, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

        return True
    
    def _collect_existing_final_result(self, tsName):
        tsFolder = self._getOutputTsFolder(tsName)
        tomFolder = self._getOutputTomFolder(tsName)

        result = {'rlnTomoName': tsName}

        def _add_if_exists(key, folder, filename):
            path = folder.join(filename)
            if os.path.exists(path):
                result[key] = path
                return path
            return None

        # Files expected in jobX/tiltseries/<tsName>/
        _add_if_exists('rlnTiltSeriesAligned', tsFolder, f'{tsName}.mrc')
        _add_if_exists('rlnTiltSeriesAlignedOdd', tsFolder, f'{tsName}_ODD.mrc')
        _add_if_exists('rlnTiltSeriesAlignedEvn', tsFolder, f'{tsName}_EVN.mrc')
        _add_if_exists('at3TomoAlignmentFile', tsFolder, f'{tsName}.aln')
        _add_if_exists('at3MappingFile', tsFolder, f'{tsName}_TLT.txt')
        _add_if_exists('at3TomoCtfFile', tsFolder, f'{tsName}_CTF.txt')
        _add_if_exists('rlnCtfImage', tsFolder, f'{tsName}_CTF.mrc')
        _add_if_exists('rlnTomoMdocFile', FolderManager(self.join('mdocs')), f'{tsName}.mdoc')

        _add_if_exists('at3MetricsCsv', tsFolder, 'TiltSeries_Metrics.csv')
        _add_if_exists('at3TimeStampCsv', tsFolder, 'TiltSeries_TimeStamp.csv')

        # Files expected in jobX/tomograms/<tsName>/
        _add_if_exists('rlnTomoReconstructedTomogram', tomFolder, f'{tsName}_Vol.mrc')
        _add_if_exists('rlnTomoNameOdd', tomFolder, f'{tsName}_ODD_Vol.mrc')
        _add_if_exists('rlnTomoNameEvn', tomFolder, f'{tsName}_EVN_Vol.mrc')
        _add_if_exists('at3ThicknessMrc', tomFolder, f'{tsName}_Thick.mrc')
        _add_if_exists('at3ThicknessCsv', tomFolder, f'{tsName}_Thick_CC.csv')

        if 'rlnTiltSeriesAligned' not in result:
            result['error'] = (
                f'Missing expected final aligned tilt-series: '
                f'{tsFolder.join(f"{tsName}.mrc")}'
            )

        return result
    
    def _register_existing_final_outputs(self):
        self.log(
            'DEBUG register-only mode: rebuilding outputs from final job folders.',
            flush=True
        )

        self._allResults = {}

        for row in self.inputTsTable:
            tsName = row.rlnTomoName

            result = self._collect_existing_final_result(tsName)
            self._allResults[tsName] = result

            if 'error' in result:
                self.log(f"DEBUG register-only: {tsName}: {result['error']}")
            else:
                self.log(f"DEBUG register-only: found final outputs for {tsName}")

        self._registerOutputs()

        self.info['register_only'] = True
        self.info['aretomo3_output'] = len([
            r for r in self._allResults.values()
            if 'error' not in r
        ])

    def _registerOutputs(self):
        """Rebuild Relion-style AreTomo3 outputs.
        Outputs:
            aligned_tilt_series.star
                Global table containing all successfully aligned tilt series.
                Each row points to its per-tilt-series STAR file through
                rlnTomoTiltSeriesStarFile.

            tilt_series/<TS_NAME>.star
                Per-tilt-series metadata for each tilt image.

            tomograms.star
                Global table containing reconstructed tomograms, only if
                reconstruction was produced.

            failed_tilt_series.star
                Input tilt-series rows that failed registration.
        """

        self.log("Registering output STAR files.")

        alignedStarFile = self.join('aligned_tilt_series.star')
        failedStarFile = self.join('failed_tilt_series.star')
        tomogramsStarFile = self.join('tomograms.star')

        inputCols = self.inputTsTable.getColumnNames()

        alignedExtraCols = [
            c for c in self._aligned_tilt_series_extra_cols()
            if c not in inputCols
        ]

        tomExtraCols = [
            c for c in self._tomogram_extra_cols()
            if c not in inputCols
        ]

        alignedTable = Table(inputCols + alignedExtraCols)
        failedTable = Table(inputCols)
        tomogramsTable = Table(inputCols + tomExtraCols)

        inputByName = {row.rlnTomoName: row for row in self.inputTsTable}

        tsDims = None
        tomDims = None
        haveTomograms = False
        newTsPs = None

        for tsName, result in self._allResults.items():
            tsRow = inputByName.get(tsName, None)
            if tsRow is None:
                self.log(f"WARNING: Result for unknown tilt series {tsName}, skipping.")
                continue

            if newTsPs is None:
                inputPs = tsRow.rlnMicrographOriginalPixelSize
                newTsPs = self.newTargetTsPs(inputPs)
                self.log(f"New target tilt series pixel size: {newTsPs:0.3f} Å/px")

            if 'error' in result:
                failedTable.addRowValues(**tsRow._asdict())
                continue

            tsAligned = result.get('rlnTiltSeriesAligned', None)
            if tsAligned is None or not os.path.exists(tsAligned):
                self.log(f"Missing aligned tilt series for {tsName}, marking failed.")
                failedTable.addRowValues(**tsRow._asdict())
                continue

            if tsDims is None:
                tsDims = Image.get_dimensions(tsAligned)

            dstMdocFile = self._copy_mdoc_to_output(tsName, result)

            idvTsStarFile = self._write_individual_tilt_series_star(
                tsName,
                tsRow,
                result, 
                newTsPs
            )

            alignedRow = self._build_aligned_ts_row(
                tsRow,
                result,
                newTsPs,
                idvTsStarFile,
                dstMdocFile
            )

            alignedTable.addRowValues(**alignedRow)

            tomogram = result.get('rlnTomoReconstructedTomogram', None)
            if tomogram and os.path.exists(tomogram):
                haveTomograms = True
                tomDimsThis = Image.get_dimensions(tomogram)
                if tomDims is None:
                    tomDims = tomDimsThis

                tomRow = self._build_tomogram_row(
                    tsRow,
                    result,
                    tomDimsThis
                )

                tomogramsTable.addRowValues(**tomRow)

        # aligned_tilt_series.star
        self.write_ts_table('global', alignedTable, alignedStarFile)
        outputNodes = [[alignedStarFile, 'TomogramGroupMetadata.star.relion.tomo.aligntiltseries']]

        if len(failedTable) > 0:
            self.write_ts_table('global', failedTable, failedStarFile)
            outputNodes.append(
                [failedStarFile, 'TomogramGroupMetadata.star.relion.tomo.aligntiltseries-failed'])

        if haveTomograms:
            # tomograms.star
            self.write_ts_table('global', tomogramsTable, tomogramsStarFile)
            outputNodes.append([tomogramsStarFile, 'TomogramGroupMetadata.star.relion.tomo.tomograms'])

        self.writeRelionOutputNodes(outputNodes)
        self._registerAretomo3CsvOutputs()   
    
    
    # ---------- Batch execution --------------
    def get_aretomo3_proc(self, gpu):
        def _aretomo3(batch):
            # In this pipeline, batch are not created until now, when we are
            # processing each one.
            # We also need to create the links to movies and mdocs files
            items = batch['items']
            mdoc = batch['tsMdoc']
            
            batch.create()
            
            def _absfn(item):
                return os.path.abspath(item['rlnMicrographMovieName'])

            mdocAbs = os.path.abspath(mdoc)
            print(f"mdoc: {mdocAbs}")

            # --- Full folder link, side location only, for browsing/debugging.
            framesFolder = os.path.dirname(_absfn(items[0]))
            framesLink = batch.join('frames')
            if not os.path.exists(framesLink):
                os.symlink(framesFolder, framesLink)

            # --- Batch-only movie links, directly in batch root.
            for item in items:
                srcPath = _absfn(item)
                baseName = os.path.basename(srcPath)
                destPath = batch.join(baseName)
                if not os.path.lexists(destPath):
                    os.symlink(srcPath, destPath)

            # --- Rewrite the mdoc's SubFramePath entries to basenames only,
            # and write the corrected copy directly into batch root (not a link,
            # since the content itself is being modified).
            mdocBase = os.path.basename(mdocAbs)
            mdocDest = batch.join(mdocBase)
            _fix_mdoc_subframe_paths(mdocAbs, mdocDest, relDir='.')
            # Link the gain reference
            acq = Acquisition(self.acq) 

            if self.inputGain:
                acq['gain'] = batch.link(self.inputGain)

            at3 = AreTomo3(acq, **self._args)
            at3.process_batch(batch, gpu=gpu)
            
            return batch

        return _aretomo3
    
    def _output(self, batch):
        """ Register output STAR files. Runs per-batch (streaming): each
        call rewrites the aggregate tables to include this batch's result,
        so outputs are available incrementally as each tilt series finishes,
        without waiting for the whole input to be processed. """
        tsName = batch['tsName']
        batch.log(f"Storing output for batch '{tsName}'", flush=True)

        if batch.error:
            batch.log(f"ERROR: {batch.error}")
            self._allResults[tsName] = {'error': batch.error}
        else:
            result = batch['results'][0] if batch['results'] else {}

            tsFolder = self._getOutputTsFolder(tsName)
            tsFolder.create()
            tomFolder = None  # only created if a tomogram was produced

            def _copy(srcKey, destFolder):
                """ Copy the file at result[srcKey] into destFolder, update
                result[srcKey] to the copied file. Returns the new path, or
                None if srcKey wasn't populated for this batch. """
                src = result.get(srcKey, None)
                if src is None or not os.path.exists(src):
                    return None

                dst = destFolder.join(os.path.basename(src))
                
                if os.path.abspath(src) != os.path.abspath(dst):
                    shutil.copy2(src, dst)
                
                result[srcKey] = dst
                return dst

            # --- Aligned tilt series outputs -> outputTsDir
            _copy('rlnTiltSeriesAligned', tsFolder)
            _copy('rlnTiltSeriesAlignedOdd', tsFolder)
            _copy('rlnTiltSeriesAlignedEvn', tsFolder)
            _copy('at3TomoAlignmentFile', tsFolder)
            _copy('at3MappingFile', tsFolder)
            _copy('at3TomoCtfFile', tsFolder)
            _copy('rlnCtfImage', tsFolder)
            _copy('at3MetricsCsv', tsFolder)
            _copy('at3TimeStampCsv', tsFolder)

            # --- Tomogram outputs (only present if reconstruction was enabled) -> outputTomDir
            if result.get('rlnTomoReconstructedTomogram', None) is not None:
                tomFolder = self._getOutputTomFolder(tsName)
                tomFolder.create()
                _copy('rlnTomoReconstructedTomogram', tomFolder)
                _copy('rlnTomoNameOdd', tomFolder)
                _copy('rlnTomoNameEvn', tomFolder)
                _copy('at3ThicknessMrc', tomFolder)
                _copy('at3ThicknessCsv', tomFolder)

            batch.info['result'] = {k: v for k, v in result.items()
                                    if k != 'error'}
            self._allResults[tsName] = result

        batch.info['tsName'] = tsName  # Store tsName in the info.json

        # Rebuild and re-register the aggregate outputs now, so they reflect
        # everything completed so far -- this is what makes results visible
        # incrementally rather than only once the whole input is processed.
        self._registerOutputs()

        self.updateBatchInfo(batch)

        if self.inputLen:
            totalOutput = len(self.info['batches'])
            percent = totalOutput * 100 / self.inputLen
            batch.log(f">>> Processed {Color.green(totalOutput)} out of "
                    f"{Color.red(self.inputLen)} "
                    f"({Color.bold('%0.2f' % percent)} %)", flush=True)
        
        return batch


    # -------- Pipeline lifecycle ---------- 
    def prerun(self):
        self.inputTsTable = self._getInputTsTable()
        self.inputTs =  self._args['input_tiltseries']
        print(f"Input tilt-series: {len(self.inputTsTable)}")  
        
        self.mkdir(self.outputTsDir)
        self.mkdir(self.outputTomDir)


        if self.registerOnly:
            self._register_existing_final_outputs()
            return
        
        batchMgr = TsStarBatchManager(self.inputTsTable, self.tmpDir)
        g = self.addGenerator(batchMgr.generate)
        outputQueue = None
        
        print(f"Creating {len(self.gpuList)} processing threads.")
        for gpu in self.gpuList:
            p = self.addProcessor(g.outputQueue,
                                  self.get_aretomo3_proc(gpu),
                                  outputQueue=outputQueue)
            outputQueue = p.outputQueue

        self.addProcessor(outputQueue, self._output)


if __name__ == '__main__':
    AreTomo3Pipeline.main()