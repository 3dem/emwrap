import os
import tempfile
import unittest

import mrcfile
import numpy as np

from emtools.metadata import Table
from emwrap.aretomo.aretomo3_align import AreTomo3AlignPipeline
from emwrap.aretomo.aretomo3_reconstruct import AreTomo3ReconstructPipeline


class TestAreTomo3ModularStaging(unittest.TestCase):
    def _pipeline(self, cls):
        pipeline = object.__new__(cls)
        pipeline.log = lambda *_args, **_kwargs: None
        return pipeline

    def test_cmd1_stack_and_tlt_follow_star_order(self):
        pipeline = self._pipeline(AreTomo3AlignPipeline)
        with tempfile.TemporaryDirectory() as directory:
            first = os.path.join(directory, 'first.mrc')
            second = os.path.join(directory, 'second.mrc')
            for path, value in ((first, 1), (second, 2)):
                with mrcfile.new(path) as mrc:
                    mrc.set_data(np.full((3, 4), value, dtype=np.float32))
            table = Table(['rlnMicrographName', 'rlnTomoNominalStageTiltAngle'])
            table.addRowValues(rlnMicrographName=second, rlnTomoNominalStageTiltAngle='20')
            table.addRowValues(rlnMicrographName=first, rlnTomoNominalStageTiltAngle='-20')
            stack = os.path.join(directory, 'TS.mrc')
            tlt = os.path.join(directory, 'TS_TLT.txt')
            pipeline._write_stack_from_images(stack, table)
            pipeline._write_tlt(tlt, table)
            with mrcfile.open(stack) as mrc:
                self.assertEqual(mrc.data.shape, (2, 3, 4))
                self.assertEqual(float(mrc.data[0, 0, 0]), 2)
                self.assertEqual(float(mrc.data[1, 0, 0]), 1)
            with open(tlt) as handle:
                self.assertEqual(handle.read().splitlines(), ['20.000000 1', '-20.000000 2'])

    def test_half_set_columns_must_be_complete(self):
        pipeline = self._pipeline(AreTomo3AlignPipeline)
        table = Table(['rlnMicrographName', 'rlnMicrographNameOdd', 'rlnMicrographNameEven'])
        table.addRowValues(rlnMicrographName='full-1.mrc', rlnMicrographNameOdd='odd-1.mrc',
                           rlnMicrographNameEven='even-1.mrc')
        table.addRowValues(rlnMicrographName='full-2.mrc', rlnMicrographNameOdd='',
                           rlnMicrographNameEven='even-2.mrc')
        self.assertFalse(pipeline._has_complete_image_column(table, 'rlnMicrographNameOdd'))
        self.assertTrue(pipeline._has_complete_image_column(table, 'rlnMicrographNameEven'))

    def test_synthetic_aln_and_ctf(self):
        pipeline = self._pipeline(AreTomo3ReconstructPipeline)
        with tempfile.TemporaryDirectory() as directory:
            table = Table(['rlnTomoYTilt', 'rlnTomoXShiftAngst', 'rlnTomoYShiftAngst',
                           'rlnTomoXTilt', 'rlnTomoZRot', 'rlnDefocusU', 'rlnDefocusV',
                           'rlnDefocusAngle', 'rlnCtfFigureOfMerit', 'rlnCtfMaxResolution'])
            table.addRowValues(rlnTomoYTilt='30', rlnTomoXShiftAngst='4',
                               rlnTomoYShiftAngst='-2', rlnTomoXTilt='0', rlnTomoZRot='0',
                               rlnDefocusU='10000', rlnDefocusV='11000', rlnDefocusAngle='5',
                               rlnCtfFigureOfMerit='0.8', rlnCtfMaxResolution='8')
            global_table = Table(['rlnTomoName', 'rlnTomoNominalTiltAxisAngle'])
            global_table.addRowValues(rlnTomoName='TS', rlnTomoNominalTiltAxisAngle='85')
            aln = os.path.join(directory, 'TS.aln')
            ctf = os.path.join(directory, 'TS_CTF.txt')
            pipeline._write_synthetic_aln(aln, table, global_table[0], 2)
            pipeline._write_synthetic_ctf(ctf, table)
            with open(aln) as handle:
                self.assertIn('1 85.000000 1 2.000000 -1.000000 0 0 1 0 30.000000', handle.read())
            with open(ctf) as handle:
                self.assertEqual(handle.read().split()[0:4], ['1', '10000.000000', '11000.000000', '5.000000'])

    def test_synthetic_ctf_rejects_missing_required_values(self):
        pipeline = self._pipeline(AreTomo3ReconstructPipeline)
        table = Table(['rlnDefocusU', 'rlnDefocusV', 'rlnDefocusAngle'])
        table.addRowValues(rlnDefocusU='10000', rlnDefocusV='', rlnDefocusAngle='0')
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, 'CTF correction requires'):
                pipeline._write_synthetic_ctf(os.path.join(directory, 'ctf.txt'), table)
