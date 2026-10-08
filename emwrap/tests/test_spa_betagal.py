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
import re
from glob import glob

from emtools.metadata import StarFile
from emtools.utils import Color

from .test_apof import TestApoF


class TestSpaBetagal(TestApoF):
    """ SPA tests with the beta-galactosidase movies of the RELION 3.0
    tutorial, taken from $EMHUB_TESTDATA/relion30_tutorial. """
    workflow_template = TestApoF.get_workflow_template('spa-otf')

    # Jobs of each test mode, in the order they need to be run.
    # MotionCor runs as the first step of the preprocessing job.
    modes = {
        'preprocessing': [
            'emw-import-movies',
            'emw-preprocessing',
        ],
        '2d': [
            'emw-import-movies',
            'emw-preprocessing',
            'emw-rln2d',
        ]
    }

    job_types = modes['preprocessing']

    expected_outputs = {
        'emw-import-movies': 'movies.star',
        'emw-preprocessing': 'micrographs.star',
        'emw-rln2d': 'RELION_OUTPUT_NODES.star',
    }

    dataset = 'relion30_tutorial'

    # RELION 3.0 tutorial acquisition and parameters used in the
    # MotionCor and preprocessing tests (tests_motioncor, tests_preprocessing)
    acquisition = {
        'acq.pixel_size': '0.885',
        'acq.voltage': '200',
        'acq.cs': '1.4',
        'acq.amplitude_contrast': '0.1',
        'acq.dose': '1.277',  # dose per frame
    }
    motioncor_bin = 2

    # The base class test is for the ApoF workflows
    test_apof = None

    def test_betagal(self):
        self._run_workflow()

    @classmethod
    def get_data_root(cls, args):
        if args.data:
            return args.data
        if testdata := os.environ.get('EMHUB_TESTDATA'):
            return os.path.join(testdata, cls.dataset)
        return None

    @classmethod
    def set_args(cls, parser):
        parser.add_argument(
            '--project', '-p', metavar='PATH',
            help='Project folder for the test run. Outputs are kept for inspection.')
        parser.add_argument(
            '--data', metavar='PATH',
            help=f'Folder with the RELION 3.0 tutorial data '
                 f'(default: $EMHUB_TESTDATA/{cls.dataset}).')
        parser.add_argument(
            '--movies', metavar='PATTERN', default='*.tiff',
            help="Pattern of the movies to use, inside the Movies folder "
                 "(e.g. '*_0002[1-4]_*.tiff' for a few of them).")
        parser.add_argument(
            '--gpus', '-g', metavar='GPUs', type=int, default=1,
            help='Number of GPUs to use for the test run.')
        parser.add_argument(
            '-v', '--verbose', action='count', default=0,
            help='Increase unittest output verbosity.')
        parser.add_argument(
            '--dry', action='store_true',
            help='Dry run: do not actually run the jobs, just print what would be done.')
        parser.add_argument(
            '--mode', choices=list(cls.modes), default='preprocessing',
            help='preprocessing: import the movies and run the preprocessing '
                 '(MotionCor3, ctffind, crYOLO and extraction), checking the '
                 'motion corrected micrographs; '
                 '2d: preprocessing followed by Relion 2D classification '
                 '(default: preprocessing).')

    def _validate_environment(self):
        if not self.data_root:
            self.fail(f'Test data path could not be resolved, set EMHUB_TESTDATA '
                      f'(with a {self.dataset} folder) or use --data')
        if not glob(os.path.join(self.data_root, 'Movies', self.args.movies)):
            self.fail(f"No movies found: {os.path.join(self.data_root, 'Movies', self.args.movies)}")
        super()._validate_environment()

    def patch_workflow_params(self, jobs):
        """ Set the tutorial acquisition and short wait times, since all
        movies are already there (the template is for on-the-fly). """
        jobs = super().patch_workflow_params(jobs)
        gain = os.path.join(self.data_root, 'Movies', 'gain.mrc')

        for job in jobs:
            params = job['params']
            if job['jobtype'] == 'emw-import-movies':
                params.update(self.acquisition)
                params.update({
                    'in_movies': f'data/Movies/{self.args.movies}',
                    'acq.gain': 'data/Movies/gain.mrc' if os.path.exists(gain) else '',
                    'wait.timeout': '1',
                    'wait.file_change': '1',
                    'wait.sleep': '1',
                })
            elif job['jobtype'] == 'emw-preprocessing':
                params.update({
                    'motioncor.bin': str(self.motioncor_bin),
                    'motioncor.patch': '5 5',
                    'motioncor.dose_weighting': True,
                    'motioncor.extra_args': '',  # no gain flip for this dataset
                    'extract.scale': '100',
                    'input_timeout': '30',
                    'launcher_preprocessing': '',  # process batches locally
                })
            elif job['jobtype'] == 'emw-rln2d':
                # With the (large) template batch size, all particles go into
                # a single batch, created when there is no new input
                params.update({
                    'wait.timeout': '30',
                    'wait.sleep': '10',
                })
        return jobs

    def _run_workflow(self):
        """Select the jobs to run based on the test mode."""
        self.job_types = self.modes[self.args.mode]
        super()._run_workflow()

    def _check_job_outputs(self, pm, job_type, job_id):
        if job_type == 'emw-import-movies':
            with StarFile(pm.join(job_id, 'movies.star')) as sf:
                self._movies = sf.getTableSize('movies')
            print(f"Imported movies: {Color.bold(self._movies)}")
            self.assertGreater(self._movies, 0, "No movies were imported")

        elif job_type == 'emw-preprocessing':
            self._check_motioncor(pm, job_id)
            if self.args.mode == '2d':
                with StarFile(pm.join(job_id, 'particles.star')) as sf:
                    self._particles = sf.getTableSize('particles')
                print(f"Extracted particles: {Color.bold(self._particles)}")
                self.assertGreater(self._particles, 0, "No particles were extracted")

        elif job_type == 'emw-rln2d':
            self._check_classes2d(pm, job_id)

    def _check_motioncor(self, pm, job_id):
        """ Check that every movie was motion corrected, with the binned
        pixel size, and that MotionCor commands were logged without errors. """
        with StarFile(pm.join(job_id, 'micrographs.star')) as sf:
            micrographs = sf.getTable('micrographs')
            optics = sf.getTable('optics')

        self.assertEqual(len(micrographs), self._movies,
                         "Not all movies were motion corrected")
        for row in micrographs:
            self.assertTrue(os.path.exists(pm.join(row.rlnMicrographName)),
                            f"Missing micrograph: {row.rlnMicrographName}")

        ps = float(self.acquisition['acq.pixel_size']) * self.motioncor_bin
        self.assertAlmostEqual(float(optics[0].rlnMicrographPixelSize), ps, places=3)

        # Batch logs with the MotionCor commands and their exit codes
        logs = glob(pm.join(job_id, 'Logs', '*_batch.log'))
        self.assertTrue(logs, f"No batch logs found in {job_id}/Logs")
        for log in logs:
            exit_codes = self._motioncor_exit_codes(log)
            self.assertTrue(exit_codes, f"No MotionCor command logged in {log}")
            self.assertFalse(any(exit_codes),
                             f"MotionCor failed in {log} (exit codes: {exit_codes})")

        print(f"Motion corrected micrographs: {Color.bold(len(micrographs))}, "
              f"pixel size: {Color.bold(ps)} Å/px, batch logs: {len(logs)}")

    def _check_classes2d(self, pm, job_id):
        """ Check that the 2D classes of all batches were registered as a
        single MultiClasses2D output (no Classes2D or Particles outputs),
        with all particles from the preprocessing. """
        nodes = pm._data._relionOutputNodes(job_id)
        types = [label.split('.')[-1] for label in nodes.values()]
        self.assertEqual(types, ['MultiClasses2D'],
                         f"Unexpected 2D classification outputs: {nodes}")

        multiStar = pm.join(job_id, 'classes2d.star')
        info = pm._data._computeOutputTypeInfo(pm._data._outputId(multiStar, job_id), None)
        print(f"{os.path.relpath(multiStar, pm.path)}: {info}")
        self.assertEqual(info['type'], 'MultiClasses2D')

        with StarFile(multiStar) as sf:
            batches = sf.getTable('classes2d')
        self.assertTrue(len(batches), f"No 2D classification batches in {multiStar}")

        classified = 0
        for row in batches:
            for fn in [row.optimiserStar, row.modelStar, row.dataStar, row.classesStack]:
                self.assertTrue(os.path.exists(pm.join(fn)), f"Missing file: {fn}")
            with StarFile(pm.join(row.dataStar)) as sf:
                classified += sf.getTableSize('particles')

        self.assertEqual(classified, self._particles,
                         "Not all particles were classified")

    @staticmethod
    def _motioncor_exit_codes(log):
        """ Exit codes of the MotionCor commands in a batch log, where each
        command line ('cd ... && <launcher> <args>') is followed by the
        program output and an '# Exit code: N' line. """
        exit_codes = []
        in_motioncor = False
        with open(log) as f:
            for line in f:
                if line.startswith('cd '):
                    in_motioncor = re.search('motioncor', line, re.I) is not None
                elif in_motioncor and (m := re.match(r'# Exit code: (\d+)', line)):
                    exit_codes.append(int(m.group(1)))
                    in_motioncor = False
        return exit_codes


if __name__ == '__main__':
    TestSpaBetagal.run_tests()
