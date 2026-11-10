"""
Outputs of Relion Refine3D jobs: the refined volume first, without the
optimiser (Classes3D) and, in tomography, without the data STAR file
(Particles, already in the TomoParticles of the optimisation set), and
the pixel size in the info of the TomoParticles.
"""

import os
import tempfile
import unittest

from emwrap.base.project_data import ProjectData


OPTICS = """
data_optics

loop_
_rlnOpticsGroup #1
_rlnOpticsGroupName #2
_rlnTomoTiltSeriesPixelSize #3
_rlnImagePixelSize #4
1 optics1 1.350 5.400

"""

PARTICLES = """
data_particles

loop_
_rlnTomoName #1
_rlnTomoParticleName #2
_rlnOpticsGroup #3
TS_01 TS_01/1 1
TS_01 TS_01/2 1
TS_02 TS_02/1 1

"""

TOMOGRAMS = """
data_global

loop_
_rlnTomoName #1
_rlnTomoTiltSeriesPixelSize #2
TS_01 1.350
TS_02 1.350

"""

OPTSET = """
data_

_rlnTomoParticlesFile               {job}/run_data.star
_rlnTomoTomogramsFile               Tomograms/job001/tomograms.star
"""


class _Job(dict):
    def __init__(self, job_id, jobtype):
        super().__init__(jobtype=jobtype)
        self.id = job_id


class TestRefine3dOutputs(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = self.tmp.name
        self.pd = ProjectData.__new__(ProjectData)
        self.pd.join = lambda *p: os.path.join(self.root, *p)
        self.pd._debug = lambda *a, **kw: None

    def tearDown(self):
        self.tmp.cleanup()

    def _write(self, path, text=''):
        fn = os.path.join(self.root, path)
        os.makedirs(os.path.dirname(fn), exist_ok=True)
        with open(fn, 'w') as f:
            f.write(text)

    def _jobFiles(self, job, names):
        for name in names:
            text = OPTSET.format(job=job) if name.endswith('optimisation_set.star') else ''
            self._write(os.path.join(job, name), text)

    def _outputs(self, job_id, jobtype):
        return [os.path.basename(o) for o in
                self.pd._relionIterOutputs(job_id, _Job(job_id, jobtype))]

    def test_tomo_iteration(self):
        job = 'Refine3D/job061'
        self._jobFiles(job, ['run_it013_optimiser.star', 'run_it013_optimisation_set.star',
                             'run_it013_data.star', 'run_it013_half1_class001.mrc',
                             'run_it014_data.star'])  # iteration not completed
        self.assertEqual(self._outputs(job, 'relion.refine3d.tomo'),
                         ['run_it013_half1_class001.mrc',
                          'run_it013_optimisation_set.star'])

    def test_tomo_final(self):
        job = 'Refine3D/job058'
        self._jobFiles(job, ['run_it021_optimiser.star', 'run_it021_data.star',
                             'run_optimiser.star', 'run_optimisation_set.star',
                             'run_data.star', 'run_class001.mrc',
                             'run_half1_class001_unfil.mrc'])
        self.assertEqual(self._outputs(job, 'relion.refine3d.tomo'),
                         ['run_class001.mrc', 'run_half1_class001_unfil.mrc',
                          'run_optimisation_set.star'])

    def test_spa_final(self):
        """ Without optimisation set the data STAR file has the particles. """
        job = 'Refine3D/job010'
        self._jobFiles(job, ['run_optimiser.star', 'run_data.star',
                             'run_class001.mrc', 'run_half1_class001_unfil.mrc'])
        self.assertEqual(self._outputs(job, 'relion.refine3d'),
                         ['run_class001.mrc', 'run_half1_class001_unfil.mrc',
                          'run_data.star'])

    def test_class3d_unchanged(self):
        job = 'Class3D/job020'
        self._jobFiles(job, ['run_it025_optimiser.star', 'run_it025_optimisation_set.star',
                             'run_it025_data.star', 'run_it025_class001.mrc'])
        self.assertEqual(self._outputs(job, 'relion.class3d.tomo'),
                         ['run_it025_optimiser.star', 'run_it025_optimisation_set.star',
                          'run_it025_data.star', 'run_it025_class001.mrc'])

    def test_tomo_particles_pixel_size(self):
        job = 'Refine3D/job058'
        self._write(f'{job}/run_optimisation_set.star', OPTSET.format(job=job))
        self._write(f'{job}/run_data.star', OPTICS + PARTICLES)
        self._write('Tomograms/job001/tomograms.star', TOMOGRAMS)
        output_id = f'{job}/run_optimisation_set.star'
        info = self.pd._computeOutputTypeInfo(output_id, [])
        self.assertEqual(info, {'type': 'TomoParticles',
                                'info': '3 items, 5.400 Å/px, Tomograms: 2'})


if __name__ == '__main__':
    unittest.main()
