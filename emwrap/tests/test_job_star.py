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

""" Unit tests for the job.star backup of Relion native jobs, kept while
relion_pipeliner runs them (no Relion, datasets or cluster needed):

    python -m unittest emwrap.tests.test_job_star
"""

import os
import shutil
import tempfile
import unittest
import unittest.mock

from emtools.metadata import RelionStar
from emtools.utils import FolderManager

from emwrap.base.config import ProcessingConfig
from emwrap.base.project_manager import ProjectManager
from emwrap.base import job_star
from emwrap.base.job_star import (JOB_STAR, JOB_STAR_BACKUP, job_star_path,
                                  read_job_params, backup_job_star,
                                  restore_job_star)
from emwrap.relion import native
from emwrap.relion.native import RelionNative

JOB_TYPE = 'relion.ctffind.tomo'
EMWRAP_VALUES = {
    'input_star_mics': 'TiltSeries/job003/tilt_series.star',
    'queue.name': 'cryoem_rtx',
    'queue.param.cryoem_rtx.mem_per_core': '7000',
}


def _pipeliner_rewrite(job_star):
    """ What relion_pipeliner does with job.star: rewrites it without the
    options it does not know (queue.*). """
    RelionStar.write_jobstar(JOB_TYPE,
                             {'input_star_mics': EMWRAP_VALUES['input_star_mics']},
                             job_star, isTomo=1)


class _Native(RelionNative):
    """ RelionNative without a project: only prerun's job.star handling. """
    def __init__(self, job_dir):
        self.workingDir = os.path.dirname(os.path.dirname(job_dir))
        self.outputDir = job_dir
        self.seen = None  # params read while relion_pipeliner "runs"
        self.interrupt = False

    def join(self, *p):
        return os.path.join(self.outputDir, *p)

    def updateBatchInfo(self, batch):
        pass

    def batch_execute(self, label, batch, args, **kwargs):
        if label == 'pipeliner':
            _pipeliner_rewrite(self.join(JOB_STAR))
            self.seen = read_job_params(self.outputDir)
            if self.interrupt:
                raise SystemExit(0)  # e.g. SIGTERM, see ProcessingPipeline
        return 0


class TestJobStarBackup(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix='emwrap_jobstar_')
        self.jobDir = os.path.join(self.tmp, 'TiltSeries', 'job005')
        os.makedirs(self.jobDir)
        self.jobStar = os.path.join(self.jobDir, JOB_STAR)
        self.backup = os.path.join(self.jobDir, JOB_STAR_BACKUP)
        RelionStar.write_jobstar(JOB_TYPE, EMWRAP_VALUES, self.jobStar, isTomo=1)

        patcher = unittest.mock.patch.object(native, 'wait_project_lock_free')
        patcher.start()
        self.addCleanup(patcher.stop)

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def _queue(self, path=None):
        return RelionStar.read_jobstar(path or self.jobStar).get('queue.name')

    def test_restored_after_run(self):
        job = _Native(self.jobDir)
        job.prerun()
        # While relion_pipeliner ran, the params came from the backup
        self.assertEqual(job.seen['queue.name'], 'cryoem_rtx')
        self.assertEqual(self._queue(), 'cryoem_rtx')
        self.assertFalse(os.path.exists(self.backup))

    def test_restored_when_interrupted(self):
        job = _Native(self.jobDir)
        job.interrupt = True
        with self.assertRaises(SystemExit):
            job.prerun()
        self.assertEqual(self._queue(), 'cryoem_rtx')
        self.assertFalse(os.path.exists(self.backup))

    def test_killed_run_backup(self):
        """ A run killed before restoring (e.g. SIGKILL) leaves the stripped
        job.star and the backup: readers use the backup and the next run
        does not overwrite it with the stripped job.star. """
        backup_job_star(self.jobDir)
        _pipeliner_rewrite(self.jobStar)
        self.assertIsNone(self._queue())
        self.assertEqual(job_star_path(self.jobDir), self.backup)
        self.assertEqual(self._queue(job_star_path(self.jobDir)), 'cryoem_rtx')

        job = _Native(self.jobDir)
        job.prerun()
        self.assertEqual(job.seen['queue.name'], 'cryoem_rtx')
        self.assertEqual(self._queue(), 'cryoem_rtx')

    def test_restore_without_backup(self):
        restore_job_star(self.jobDir)
        self.assertEqual(self._queue(), 'cryoem_rtx')
        self.assertEqual(job_star_path(self.jobDir), self.jobStar)

    def test_restored_while_reading(self):
        """ The run restores the backup between finding it and reading it
        (e.g. EMhub updating the project while the job ends). """
        backup_job_star(self.jobDir)
        _pipeliner_rewrite(self.jobStar)
        read_jobstar = RelionStar.read_jobstar

        def _read(path):
            if path == self.backup:
                restore_job_star(self.jobDir)
            return read_jobstar(path)

        with unittest.mock.patch.object(job_star.RelionStar, 'read_jobstar',
                                        side_effect=_read):
            params = read_job_params(self.jobDir)
        self.assertEqual(params['queue.name'], 'cryoem_rtx')
        self.assertFalse(os.path.exists(self.backup))

    def test_no_temporary_files(self):
        backup_job_star(self.jobDir)
        restore_job_star(self.jobDir)
        self.assertEqual(os.listdir(self.jobDir), [JOB_STAR])

    def test_project_manager(self):
        """ ProjectManager reads the params from the backup (e.g. the queue
        to stop a running job) and writing job.star drops an old backup. """
        pm = ProjectManager.__new__(ProjectManager)
        FolderManager.__init__(pm, self.tmp)
        pm._verbose = 0
        job = unittest.mock.MagicMock(id='TiltSeries/job005')
        job.__getitem__.side_effect = {'jobtype': JOB_TYPE}.__getitem__

        backup_job_star(self.jobDir)
        _pipeliner_rewrite(self.jobStar)
        with unittest.mock.patch.object(ProcessingConfig, 'get_job_form',
                                        return_value=None), \
             unittest.mock.patch.object(ProcessingConfig, 'get_job_conf',
                                        return_value={'tomo': True}), \
             unittest.mock.patch('emwrap.base.project_manager.JobForm.get_values',
                                 return_value={}):
            params = pm._readJobParams(job)
            self.assertEqual(params['queue.name'], 'cryoem_rtx')

            params['queue.name'] = 'cryoem'
            pm._writeJobStarFile(JOB_TYPE, params, self.jobStar)

        self.assertFalse(os.path.exists(self.backup))
        self.assertEqual(self._queue(), 'cryoem')


if __name__ == '__main__':
    unittest.main()
