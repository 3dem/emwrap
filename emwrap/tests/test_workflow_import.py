# **************************************************************************
# *
# * Authors:     J.M. de la Rosa Trevin (delarosatrevin@gmail.com)
# *
# * This program is free software; you can redistribute it and/or modify
# * it under the terms of the GNU General Public License as published by
# * the Free Software Foundation; either version 3 of the License, or
# * (at your option) any later version.
# *
# **************************************************************************

""" Unit tests for loading a workflow (job list) into a project, as when
pasting jobs copied from another project (no datasets needed):

    python -m unittest emwrap.tests.test_workflow_import
"""

import unittest

from emwrap.base.project_manager import ProjectManager


class _FakeJob:
    def __init__(self, jobId):
        self.id = jobId


def _fake_pm(path='/data/projects/target'):
    """ A ProjectManager whose saveJob only records the saved jobs and
    gives them new ids, starting at job100. """
    pm = ProjectManager.__new__(ProjectManager)
    pm.saved = []
    pm.relpath = lambda p: p[len(path) + 1:] if p.startswith(path + '/') else '../' + p

    def saveJob(jobType, params):
        jobId = f"{jobType}/job{100 + len(pm.saved)}"
        pm.saved.append((jobId, params))
        return _FakeJob(jobId)

    pm.saveJob = saveJob
    return pm


# Jobs copied from a source project: Ctf and Picking, whose input is an
# Import job that was not copied (external)
WORKFLOW = {'jobs': [
    {'jobid': 'Ctf/job004', 'jobtype': 'Ctf',
     'params': {'input_star': 'Import/job002/movies.star', 'box': '256',
                'data': 'data/*.tiff'}},
    {'jobid': 'Picking/job007/', 'jobtype': 'Picking',
     'params': {'input_star': 'Ctf/job004/micrographs_ctf.star',
                'model': 'Import/job002'}},
]}


class TestWorkflowImport(unittest.TestCase):

    def test_keep_external_inputs(self):
        pm = _fake_pm()
        id_map = pm.loadWorkflow(workflow=WORKFLOW)
        self.assertEqual(id_map, {'Ctf/job004': 'Ctf/job100',
                                  'Picking/job007': 'Picking/job101'})
        ctf, picking = [p for _, p in pm.saved]
        self.assertEqual(ctf['input_star'], 'Import/job002/movies.star')
        self.assertEqual(picking['input_star'], 'Ctf/job100/micrographs_ctf.star')
        self.assertEqual(picking['model'], 'Import/job002')

    def test_clear_external_inputs(self):
        pm = _fake_pm()
        cleared = []
        id_map = pm.loadWorkflow(workflow=WORKFLOW, external_inputs='clear',
                                 cleared=cleared)
        self.assertEqual(len(id_map), 2)
        ctf, picking = [p for _, p in pm.saved]
        # External job references are cleared, other values untouched
        self.assertEqual(ctf['input_star'], '')
        self.assertEqual(ctf['box'], '256')
        self.assertEqual(ctf['data'], 'data/*.tiff')
        self.assertEqual(picking['model'], '')
        # Links between the copied jobs are remapped
        self.assertEqual(picking['input_star'], 'Ctf/job100/micrographs_ctf.star')
        self.assertEqual(sorted((c['jobid'], c['param']) for c in cleared),
                         [('Ctf/job004', 'input_star'),
                          ('Picking/job007', 'model')])
        # The given workflow is not modified
        self.assertEqual(WORKFLOW['jobs'][0]['params']['input_star'],
                         'Import/job002/movies.star')

    def test_absolute_job_reference(self):
        pm = _fake_pm()
        cleared = []
        workflow = {'jobs': [
            {'jobid': 'Ctf/job004', 'jobtype': 'Ctf',
             'params': {'input_star': '/data/projects/target/Import/job002/movies.star',
                        'other': '/data/projects/source/Import/job002/movies.star'}}]}
        pm.loadWorkflow(workflow=workflow, external_inputs='clear', cleared=cleared)
        params = pm.saved[0][1]
        self.assertEqual(params['input_star'], '')
        # Outside the project: not a job of this project, kept
        self.assertEqual(params['other'], '/data/projects/source/Import/job002/movies.star')

    def test_invalid_option(self):
        with self.assertRaises(Exception):
            _fake_pm().loadWorkflow(workflow=WORKFLOW, external_inputs='drop')


if __name__ == '__main__':
    unittest.main()
