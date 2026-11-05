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

import json
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
        id_map = pm.loadWorkflow(workflow=WORKFLOW, external_inputs='keep')
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

    def test_nested_job_reference(self):
        """ Job references inside lists of dicts (e.g. M 'sources') are
        remapped, or cleared when external, without changing the input. """
        workflow = {'jobs': [
            {'jobid': 'Export/job006', 'jobtype': 'Export', 'params': {}},
            {'jobid': 'Population/job022', 'jobtype': 'Population',
             'params': {'sources': [
                 {'name': 'A', 'warp_folder': 'Export/job006'},
                 {'name': 'B', 'warp_folder': 'Other/job003/sub'}],
                 'gpus': '1'}},
        ]}
        pm = _fake_pm()
        cleared = []
        id_map = pm.loadWorkflow(workflow=workflow, external_inputs='clear',
                                 cleared=cleared)
        self.assertEqual(id_map, {'Export/job006': 'Export/job100',
                                  'Population/job022': 'Population/job101'})
        params = pm.saved[1][1]
        self.assertEqual(params['sources'],
                         [{'name': 'A', 'warp_folder': 'Export/job100'},
                          {'name': 'B', 'warp_folder': ''}])
        self.assertEqual(params['gpus'], '1')
        self.assertEqual(cleared, [{'jobid': 'Population/job022',
                                    'param': 'sources',
                                    'value': 'Other/job003/sub'}])
        sources = workflow['jobs'][1]['params']['sources']
        self.assertEqual(sources[0]['warp_folder'], 'Export/job006')
        self.assertEqual(sources[1]['warp_folder'], 'Other/job003/sub')

    def test_nested_reference_order(self):
        """ A nested reference is a dependency: the job is created after
        its parent, even when listed before it. """
        workflow = {'jobs': [
            {'jobid': 'Population/job022', 'jobtype': 'Population',
             'params': {'sources': [{'warp_folder': 'Export/job006'}]}},
            {'jobid': 'Export/job006', 'jobtype': 'Export', 'params': {}},
        ]}
        pm = _fake_pm()
        id_map = pm.loadWorkflow(workflow=workflow)
        self.assertEqual(id_map['Export/job006'], 'Export/job100')
        self.assertEqual(pm.saved[1][1]['sources'],
                         [{'warp_folder': 'Export/job100'}])

    def test_apof_tutorial_part1(self):
        """ The Warp ApoF tutorial part 1: the M population source points
        at the new export particles job. """
        import os
        here = os.path.dirname(os.path.abspath(__file__))
        wf_file = os.path.join(here, '..', '..', 'config', 'workflows',
                               'apof-warp-tutorial-part1.json')
        pm = _fake_pm()
        id_map = pm.loadWorkflow(workflow_file=wf_file)
        saved = dict(pm.saved)
        pop = saved[id_map['WarpMCreatePopulation/job022']]
        self.assertEqual(pop['sources'][0]['warp_folder'],
                         id_map['WarpExportParticles/job006'])

    def test_clear_by_default(self):
        """ Loading a workflow clears the external references by default,
        their ids come from the project where it was made. """
        pm = _fake_pm()
        cleared = []
        pm.loadWorkflow(workflow=WORKFLOW, cleared=cleared)
        ctf, picking = [p for _, p in pm.saved]
        self.assertEqual(ctf['input_star'], '')
        self.assertEqual(picking['model'], '')
        self.assertEqual(picking['input_star'], 'Ctf/job100/micrographs_ctf.star')
        self.assertEqual(len(cleared), 2)

    def test_duplicate_keeps_external_inputs(self):
        """ Duplicating jobs in the same project keeps their inputs from
        jobs that are not duplicated. """
        pm = _fake_pm()
        jobDict = {'Picking/job007': {
            'jobtype': 'Picking', 'parents': set(), 'children': set(),
            'params': {'input_star': 'Ctf/job004/micrographs_ctf.star'}}}
        pm._instanciateJobs(jobDict)
        self.assertEqual(pm.saved[0][1]['input_star'],
                         'Ctf/job004/micrographs_ctf.star')

    def test_export_clears_external_inputs(self):
        """ Exported jobs only keep the references between them, also the
        nested ones, the others are cleared and reported. """
        jobs = {
            'Ctf/job004': ('Ctf', {'input_star': 'Import/job002/movies.star',
                                   'box': '256'}),
            'Population/job011': ('Population', {
                'sources': [{'name': 'A', 'warp_folder': 'Ctf/job004'},
                            {'name': 'B', 'warp_folder': 'Export/job003'}],
                'mask': 'External/job010/mask.mrc'}),
        }

        class _Job(dict):
            def __init__(self, jobId, jobType):
                super().__init__(jobtype=jobType)
                self.id = jobId

        pm = _fake_pm()
        pm._getJob = lambda jobId: _Job(jobId, jobs[jobId][0])
        pm._readJobParams = lambda job: jobs[job.id][1]
        cleared = []
        wf = pm.getWorkflowJobs(['Ctf/job004', 'Population/job011/'],
                                clear_external=True, cleared=cleared)
        ctf, pop = [j['params'] for j in wf['jobs']]
        self.assertEqual(ctf, {'input_star': '', 'box': '256'})
        self.assertEqual(pop['sources'],
                         [{'name': 'A', 'warp_folder': 'Ctf/job004'},
                          {'name': 'B', 'warp_folder': ''}])
        self.assertEqual(pop['mask'], '')
        self.assertEqual(sorted(c['value'] for c in cleared),
                         ['Export/job003', 'External/job010/mask.mrc',
                          'Import/job002/movies.star'])
        # Copying jobs (e.g. to paste them) keeps the references
        wf = pm.getWorkflowJobs(['Ctf/job004'])
        self.assertEqual(wf['jobs'][0]['params']['input_star'],
                         'Import/job002/movies.star')

    def test_templates_have_no_external_refs(self):
        """ The workflow templates only reference their own jobs. """
        import glob
        import os
        from emwrap.base.job_form import iter_param_strings
        here = os.path.dirname(os.path.abspath(__file__))
        wf_dir = os.path.join(here, '..', '..', 'config', 'workflows')
        pm = _fake_pm()
        for wf_file in glob.glob(os.path.join(wf_dir, '**', '*.json'),
                                 recursive=True):
            with open(wf_file) as f:
                workflow = json.load(f)
            ids = {j['jobid'].rstrip('/') for j in workflow.get('jobs', [])}
            for j in workflow.get('jobs', []):
                for v in iter_param_strings(j.get('params', {})):
                    with self.subTest(file=os.path.basename(wf_file),
                                      job=j['jobid'], value=v):
                        self.assertIsNone(pm._external_job_ref(v, ids))

    def test_invalid_option(self):
        with self.assertRaises(Exception):
            _fake_pm().loadWorkflow(workflow=WORKFLOW, external_inputs='drop')


if __name__ == '__main__':
    unittest.main()
