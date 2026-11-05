"""
Tests of emwrap.base.workflow_refs, the job references shared by
ProjectManager and the EMhub WorkflowEditor.
"""

import os
import unittest

from emwrap.base import workflow_refs as wr

PROJECT = '/data/proj'


def _relpath(p):
    return os.path.relpath(p, PROJECT)


class TestReferences(unittest.TestCase):

    def test_string_references_job(self):
        self.assertTrue(wr.string_references_job('Ctf/job004', 'Ctf/job004'))
        self.assertTrue(wr.string_references_job('Ctf/job004/a.star', 'Ctf/job004'))
        self.assertFalse(wr.string_references_job('Ctf/job0041', 'Ctf/job004'))
        self.assertFalse(wr.string_references_job('Ctf/job004x/a', 'Ctf/job004'))
        self.assertFalse(wr.string_references_job(256, 'Ctf/job004'))

    def test_absolute_paths(self):
        abs_ref = f'{PROJECT}/Ctf/job004/a.star'
        # Without relpath an absolute path never references a job
        self.assertFalse(wr.string_references_job(abs_ref, 'Ctf/job004'))
        self.assertTrue(wr.string_references_job(abs_ref, 'Ctf/job004', _relpath))
        self.assertFalse(wr.string_references_job('/other/Ctf/job004/a.star',
                                                  'Ctf/job004', _relpath))
        self.assertEqual(wr.job_ref(abs_ref, _relpath), 'Ctf/job004')
        self.assertIsNone(wr.job_ref('/other/Ctf/job004/a.star', _relpath))

    def test_nested(self):
        value = [{'name': 'A', 'warp_folder': 'Export/job006'}]
        self.assertTrue(wr.param_references_job(value, 'Export/job006'))
        self.assertEqual(wr.referenced_jobs(value, ['Export/job006', 'Ctf/job004']),
                         ['Export/job006'])

    def test_job_refs(self):
        params = {'input': 'Ctf/job004/a.star', 'box': '256', 'empty': '',
                  'sources': [{'warp_folder': 'Export/job006'}]}
        refs = list(wr.job_refs(params, {'Ctf/job004', 'Export/job006'}))
        self.assertEqual(refs, [('input', 'Ctf/job004/a.star', 'Ctf/job004'),
                                ('sources', 'Export/job006', 'Export/job006')])

    def test_job_ref_and_ids(self):
        self.assertEqual(wr.job_ref('Ctf/job004/a.star'), 'Ctf/job004')
        self.assertIsNone(wr.job_ref('256'))
        self.assertIsNone(wr.job_ref(''))
        self.assertIsNone(wr.job_ref('/data/movies/*.tif'))
        self.assertEqual(wr.normalize_id('Ctf/job004/'), 'Ctf/job004')
        self.assertEqual(wr.normalize_id('Ctf/job004'), 'Ctf/job004')


class TestClearAndRemap(unittest.TestCase):

    def test_clear_external(self):
        params = {'input': 'Import/job002/movies.star', 'box': '256',
                  'sources': [{'name': 'A', 'warp_folder': 'Ctf/job004'},
                              {'name': 'B', 'warp_folder': 'Export/job003'}]}
        cleared = []
        new = wr.clear_external(params, {'Ctf/job004'}, cleared, 'Pop/job011')
        self.assertEqual(new['input'], '')
        self.assertEqual(new['box'], '256')
        self.assertEqual(new['sources'][0]['warp_folder'], 'Ctf/job004')
        self.assertEqual(new['sources'][1], {'name': 'B', 'warp_folder': ''})
        self.assertEqual(params['input'], 'Import/job002/movies.star')
        self.assertEqual(sorted(c['value'] for c in cleared),
                         ['Export/job003', 'Import/job002/movies.star'])
        self.assertEqual({c['jobid'] for c in cleared}, {'Pop/job011'})

    def test_remap_longest_first(self):
        # Hypothetical ids where one is a path prefix of the other
        parents = {'A/job001', 'A/job001/B/job002'}
        id_map = {'A/job001': 'A/job010', 'A/job001/B/job002': 'X/job020'}
        self.assertEqual(wr.remap_string('A/job001/B/job002/f', id_map, parents),
                         'X/job020/f')
        self.assertEqual(wr.remap_string('A/job001/f', id_map, parents),
                         'A/job010/f')
        # A parent without new id is cleared, other strings are kept
        self.assertEqual(wr.remap_string('C/job003/f', {}, {'C/job003'}), '')
        self.assertEqual(wr.remap_string('D/job004/f', {}, {'C/job003'}),
                         'D/job004/f')

    def test_remap_absolute(self):
        v = f'{PROJECT}/Ctf/job004/a.star'
        self.assertEqual(
            wr.remap_string(v, {'Ctf/job004': 'Ctf/job010'}, {'Ctf/job004'},
                            _relpath),
            f'{PROJECT}/Ctf/job010/a.star')

    def test_remap_params(self):
        params = {'sources': [{'warp_folder': 'E/job006'}], 'n': 3}
        new = wr.remap_params(params, {'E/job006': 'E/job020'}, {'E/job006'})
        self.assertEqual(new, {'sources': [{'warp_folder': 'E/job020'}], 'n': 3})


def _jobs(*entries):
    return {jid: {'jobtype': jid.split('/')[0], 'params': params}
            for jid, params in entries}


class TestInstantiate(unittest.TestCase):

    def _creator(self):
        created = []

        def create(old_id, jobtype, params):
            new_id = f'{jobtype}/job{100 + len(created):03d}'
            created.append((old_id, new_id, params))
            return new_id
        return created, create

    def test_graph(self):
        jobs = _jobs(('A/job001', {}),
                     ('B/job002', {'i': 'A/job001/x', 'self': 'B/job002/y'}),
                     ('C/job003', {'t': [{'f': 'A/job001'}, {'f': 'B/job002'}]}))
        g = wr.build_graph(jobs)
        self.assertEqual(g['A/job001']['children'], {'B/job002', 'C/job003'})
        self.assertEqual(g['B/job002']['parents'], {'A/job001'})
        self.assertEqual(g['C/job003']['parents'], {'A/job001', 'B/job002'})

    def test_order_keeps_workflow_order(self):
        jobs = _jobs(('C/job003', {'i': 'B/job002'}),
                     ('Z/job009', {}),
                     ('B/job002', {'i': 'A/job001'}),
                     ('A/job001', {}))
        order = wr.instantiation_order(jobs, wr.build_graph(jobs))
        self.assertEqual(order, ['Z/job009', 'A/job001', 'B/job002', 'C/job003'])

    def test_cycle(self):
        jobs = _jobs(('A/job001', {'i': 'B/job002'}),
                     ('B/job002', {'i': 'A/job001'}))
        with self.assertRaises(Exception):
            wr.instantiate(jobs, lambda *a: 'x')

    def test_keep(self):
        jobs = _jobs(('A/job001', {'i': 'Ext/job050/a.star'}),
                     ('B/job002', {'i': 'A/job001/x',
                                   'sources': [{'f': 'A/job001'}]}))
        created, create = self._creator()
        id_map = wr.instantiate(jobs, create)
        self.assertEqual(id_map, {'A/job001': 'A/job100', 'B/job002': 'B/job101'})
        self.assertEqual(created[0][2], {'i': 'Ext/job050/a.star'})
        self.assertEqual(created[1][2], {'i': 'A/job100/x',
                                         'sources': [{'f': 'A/job100'}]})
        # The input is not modified
        self.assertEqual(jobs['B/job002']['params']['i'], 'A/job001/x')
        self.assertNotIn('parents', jobs['A/job001'])

    def test_clear(self):
        jobs = _jobs(('A/job001', {'i': 'Ext/job050/a.star', 'n': '5'}),
                     ('B/job002', {'i': 'A/job001/x'}))
        created, create = self._creator()
        cleared = []
        wr.instantiate(jobs, create, external_inputs='clear', cleared=cleared)
        self.assertEqual(created[0][2], {'i': '', 'n': '5'})
        self.assertEqual(created[1][2], {'i': 'A/job100/x'})
        self.assertEqual(cleared, [{'jobid': 'A/job001', 'param': 'i',
                                    'value': 'Ext/job050/a.star'}])
        self.assertEqual(jobs['A/job001']['params']['i'], 'Ext/job050/a.star')

    def test_absolute_refs(self):
        jobs = _jobs(('A/job001', {}),
                     ('B/job002', {'i': f'{PROJECT}/A/job001/x'}))
        created, create = self._creator()
        wr.instantiate(jobs, create, relpath=_relpath)
        self.assertEqual(created[1][2], {'i': f'{PROJECT}/A/job100/x'})

    def test_invalid_option(self):
        with self.assertRaises(Exception):
            wr.instantiate({}, lambda *a: 'x', external_inputs='drop')


if __name__ == '__main__':
    unittest.main()
