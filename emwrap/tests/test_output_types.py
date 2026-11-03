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

""" Datatype of MRC outputs from the Relion node types of their job
(no datasets needed):

    python -m unittest emwrap.tests.test_output_types
"""

import os
import shutil
import tempfile
import unittest

import mrcfile
import numpy as np

from emtools.utils import FolderManager
from emtools.metadata import StarFile, Table

from emwrap.base.project_data import ProjectData


class _Data(dict):
    def __init__(self, parent, **kwargs):
        super().__init__(**kwargs)
        self.parent = parent


class _Job(dict):
    def __init__(self, job_id, jobtype):
        super().__init__(jobtype=jobtype, status='Succeeded')
        self.id = job_id


class _Workflow:
    """ The outputs of the jobs, as ProjectData reads them from the workflow. """
    def __init__(self, outputs, nodetypes=None):
        self._outputs = outputs  # {output_id: job}
        self._nodetypes = nodetypes or {}  # {output_id: pipeline node type}

    def hasData(self, output_id):
        return output_id in self._outputs

    def getData(self, output_id):
        nodetype = self._nodetypes.get(output_id)
        return _Data(self._outputs[output_id], **({'nodetype': nodetype} if nodetype else {}))


class TestOutputTypes(unittest.TestCase):

    def setUp(self):
        self.path = tempfile.mkdtemp(prefix='emwrap_output_types_')
        self.data = ProjectData.__new__(ProjectData)
        FolderManager.__init__(self.data, self.path)
        self.data._debug = lambda *args: None

    def tearDown(self):
        shutil.rmtree(self.path, ignore_errors=True)

    def _job(self, job_id, jobtype, nodes, nodesStar=True):
        """ A job folder with its volumes and RELION_OUTPUT_NODES.star
        (without it, as older runs, the types are only in the pipeline). """
        os.makedirs(self.data.join(job_id))
        t = Table(columns=['rlnPipeLineNodeName', 'rlnPipeLineNodeTypeLabel'])
        for name, label in nodes:
            with mrcfile.new(self.data.join(job_id, name)) as mrc:
                mrc.set_data(np.zeros((8, 8, 8), dtype=np.float32))
            t.addRowValues(f'{job_id}/{name}', label)
        if nodesStar:
            with StarFile(self.data.join(job_id, 'RELION_OUTPUT_NODES.star'), 'w') as sf:
                sf.writeTable('pipeline_nodes', t)
        return _Job(job_id, jobtype)

    def test_masks(self):
        pytom = self._job('PyTom/job008', 'emw-pytom-create_template', [
            ('emd_template.mrc', 'TomogramGroupMetadata.star.relion.volume'),
            ('emd_template_mask.mrc', 'TomogramGroupMetadata.star.relion.mask3d')])
        relion = self._job('MaskCreate/job011', 'emw-relion-mask_create', [
            ('mask.mrc', 'TomogramGroupMetadata.star.relion.mask3d')])
        outputs = {'PyTom/job008/emd_template.mrc': pytom,
                   'PyTom/job008/emd_template_mask.mrc': pytom,
                   'MaskCreate/job011/mask.mrc': relion}
        self.data._wf = _Workflow(outputs)

        types = {o: self.data._computeOutputTypeInfo(o, [])['type'] for o in outputs}
        self.assertEqual(types, {'PyTom/job008/emd_template.mrc': 'Volume',
                                 'PyTom/job008/emd_template_mask.mrc': 'VolumeMask',
                                 'MaskCreate/job011/mask.mrc': 'VolumeMask'})

        # An older run: no RELION_OUTPUT_NODES.star, the node types of the
        # pipeline (<name>.mrc and <name>_mask.mrc)
        volumeLabel = 'TomogramGroupMetadata.star.relion.volume'
        maskLabel = 'TomogramGroupMetadata.star.relion.mask3d'
        old = self._job('External/job005', 'emw-pytom-create_template', [
            ('emd.mrc', volumeLabel), ('emd_mask.mrc', maskLabel)], nodesStar=False)
        oldOutputs = {'External/job005/emd.mrc': old, 'External/job005/emd_mask.mrc': old}
        self.data._wf = _Workflow({**outputs, **oldOutputs}, {
            'External/job005/emd.mrc': volumeLabel,
            'External/job005/emd_mask.mrc': maskLabel})
        types = {o: self.data._computeOutputTypeInfo(o, [])['type'] for o in oldOutputs}
        self.assertEqual(types, {'External/job005/emd.mrc': 'Volume',
                                 'External/job005/emd_mask.mrc': 'VolumeMask'})

        # A mask cached as a Volume (before) is computed again
        self.assertTrue(self.data._isStaleMaskInfo(
            'MaskCreate/job011/mask.mrc', relion, {'type': 'Volume'}))
        self.assertFalse(self.data._isStaleMaskInfo(
            'PyTom/job008/emd_template.mrc', pytom, {'type': 'Volume'}))


if __name__ == '__main__':
    unittest.main()
