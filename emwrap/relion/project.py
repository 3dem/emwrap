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

from emtools.utils import FolderManager
from emtools.jobs import BatchManager, Workflow
from emtools.metadata import Table, StarFile, StarMonitor

from emwrap.base import Acquisition


class RelionProject(FolderManager):
    """ Class to manipulate information about a Relion project. """

    def __init__(self, path):
        FolderManager.__init__(self, path)
        self._wf =

    def _loadPipeline(self):
        """ Load pipeline Graph from the default_pipeline.star file. """
        protList = []
        status_map = {
            'Succeeded': 'finished',
            'Running': 'running',
            'Aborted': 'aborted',
            'Failed': 'failed'
        }
        pipelineStar = self.join('default_pipeline.star')

        wf = Workflow.fromRelionPipeline(pipelineStar)

        for job in wf.jobs():
            a = job['alias']
            protList.append({
                'id': job.id,
                'label': j.id if (a is None or a == 'None') else a,
                'status': status_map.get(job['status'], 'unknown'),
                'type': job['type'],
                'links': [o.parent.id for o in job.outputs]
            })

        return protList
