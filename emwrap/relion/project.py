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
import argparse

from emtools.utils import FolderManager, Process
from emtools.jobs import BatchManager, Workflow
from emtools.metadata import Table, StarFile, StarMonitor

from .star import RelionStar

STATUS_LAUNCHED = 'Launched'
STATUS_RUNNING = 'Running'
STATUS_SUCCEEDED = 'Succeeded'
STATUS_FAILED = 'Failed'
STATUS_ABORTED = 'Aborted'

JOB_STATUS_FILES = {
    'RELION_JOB_RUNNING': STATUS_RUNNING,
    'RELION_JOB_EXIT_SUCCESS': STATUS_SUCCEEDED,
    'RELION_JOB_EXIT_FAILURE': STATUS_FAILED,
    'RELION_JOB_EXIT_ABORTED': STATUS_ABORTED
}


class RelionProject(FolderManager):
    """ Class to manipulate information about a Relion project. """

    def __init__(self, path):
        FolderManager.__init__(self, path)
        apath = os.path.abspath(path)

        if not self.exists():
            raise Exception(f"Project path '{apath}' does not exist")

        if self.exists(self.pipeline_star):
            self.log(f"Loading project from: {apath}")
            self._wf = RelionStar.pipeline_to_workflow(self.pipeline_star)
        else:
            # Create a new project
            self._wf = Workflow()
            self.__create()


    @property
    def pipeline_star(self):
        return self.join('default_pipeline.star')

    def __create(self):
        """ Create a new project in the given path. """
        if self.exists(self.pipeline_star):
            raise Exception(f"Can not create project, pipeline already exists: "
                            f"{self.pipeline_star}")

        self.log(f"Creating new project at: {os.path.abspath(self.path)}")

        with open(self.join('.gui_projectdir'), 'w'):
            pass

        RelionStar.write_pipeline(self.pipeline_star)

    def clean(self):
        """ Remove all project files. """
        for name in ['.gui_projectdir', '.TMP_runfiles', '.relion_lock',
                     'default_pipeline.star',
                     'Import', 'External']:
            if self.exists(name):
                Process.system(f"rm -rf '{self.join(name)}'", print=self.log)

        self.__create()

    def getJobTypeFolder(self, jobtype):
        """ Return the job folder depending on the job type. """
        return 'External'

    def run(self, jobtype, cmd):
        jobtypeFolder = self.getJobTypeFolder(jobtype)
        jobIndex = self._wf.jobNextIndex
        jobId = f'{jobtypeFolder}/job{jobIndex:03}'
        self.mkdir(jobId)
        job = self._wf.registerJob(jobId,
                                   status='Launched',
                                   alias='None',
                                   jobtype=jobtype)
        fullCmd = f"{cmd} --output {jobId} &"
        Process.system(fullCmd)
        # Update the Pipeline with new job
        RelionStar.workflow_to_pipeline(self._wf, self.pipeline_star)

    def update(self):
        """ Update status of the running jobs. """
        active = [STATUS_LAUNCHED, STATUS_RUNNING]
        update = False
        for job in self._wf.jobs():
            if job['status'] in active:  # Check job status
                for statusFile, status in JOB_STATUS_FILES.items():
                    if self.exists(job.id, statusFile):
                        job['status'] = status
                        update = True
        if update:
            RelionStar.workflow_to_pipeline(self._wf, self.pipeline_star)


