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
import shlex
import json
import subprocess
import argparse

from emtools.utils import FolderManager, Process, Color, Path
from emtools.jobs import BatchManager, Workflow
from emtools.metadata import RelionStar

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

    def _saveCmd(self, cmd, jobId):
        """ Write command.txt file to be used for restart. """
        with open(self.join(jobId, 'command.txt'), 'w') as f:
            f.write(f"{cmd}\n")

    def _loadCmd(self, jobId):
        with open(self.join(jobId, 'command.txt')) as f:
            return f.readline().strip()

    def getJobTypeFolder(self, jobtype):
        """ Return the job folder depending on the job type. """
        return 'External'

    def _runCmd(self, cmd, jobId, wait=False):
        self._saveCmd(cmd, jobId)
        args = shlex.split(cmd)
        stdout = open(self.join(jobId, 'run.out'), 'a')
        stderr = open(self.join(jobId, 'run.err'), 'a')
        cmd = self.log(f"{Color.green(args[0])} {Color.bold(' '.join(args[1:]))}")
        stdout.write(f"\n\n{cmd}\n")
        stdout.flush()

        # Run the command
        p = subprocess.Popen(args, stdout=stdout, stderr=stderr, close_fds=True)

        # Update the Pipeline with new job
        RelionStar.workflow_to_pipeline(self._wf, self.pipeline_star)

        if wait:
            p.wait()

    def run(self, cmd, jobtype, wait=False):
        jobtypeFolder = self.getJobTypeFolder(jobtype)
        jobIndex = self._wf.jobNextIndex
        jobId = f'{jobtypeFolder}/job{jobIndex:03}'
        self.mkdir(jobId)
        job = self._wf.registerJob(jobId, status='Launched',
                                   alias='None', jobtype=jobtype)
        cmd += f" --output {jobId}"
        self._runCmd(cmd, jobId, wait=wait)

    def restart(self, jobId, clean=False, wait=False):
        if not self._wf.hasJob(jobId):
            raise Exception(f"There is not job with id: '{jobId}'")

        if not self.exists(jobId):
            raise Exception(f"Missing folder for job: '{jobId}'")

        job = self._wf.getJob(jobId)
        job['status'] = 'Launched'
        cmd = self._loadCmd(jobId)
        if clean:
            FolderManager(self.join(jobId)).create()

        self._runCmd(cmd, jobId, wait=wait)

    def loadJobInfo(self, job):
        """ Load the info.json file for a given run. """
        jobInfoFn = self.join(job.id, 'info.json')
        if os.path.exists(jobInfoFn):
            with open(jobInfoFn) as f:
                return json.load(f)
        return None

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

                if jobInfo := self.loadJobInfo(job):
                    for o in jobInfo['outputs']:
                        for fn, datatype in o['files']:
                            if not job.hasOutput(fn):
                                job.registerOutput(fn, datatype=datatype)
                                update = True

        if update:
            self.log(f"Updating {self.pipeline_star}")
            RelionStar.workflow_to_pipeline(self._wf, self.pipeline_star)


def main():
    p = argparse.ArgumentParser()
    p.add_argument('path', metavar="PROJECT_PATH",
                   help="Project path", default='.', nargs='?')
    g = p.add_mutually_exclusive_group()
    g.add_argument('--clean', '-c', action='store_true',
                   help="Clean project files")
    g.add_argument('--update', '-u', action='store_true',
                   help="Update job status and pipeline star file.")
    g.add_argument('--run', '-r', nargs='+',
                   metavar=('COMMAND_OR_FOLDER', 'RUN_MODE'),
                   help="Input a command (new job) or an existing job folder. "
                        "In the case of the existing job folder, the special "
                        "'clean' mode can be passed to delete previous run "
                        "data.")
    p.add_argument('-k', '--check', action='count', default=0,
                   help='Check and/or kill processes related to this project.'
                        'Pass more than one -k to kill processes.')

    p.add_argument('--wait', '-w', action='store_true',
                   help="Works with --run and make the project waits for "
                        "the sub-process to complete. Useful for scripting "
                        "and benchmarking.")

    args = p.parse_args()

    rlnProject = RelionProject(args.path)

    if args.clean:
        rlnProject.clean()
    elif args.update:
        rlnProject.update()
    elif args.run:
        first = args.run[0]
        if os.path.exists(first):
            mode = args.run[1] if len(args.run) > 1 else None
            # Make sure we get the jobId without ending slash
            jobId = Path.rmslash(first)
            clean = mode == 'clean'
            rlnProject.restart(jobId, clean=clean, wait=args.wait)
        else:
            jobType = first.split()[0]
            rlnProject.run(first, jobType, wait=args.wait)
    elif args.check > 0:
        kill = args.check > 1
        folderPath = os.path.abspath(args.path)
        Process.checkChilds('emw', folderPath, kill=kill, verbose=True)

