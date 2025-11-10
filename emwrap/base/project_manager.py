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
import sys
import shlex
import json
import subprocess
import argparse

from emtools.utils import FolderManager, Process, Color, Path
from emtools.jobs import BatchManager, Workflow
from emtools.metadata import Table, StarFile, RelionStar

from .config import ProcessingConfig


STATUS_LAUNCHED = 'Launched'
STATUS_RUNNING = 'Running'
STATUS_SUCCEEDED = 'Succeeded'
STATUS_FAILED = 'Failed'
STATUS_ABORTED = 'Aborted'
STATUS_SAVED = 'Saved'

JOB_STATUS_FILES = {
    'RELION_JOB_RUNNING': STATUS_RUNNING,
    'RELION_JOB_EXIT_SUCCESS': STATUS_SUCCEEDED,
    'RELION_JOB_EXIT_FAILURE': STATUS_FAILED,
    'RELION_JOB_EXIT_ABORTED': STATUS_ABORTED
}


class ProjectManager(FolderManager):
    """ Class to manipulate information about a Relion project. """

    def __init__(self, path, create=False):
        FolderManager.__init__(self, path)
        apath = os.path.abspath(path)

        if not self.exists():
            raise Exception(f"Project path '{apath}' does not exist")

        if self.exists(self.pipeline_star):
            self.log(f"Loading project from: {apath}")
            self._wf = RelionStar.pipeline_to_workflow(self.pipeline_star)
        elif create:
            # Create a new project
            self._wf = Workflow()
            self._create()
        else:
            raise Exception(f"'{self.pipeline_star} does not exist")

    @staticmethod
    def write_jobstar(params, jobStarFile):
        """ Convert params dict to a Relion job.star file. """
        pass

    @property
    def pipeline_star(self):
        return self.join('default_pipeline.star')

    def create(self):
        """ Create a new project. Existing files will be cleaned. """
        self.clean()

    def clean(self):
        """ Remove all project files. """
        for name in ['.gui_projectdir', '.TMP_runfiles', '.relion_lock',
                     'default_pipeline.star',
                     'Import', 'External']:
            if self.exists(name):
                Process.system(f"rm -rf '{self.join(name)}'", print=self.log)

        self._create()

    def run(self, cmd, jobType, wait=False):
        job = self._createJob(jobType)
        job['status'] = 'Launched'
        cmd += f" --output {jobId}"
        self._runCmd(cmd, jobId, wait=wait)

    def _create(self):
        """ Create a new project in the given path. """
        if self.exists(self.pipeline_star):
            raise Exception(f"Can not create project, pipeline already exists: "
                            f"{self.pipeline_star}")

        self.log(f"Creating new project at: {os.path.abspath(self.path)}")

        with open(self.join('.gui_projectdir'), 'w'):
            pass

        RelionStar.write_pipeline(self.pipeline_star)

    def _update_pipeline_star(self):
        self.log(f"Updating {self.pipeline_star}")
        RelionStar.workflow_to_pipeline(self._wf, self.pipeline_star)

    def _saveCmd(self, cmd, jobId):
        """ Write command.txt file to be used for restart. """
        with open(self.join(jobId, 'command.txt'), 'w') as f:
            f.write(f"{cmd}\n")

    def _loadCmd(self, jobId):
        with open(self.join(jobId, 'command.txt')) as f:
            return f.readline().strip()

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

    def _writeJobParams(self, job, params):
        """ Write the job.star for the given job. """
        # Write job params in the output folder
        jobType = job['jobtype']
        jobConf = ProcessingConfig.get_job_conf(jobType)
        jobForm = ProcessingConfig.get_job_form(jobType)
        values = ProcessingConfig.get_form_values(jobForm)
        values.update(params)
        paramsFile = self.join(job.id, 'job.star')
        self.log(f"Saving job params: {paramsFile}")
        isContinue = 1 if os.path.exists(paramsFile) else 0  # FIXME
        isTomo = 1 if jobConf.get('tomo', False) else 0
        with StarFile(paramsFile, 'w') as sfOut:
            tJob = Table(['rlnJobTypeLabel', 'rlnJobIsContinue', 'rlnJobIsTomo'])
            tJob.addRowValues(jobType, isContinue, isTomo)  # FIXME check continue and isTomo
            sfOut.writeTimeStamp()
            sfOut.writeTable('job', tJob, singleRow=True)
            tValues = Table(['rlnJobOptionVariable', 'rlnJobOptionValue'])
            for k, v in values.items():
                val = ('Yes' if v else 'No') if isinstance(v, bool) else v
                tValues.addRowValues(k, val)
            sfOut.writeTable('joboptions_values', tValues, computeFormat=True)

    def _createJob(self, jobType, params):
        jobConf = ProcessingConfig.get_job_conf(jobType)

        if jobConf is None:
            raise Exception(f"Unknown job type: {jobType}.")

        # Get jobIndex for the new job
        jobIndex = self._wf.jobNextIndex

        # Setup job's id as its output folder, base on the
        # configured output folder for this jobType and its ID
        jobTypeFolder = jobConf.get('folder', 'External')
        jobId = f'{jobTypeFolder}/job{jobIndex:03}'
        self.mkdir(jobId)

        # Register the new job in the workflow dict
        # and write updated pipeline_star
        job = self._wf.registerJob(jobId,
                                   status=STATUS_SAVED,
                                   alias='None',
                                   jobtype=jobType)

        # Write job.star file
        self._writeJobParams(job, params)

        self._update_pipeline_star()

        return job

    def _hasJob(self, jobId):
        return self._wf.hasJob(jobId)

    def _getJob(self, jobId):
        """ Load a given job and check its folder exist. """
        if not self._wf.hasJob(jobId):
            raise Exception(f"There is not job with id: '{jobId}'")

        if not self.exists(jobId):
            raise Exception(f"Missing folder for job: '{jobId}'")

        return self._wf.getJob(jobId)

    def restart(self, jobId, clean=False, wait=False):
        job = self._getJob(jobId)
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

    def listJobs(self):
        """ List current jobs. """
        """ Update status of the running jobs. """
        header = ["JOB_ID", "JOB_TYPE", "JOB_STATUS"]
        format = u'{:<25}{:<25}{:<25}'
        print(format.format(*header))

        for job in self._wf.jobs():
            print(format.format(job.id, job['jobtype'], job['status']))

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
            self._update_pipeline_star()

    def saveJob(self, jobTypeOrId, params):
        """ Save a job. If jobId = None, a new job is created
        and the parameters are saved. If jobId is not None,
        the save action is allowed only if the job is in 'Saved'
        state.
        """
        job = None
        jobTypeOrId = Path.rmslash(jobTypeOrId)

        if self._hasJob(jobTypeOrId):
            job = self._getJob(jobTypeOrId)
            if job['status'] != STATUS_SAVED:
                raise Exception("Can only save un-run jobs.")
            self._writeJobParams(job, params)
        else:
            if jobDef := ProcessingConfig.get_job_form(jobTypeOrId):
                job = self._createJob(jobTypeOrId, params)

        if job is None:
            raise Exception(f"{jobTypeOrId} is not an existing jobId or job type.")

    @staticmethod
    def main():
        p = argparse.ArgumentParser(
            prog='emw',
            description='emwrap project manager, compatible with the Relion '
                        'project structure. This program should be run in '
                        'the project folder')

        p.add_argument('--path', '-p', metavar="PROJECT_PATH",
                       help="Project path", default='.', nargs='?')
        g = p.add_mutually_exclusive_group()

        g.add_argument('--update', '-u', action='store_true',
                       help="Update job status and pipeline star file.")

        g.add_argument('--list', '-l', action='store_true',
                       help="List jobs in the current project.")

        g.add_argument('--run', '-r', nargs='+',
                       metavar=('COMMAND_OR_FOLDER', 'RUN_MODE'),
                       help="Input a command (new job) or an existing job folder. "
                            "In the case of the existing job folder, the special "
                            "'clean' mode can be passed to delete previous run "
                            "data.")
        g.add_argument('--save', '-s', nargs=2,
                       metavar=('JOB_TYPE_OR_ID', 'PARAMS'),
                       help="Save an existing job or create a new one, "
                            "updating the parameters")

        g.add_argument('-k', '--check', action='count', default=0,
                       help='Check and/or kill processes related to this project.'
                            'Pass more than one -k to kill processes.')

        p.add_argument('--wait', '-w', action='store_true',
                       help="Works with --run and make the project waits for "
                            "the sub-process to complete. Useful for scripting "
                            "and benchmarking.")

        g.add_argument('--clean', '-c', action='store_true',
                       help="If this option is used alone, it will "
                            "clean project files and create a new project. "
                            "If used in with --run, it will clean the job "
                            "output before running the command. ")

        args = p.parse_args()
        n = len(sys.argv)

        if n == 1:
            p.print_help(sys.stderr)
            sys.exit(1)
        else:
            if n == 2 and args.clean:
                # Only clean option, clean and create project
                pm = ProjectManager(args.path, create=True)
            else:
                # Just try to load the existing project
                pm = ProjectManager(args.path)

        if args.update:
            pm.update()
        elif args.list:
            pm.listJobs()
        elif args.run:
            first = args.run[0]
            if os.path.exists(first):
                mode = args.run[1] if len(args.run) > 1 else None
                # Make sure we get the jobId without ending slash
                jobId = Path.rmslash(first)
                clean = mode == 'clean'
                pm.restart(jobId, clean=clean, wait=args.wait)
            else:
                jobType = first.split()[0]
                pm.run(first, jobType, wait=args.wait)

        elif args.save:
            jobIdOrType = args.save[0]
            params = json.loads(args.save[1])
            pm.saveJob(jobIdOrType, params)
        elif args.check > 0:
            kill = args.check > 1
            folderPath = os.path.abspath(args.path)
            Process.checkChilds('emw', folderPath, kill=kill, verbose=True)
