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
import shutil
from datetime import datetime

from emtools.utils import FolderManager, Process, Color, Path, Timer
from emtools.jobs import BatchManager, Workflow
from emtools.metadata import Table, StarFile, RelionStar

from .config import ProcessingConfig
from .processing_pipeline import ProcessingPipeline


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

JOB_STATUS_ACTIVE = [STATUS_LAUNCHED, STATUS_RUNNING]


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

    def get_workflow(self):
        return self._wf

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

    def listJobs(self):
        """ List current jobs. """
        self.update()

        header = ["JOB_ID", "JOB_TYPE", "JOB_STATUS"]
        format = u'{:<25}{:<35}{:<25}'
        print(format.format(*header))

        for job in self._wf.jobs():
            print(format.format(job.id, job['jobtype'], job['status']))

    def listOutputs(self):
        """ List current jobs. """
        self.update()

        header = ["JOB_ID", "OUTPUT", "DATATYPE", "INFO"]
        format = u'{:<20}{:<55}{:<45}{:<45}'
        print(format.format(*header))

        for job in self._wf.jobs():
            filesDict = self.loadJobOutputs(job)
            for o in job.outputs:
                if oInfo := filesDict.get(o.id, None):
                    datatype = oInfo['type']
                    info = oInfo['info']
                else:
                    datatype = 'No-type'
                    info = 'No-info'

                print(format.format(job.id, o.id, datatype, info))

    def listInputs(self):
        header = ["JOB_ID", "KEY", "INPUT", "DATATYPE", "INFO"]
        format = u'{:<20}{:25}{:<45}{:<35}{:<45}'

        # Build the list of outputs for all jobs
        filesDict = {}
        for job in self._wf.jobs():
            jobFilesDict = self.loadJobOutputs(job)
            filesDict.update(jobFilesDict)
            # for k, v in jobFilesDict.items():
            #     if not job.hasOutput(k):
            #         job.registerOutput(k, )

        for job in self._wf.jobs():
            params = self._readJobParams(job)
            for k, v in params.items():
                if v in filesDict:
                    info = filesDict[v]
                    print(format.format(job.id, k, v, info['type'], info['info']))
                    if not job.hasInput(v):
                        job.addInputs([self._wf.getData(v)])

        self._update_pipeline_star()


    def update(self):
        """ Update status of the running jobs. """
        self.log("Updating project.")
        t = Timer()
        update = False
        for job in self._wf.jobs():
            if self._isActiveJob(job):
                for statusFile, status in JOB_STATUS_FILES.items():
                    if self.exists(job.id, statusFile):
                        job['status'] = status
                        update = True

                if jobInfo := self.loadJobInfo(job):
                    for k, o in jobInfo['outputs'].items():
                        for fn, datatype in o['files']:
                            if not job.hasOutput(fn):
                                job.registerOutput(fn, datatype=datatype)
                                update = True

                # # FIXME: this is a quick and dirty to define some known
                # # output star files for tomography
                # jobPath = self.join(job.id)
                #
                # def _is_output(fn):
                #     return (fn.endswith('.star') and
                #             (fn.startswith('tomograms') or
                #              fn.startswith('tilt_series')))
                #
                # for fn in os.listdir(jobPath):
                #     if _is_output(fn):
                #         dataId = os.path.join(job.id, fn)
                #         if not job.hasOutput(dataId):
                #             job.registerOutput(dataId, datatype='File')

        if update:
            self._update_pipeline_star()

        self.log(t.getToc("Update took"))

    def _validateJobInputs(self, jobDef, params):
        """ Validate that provide values match with the job definition.
        For example, format values or that PathParam exists.
        """
        pass

    def _updateJobInputs(self, job, params):
        # Clear jobs inputs and add new ones
        job.inputs = []
        for k, v in params.items():
            for job2 in self._wf.jobs():
                if isinstance(v, str) and job2.id in v:
                    # In this case the saved job is taking an input from this job
                    data = job2.getOutput(v)
                    if data is None:
                        data = job2.registerOutput(v, datatype="File")
                    job.addInputs([data])

    def saveJob(self, jobTypeOrId, params, update=True):
        """ Save a job. If jobId = None, a new job is created
        and the parameters are saved. If jobId is not None,
        the save action is allowed only if the job is in 'Saved'
        state.
        By default, the saveJob will first update the workflow.
        You can pass update=False in a context where the
        workflow has been already updated before the call to saveJob.
        """
        if update:
            self.update()

        job = None
        if self._hasJob(jobTypeOrId):
            job = self._getJob(jobTypeOrId)
            # FIXME Activate the following validation once we allow to override the job's status
            # if job['status'] != STATUS_SAVED:
            #     raise Exception("Can only save un-run jobs.")
            self._writeJobParams(job, params)
        else:
            if jobDef := ProcessingConfig.get_job_form(jobTypeOrId):
                job = self._createJob(jobTypeOrId, params, update=False)

        if job is None:
            raise Exception(f"{jobTypeOrId} is not an existing jobId or job type.")

        job['status'] = STATUS_SAVED
        self._updateJobInputs(job, params)
        self._update_pipeline_star()

        return job

    def copyJob(self, jobId, params=None):
        """ Make a copy of an existing job and optionally update some params. """
        job = self._getJob(jobId)
        job_params = self._readJobParams(job, extraParams=params)
        self.saveJob(job['jobtype'], job_params)

    def _instanciateJobs(self, jobDict):
        """
        Instanciate jobs in jobDict giving new Ids and preserving dependencies.
        This method can be called from duplicateJobs or from loadWorkflow
        """
        # Compute graph with dependencies
        for jobId, jobInfo in jobDict.items():
            for k, v in jobInfo['params'].items():
                for jobId2 in jobDict:
                    if isinstance(v, str) and v.startswith(jobId2):
                        jobInfo['parents'].add(jobId2)
                        jobDict[jobId2]['children'].add(jobId)

        # Let's start with root nodes
        toDuplicate = [jobId for jobId in jobDict if not jobDict[jobId]['parents']]
        newIdsDict = {}  # Map old ids to new ids

        def _new_value(v, parents):
            for p in parents:
                if isinstance(v, str) and v.startswith(p):
                    return v.replace(p, newIdsDict[p])
            return v

        while toDuplicate:
            jobId = toDuplicate.pop(0)
            jobInfo = jobDict[jobId]
            params = jobInfo['params']
            # Fix the params with the new ids of the parents
            new_params = {k: _new_value(v, jobInfo['parents'])
                          for k, v in params.items()}
            newJob = self.saveJob(jobInfo['jobtype'], new_params)
            newIdsDict[jobId] = newJob.id
            toDuplicate.extend(jobInfo['children'])

        return list(newIdsDict.values())

    def duplicateJobs(self, jobIds):
        """ Duplicate one or many jobs.
        If there are more than one job, the links will be
        fixed to preserve relations to the newly created jobs.
        """
        def _jobInfo(jobId):
            job = self._getJob(jobId)
            return {
                'jobtype': job['jobtype'],
                'params': self._readJobParams(job),
                'parents': set(),
                'children': set()
            }

        return self._instanciateJobs({jobId: _jobInfo(jobId) for jobId in jobIds})

    def loadWorkflow(self, workflowJobs):
        """ Load a workflow with jobs templates. """
        def _jobInfo(jobEntry):
            return {
                'jobtype': jobEntry['jobtype'],
                'params': jobEntry['params'],
                'parents': set(),
                'children': set()
            }
        return self._instanciateJobs({e['jobid']: _jobInfo(e) for e in workflowJobs})

    def runJob(self, jobTypeOrId, params=None, clean=False, wait=False, update=True):
        """ Run a job.
        If the job already exist:
            - Must provide jobId and
            - Optionally, some params to override
            - Clean = True will clean up the output directory before run
        If it is a new job:
            - Must provide jobType and params
        """
        if update:
            self.update()

        job = None
        jobTypeOrId = Path.rmslash(jobTypeOrId)

        if self._hasJob(jobTypeOrId):
            job = self._getJob(jobTypeOrId)
            jobStar = self.join(job.id, 'job.star')
            jobType = job['jobtype']

            if self._isActiveJob(job):
                raise Exception("Can not re-run running or launched jobs.")

            job_params = self._readJobParams(job, extraParams=params)

            if clean:
                self.log(f"Clean job folder {job.id}")
                self._deleteJobFolder(job)
                self.mkdir(job.id)

            jobDef = ProcessingConfig.get_job_conf(jobType)
            self._updateJobInputs(job, job_params)
            self._writeJobParams(job, job_params)

        else:
            job_params = params
            jobType = jobTypeOrId
            jobDef = ProcessingConfig.get_job_conf(jobType)
            if jobDef:
                job = self._createJob(jobType, job_params)
                self._updateJobInputs(job, job_params)
                jobStar = self.join(job.id, 'job.star')

        if job is None:
            raise Exception(f"{jobTypeOrId} is not an existing jobId or job type.")

        launcher = ProcessingConfig.get_job_launcher(jobType)

        if not launcher:
            raise Exception(f"Invalid launcher for job type: {jobType}")

        self._runCmd(f"{launcher} -i {jobStar} -o {job.id}", job.id,
                     wait=wait, job_params=job_params)
        job['status'] = STATUS_LAUNCHED
        self._update_pipeline_star()

        return job

    def _deleteJobFolder(self, job, validate=True):
        if validate and self._isActiveJob(job):
            raise Exception("Can not delete launched or running jobs, stop them first.")

        if not self.exists('.Trash'):
            self.mkdir('.Trash')

        jobId = job.id
        now = datetime.now()
        uniqueTs = now.strftime("%Y%m%d_%H%M%S_%f")
        newName = f"{uniqueTs}_{os.path.basename(jobId)}"
        self.log(f"Deleting job {jobId}: mv {self.join(jobId)} {self.join('.Trash', newName)}")
        if self.exists(jobId):
            shutil.move(self.join(jobId), self.join('.Trash', newName))

    def deleteJobs(self, jobIds):
        """ Clean up job's folder. """
        deleted = []
        for jobId in jobIds:
            jobId = Path.rmslash(jobId)
            if job := self._getJob(jobId, validateExists=False):
                self._deleteJobFolder(job)
                self._wf.deleteJob(job)
                deleted.append(jobId)
            else:
                raise Exception(f"{jobTypeOrId} is not an existing jobId or job type.")

        self._update_pipeline_star()
        return deleted

    def _isActiveJob(self, job):
        return job['status'] in JOB_STATUS_ACTIVE

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

    def _runCmd(self, cmd, jobId, wait=False, job_params=None):
        self._saveCmd(cmd, jobId)
        if cluster := ProcessingConfig.get_cluster():
            # Create the template script from cluster template
            templateFile = ProcessingConfig.get_cluster_template()
            self.log(f"Reading template file: {templateFile}")
            with open(templateFile) as f:
                template = f.read()

            scriptFile = self.join(jobId, 'job.script')
            self.log(f"Writing script file: {scriptFile}")
            with open(scriptFile, 'w') as f:
                gpus = int(job_params.get('gpus', 1))   # FIXME Get gpu list and take the length
                cpus = max(int(job_params.get('cpus', 1)), gpus * 10)
                if gpus:
                    # FIXME: Use emgoat for a more general interaction with HPC
                    gpu_line = f'#BSUB -gpu "num={gpus}/host:mode=shared"'
                else:
                    gpu_line = ''

                f.write(template.format(jobId=jobId, command=cmd,
                                        gpus=gpus, cpus=cpus, gpu_line=gpu_line,
                                        workingDir=self.path))

            # FIXME Implement the wait option when submitting to a cluster
            submit = ProcessingConfig.get_cluster()['submit']
            submitCmd = submit.format(job_script=scriptFile)
            self.log(f"Executing: {Color.green(submitCmd)}")
            ###os.system(submitCmd)
            try:
                subprocess.run(shlex.split(submitCmd), check=True,
                               capture_output=True, text=True)
            except subprocess.CalledProcessError as e:
                self.log("Submission to cluster failed")
                self.log(f"  Error: '{e.stderr.rstrip()}'")
                self.log(f"  Cluster configured in config: {ProcessingConfig._fm.join('config.json')}")
                self.log( "  Maybe try to run locally?\n")
        else:
            args = shlex.split(cmd)
            stdout = open(self.join(jobId, 'run.out'), 'a')
            stderr = open(self.join(jobId, 'run.err'), 'a')
            cmd = self.log(f"{Color.green(args[0])} {Color.bold(' '.join(args[1:]))}")
            stdout.write(f"\n\n{cmd}\n")
            stdout.flush()

            # Run the command
            p = subprocess.Popen(args, cwd=self.path,
                                 stdout=stdout, stderr=stderr, close_fds=True)

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
        RelionStar.write_jobstar(jobType, values, paramsFile,
                                 isTomo=isTomo, isContinue=isContinue)

    def _readJobParams(self, job, extraParams=None):
        """ Read params from job.star and optionally update
        some of the params.
        """
        job_params = RelionStar.read_jobstar(self.join(job.id, 'job.star'))
        if extraParams:
            job_params.update(extraParams)
        return job_params

    def _createJob(self, jobType, params, update=True):
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

        if update:
            self._update_pipeline_star()

        return job

    def _hasJob(self, jobId):
        return self._wf.hasJob(Path.rmslash(jobId))

    def _getJob(self, jobId, validateExists=True):
        """ Load a given job and check its folder exist. """
        jid = Path.rmslash(jobId)
        if not self._wf.hasJob(jid):
            raise Exception(f"There is not job with id: '{jobId}'")

        if validateExists and not self.exists(jobId):
            raise Exception(f"Missing folder for job: '{jobId}'")

        return self._wf.getJob(jid)

    def loadJobInfo(self, job):
        """ Load the info.json file for a given run. """
        jobInfoFn = self.join(job.id, 'info.json')
        if os.path.exists(jobInfoFn):
            with open(jobInfoFn) as f:
                return json.load(f)
        return None

    def loadJobOutputs(self, job):
        filesDict = {}
        if jobInfo := self.loadJobInfo(job):
            filesDict = {o['files'][0][0]: o for o in jobInfo['outputs'].values()}
        return filesDict

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

        g.add_argument('--outputs', '-o', action='store_true',
                       help="List outputs from each job.")

        g.add_argument('--inputs', '-i', action='store_true',
                       help="List inputs from each job.")

        g.add_argument('--run', '-r', nargs='+',
                       metavar=('JOB_TYPE_OR_ID', 'PARAMS'),
                       help="Run a new job, passing job type and params"
                            "or re-run an existing one passing job_id."
                            "If --clean is added, the output folder will "
                            "be cleaned before running the job. ")
        g.add_argument('--save', '-s', nargs=2,
                       metavar=('JOB_TYPE_OR_ID', 'PARAMS'),
                       help="Save an existing job or create a new one, "
                            "updating the parameters")
        g.add_argument('--copy', '-y', nargs='+',
                       metavar=('JOB_ID', 'PARAMS'),
                       help="Copy an existing job and optionally, "
                            "updating some parameters")
        g.add_argument('--duplicate', nargs='+',
                       metavar='JOB_IDS',
                       help="Duplicate one or more jobs, preserving relations.")
        g.add_argument('--delete', '-d', nargs='+', metavar='JOB_IDS',
                       help="Delete one or more jobs.")

        g.add_argument('-k', '--check', action='count', default=0,
                       help='Check and/or kill processes related to this project.'
                            'Pass more than one -k to kill processes.')

        p.add_argument('--wait', '-w', action='store_true',
                       help="Works with --run and make the project waits for "
                            "the sub-process to complete. Useful for scripting "
                            "and benchmarking.")

        p.add_argument('--clean', '-c', action='store_true',
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

        def _params(params, i):
            n = len(params)
            return ProcessingPipeline.loadParams(params[i]) if i < n else None

        if args.update:
            pm.update()

        elif args.list:
            pm.listJobs()

        elif args.outputs:
            pm.listOutputs()

        elif args.inputs:
            pm.listInputs()

        elif args.run:
            jobTypeOrId = args.run[0]
            pm.runJob(jobTypeOrId, _params(args.run, 1),
                      clean=args.clean,
                      wait=args.wait)

        elif args.copy:
            jobId = args.copy[0]
            pm.copyJob(jobId,  _params(args.copy, 1))

        elif args.duplicate:
            pm.duplicateJobs(args.duplicate)

        elif args.save:
            jobIdOrType = args.save[0]
            params = json.loads(args.save[1])
            pm.saveJob(jobIdOrType, params)

        elif args.delete:
            pm.deleteJobs(args.delete)

        elif args.check > 0:
            kill = args.check > 1
            folderPath = os.path.abspath(pm.path)
            Process.checkChilds('emw', folderPath, kill=kill, verbose=True)
