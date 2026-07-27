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

from emtools.utils import FolderManager, Process, Color, Path, Timer, Pretty
from emtools.jobs import BatchManager, Workflow
from emtools.metadata import Table, StarFile, RelionStar

from .config import ProcessingConfig
from .processing_pipeline import ProcessingPipeline
from .project_data import ProjectData



class ProjectManager(FolderManager):
    """ Class to manipulate information about a Relion project. """

    def __init__(self, path, create=False, verbose=1):
        """ Create a ProjectManager in that path.

        Args:
            path: Path to the project directory.
            create: Create a new project if it does not exist.
            verbose: Verbosity level.
        """
        FolderManager.__init__(self, path)
        apath = os.path.abspath(path)
        self._verbose = verbose

        if not self.exists():
            raise Exception(f"Project path '{apath}' does not exist")

        if self.exists(self.pipeline_star):
            self.log(f"ProjectManager::: Loading project from: {apath}")
            self._wf = RelionStar.pipeline_to_workflow(self.pipeline_star)
            
        elif create:
            # Create a new project
            self._wf = Workflow()
            self._create()
        else:
            raise Exception(f"'{self.pipeline_star} does not exist")

        self._data = ProjectData(self)  

    def _print(self, message, level=1):
        if self._verbose >= level:
            print(f"{Pretty.now()}: {message}", flush=True)

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

    def listJobDetails(self, job, update=True, tail_lines=10):
        return self._data.listJobDetails(job, update=update, tail_lines=tail_lines)

    def listOutputDetails(self, output, update=True):
        return self._data.listOutputDetails(output, update=update)

    def listJobs(self, update=True):
        return self._data.listJobs(update=update)

    def listOutputs(self):
        return self._data.listOutputs()

    def listInputs(self):
        return self._data.listInputs()

    def update(self):
        """ Update status of the running jobs. """
        self.log("ProjectManager::: Updating project.")
        t = Timer()
        updated = self._data.updateWorkflow()
        if updated:
            self._save_workflow_data()

        self.log(t.getToc(f"{Color.cyan('Update took')}"))

    def _validateJobInputs(self, jobDef, params):
        """ Validate that provide values match with the job definition.
        For example, format values or that PathParam exists.
        """
        pass

    
    def _param_references_job(self, param_value, job_id):
        """Return True when a param value points at a job folder or its outputs."""
        if not isinstance(param_value, str):
            return False

        if os.path.isabs(param_value):
            param_value = self.relpath(param_value)
            
        return param_value == job_id or param_value.startswith(f'{job_id}/')
         
    def _updateJobInputs(self, job, params):
        # Clear jobs inputs and add new ones
        job.clearInputs()
        for k, v in params.items():
            for job2 in self._wf.jobs():
                if self._param_references_job(v, job2.id):
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
        is_existing = self._hasJob(jobTypeOrId)
        if is_existing:
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

        self._data.setJobStatus(job.id, ProjectData.STATUS_SAVED)
        self._updateJobInputs(job, params)
        if is_existing:
            self._data.resetJobForSave(job.id)
        self._save_workflow_data()

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
            for key, value in (jobInfo.get('params') or {}).items():
                for jobId2 in jobDict:
                    if jobId2 != jobId and self._param_references_job(value, jobId2):
                        jobInfo['parents'].add(jobId2)
                        jobDict[jobId2]['children'].add(jobId)

        # Instanciate in dependency order; jobs with multiple parents are created once
        # after all of their parents exist in newIdsDict.
        newIdsDict = {}  # Map old ids to new ids
        remaining = set(jobDict.keys())

        def _new_value(v, parents):
            for p in sorted(parents, key=len, reverse=True):
                if self._param_references_job(v, p):
                    return v.replace(p, newIdsDict[p], 1) if p in newIdsDict else ''
            return v

        while remaining:
            ready = [
                jobId for jobId in remaining
                if jobDict[jobId]['parents'].issubset(newIdsDict)
            ]
            if not ready:
                raise Exception(
                    "Workflow job dependency cycle or missing parent references."
                )

            for jobId in ready:
                jobInfo = jobDict[jobId]
                params = jobInfo['params']
                new_params = {k: _new_value(v, jobInfo['parents'])
                              for k, v in params.items()}
                newJob = self.saveJob(jobInfo['jobtype'], new_params)
                newIdsDict[jobId] = newJob.id
                remaining.remove(jobId)

        return newIdsDict

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

    def exportWorkflow(self, jobIds, output_path):
        """" Export a subworkflow with the given job ids. """
        workflow_json = {"jobs": []}
        for jobId in jobIds:
            job = self._getJob(jobId)
            workflow_json['jobs'].append({
                'jobid': job.id,
                'jobtype': job['jobtype'],
                'params': self._readJobParams(job),
            })

        # FIXME: For now, let's write all outputs to the project directory
        output_name = os.path.basename(output_path)  
        with open(self.join(output_name), 'w') as f:
            json.dump(workflow_json, f, indent=4)

    def loadWorkflow(self, **kwargs):
        """ Load a workflow with jobs templates. """
        if 'workflow_file' in kwargs:
            workflow_file = kwargs['workflow_file']
            if not os.path.exists(workflow_file):
                raise Exception(f"Workflow file not found: {workflow_file}")
            with open(workflow_file) as f:
                workflow = json.load(f)
        elif 'workflow_id' in kwargs:
            workflow = ProcessingConfig.get_workflow(kwargs['workflow_id'])
        elif 'workflow' in kwargs:
            workflow = kwargs['workflow']
        else:
            raise Exception("workflow_id, workflow_file or workflow is required.")

        def _jobInfo(jobEntry):
            return {
                'jobtype': jobEntry['jobtype'],
                'params': jobEntry['params'],
                'parents': set(),
                'children': set()
            }
        return self._instanciateJobs({e['jobid']: _jobInfo(e) for e in workflow['jobs']})

    def runJob(self, jobTypeOrId, params=None, clean=False, wait=False, update=True):
        """ Run a job.
        If the job already exist:
            - Must provide jobId and
            - Optionally, some params to override
            - Clean = True will clean up the output directory before run
        If it is a new job:
            - Must provide jobType and params
        """
        if not jobTypeOrId:
            raise Exception("Job type or id is required to run a job.")

        if update:
            self.update()

        job = None
        jobTypeOrId = Path.rmslash(jobTypeOrId)

        if self._hasJob(jobTypeOrId):
            job = self._getJob(jobTypeOrId)
            jobStar = os.path.join(job.id, 'job.star')
            jobType = job['jobtype']

            if self._data.isActiveJob(job):
                raise Exception("Can not re-run running or launched jobs.")

            job_params = self._readJobParams(job, extraParams=params)

            if clean:
                self.log(f"Clean job folder {job.id}")
                self._deleteJobFolder(job)
                self.mkdir(job.id)
                self._data.clearJobOutputs(job.id)

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
                jobStar = os.path.join(job.id, 'job.star')

        if job is None:
            raise Exception(f"{jobTypeOrId} is not an existing jobId or job type.")

        launcher = ProcessingConfig.get_job_launcher(jobType)

        if not launcher:
            raise Exception(f"Invalid launcher for job type: {jobType}")

        self._data._clearJobStatusFiles(job.id)
        self._runCmd(f"{launcher} -i {jobStar} -o {job.id}", job.id,
                     wait=wait, job_params=job_params)
        self._data.setJobStatus(job.id, ProjectData.STATUS_LAUNCHED)
        self._save_workflow_data()

        return job

    def stopJob(self, jobId):
        """ Stop a job. """
        job = self._getJob(jobId)
        if not self._data.isActiveJob(job):
            raise Exception("Can not stop non-running jobs.")

        self._data.setJobStatus(jobId, ProjectData.STATUS_ABORTED)
        if self.exists(jobId, 'job.id'):
            with open(self.join(jobId, 'job.id')) as f:
                job_id = f.readline().strip()

            job_params = self._readJobParams(job)
            qname = job_params.get('queue.name', 'NO-NAME')
            if qname == 'NO-NAME':
                raise Exception(f"No queue name found for stopping job {jobId}.")

            if queue := ProcessingConfig.get_queue(qname):
                cancelCmd = queue['cancel'].format(job_id=job_id)
            else:
                raise Exception(f"Queue {qname} not found in config for stopping job {jobId}.")

            scriptLog = self.join(jobId, 'job.log')
            try:
                subprocess.run(shlex.split(cancelCmd), check=True, capture_output=True, text=True)                
                self._log(f"Stopping CLUSTER job, {cancelCmd}", jobFile=scriptLog, flush=True)
            except subprocess.CalledProcessError as e:
                self._log("ERROR: Stopping CLUSTER job failed", jobFile=scriptLog)
                self._log(f"  Error: '{e.stderr.rstrip()}'", jobFile=scriptLog)
        self._data.setJobStatus(jobId, ProjectData.STATUS_ABORTED)
        self._save_workflow_data()
        return job

    def _deleteJobFolder(self, job, validate=True):
        if validate and self._data.isActiveJob(job):
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
                raise Exception(f"{jobId} is not an existing jobId.")

        self._save_workflow_data()
        return deleted

    def _create(self):
        """ Create a new project in the given path. """
        if self.exists(self.pipeline_star):
            raise Exception(f"Can not create project, pipeline already exists: "
                            f"{self.pipeline_star}")

        self.log(f"Creating new project at: {os.path.abspath(self.path)}")

        with open(self.join('.gui_projectdir'), 'w'):
            pass

        RelionStar.write_pipeline(self.pipeline_star)

    def _save_workflow_data(self):
        self.log(f"Updating {self.pipeline_star}")
        self._data.save()
        RelionStar.workflow_to_pipeline(self._wf, self.pipeline_star)

    def _saveCmd(self, cmd, jobId):
        """ Write command.txt file to be used for restart. """
        with open(self.join(jobId, 'command.txt'), 'w') as f:
            f.write(f"{cmd}\n")

    def _loadCmd(self, jobId):
        with open(self.join(jobId, 'command.txt')) as f:
            return f.readline().strip()

    def __fixMapping(self, cluster, path):
        for k, v in cluster.get('mappings', {}).items():
            if path.startswith(k):
                return path.replace(k, v)
        return path

    def _log(self, msg, jobFile=None, flush=False):
        """ Log also to a job file. """
        self.log(msg)
        if jobFile:
            with open(jobFile, 'a') as f:
                f.write(f"\n{Pretty.now()}: {msg}\n")
                if flush:
                    f.flush()

    def _resolveOutputFolder(self, output_folder):
        if os.path.isabs(output_folder):
            return output_folder
        return self.join(output_folder)

    def _writeJobStarFile(self, job_type, params, job_star):
        job_conf = ProcessingConfig.get_job_conf(job_type)
        job_form = ProcessingConfig.get_job_form(job_type)
        values = ProcessingConfig.get_form_values(job_form)
        values.update(params)
        is_continue = 1 if os.path.exists(job_star) else 0
        is_tomo = 1 if job_conf.get('tomo', False) else 0
        self.log(f"Writing job params: {job_star}")
        RelionStar.write_jobstar(job_type, values, job_star,
                                 isTomo=is_tomo, isContinue=is_continue)

    def _prepareQueueSubmission(self, cmd, job_params, folder_path, job_id=None):
        """Build cluster submission script content and command for a job folder."""
        qname = job_params.get('queue.name', 'NO-NAME')

        def eprint(*args, **kwargs):
            print(Pretty.now(), *args, file=sys.stderr, flush=True, **kwargs)

        eprint(f'qname = {qname}\n')

        if qname == 'None':
            return None

        queue = ProcessingConfig.get_queue(qname)
        if not queue:
            msg = f"Queue {qname} not found in config for submitting job {job_id}."
            self.log(msg)
            eprint(msg)
            return None

        qprefix = f'queue.param.{qname}.'
        qparams = {k.replace(qprefix, ''): v for k, v in job_params.items()
                   if k.startswith(qprefix)}

        script_file = os.path.join(folder_path, 'job.script')
        script_log = os.path.join(folder_path, 'job.log')
        gpus = int(job_params.get('gpus', 0))   # FIXME Get gpu list and take the length

        def _load_cpus(gpus):
            mpi, threads = 1, 1
            if 'nr_mpi' in job_params or 'nr_threads' in job_params:
                mpi = int(job_params.get('nr_mpi', 1))
                threads = int(job_params.get('nr_threads', 1))
            elif cpus_str := job_params.get('cpus', ''):
                if 'x' in cpus_str:
                    mpi, threads = map(int, cpus_str.split('x'))
                else:
                    mpi, threads = int(cpus_str), 1
            return mpi * threads

        cpus = _load_cpus(gpus)

        if gpus > 0:
            cpus = max(cpus, gpus * 10)

        if cpus == 0:
            raise Exception("Neither CPUs nor GPUs are set. Please set at least one of them.")

        if gpus:
            # FIXME: Use emgoat for a more general interaction with HPC
            mig = ':mig=2' if qparams.get('mig', False) else ''
            gpu_line = f'#BSUB -gpu "num={gpus}/host:mode=shared{mig}"'
            gpu_type = qparams.get('gpu_type', 'any')
            if gpu_type != 'any':
                gpu_line += f'\n#BSUB -R {gpu_type.lower()}'
        else:
            gpu_line = ''

        qparams.update({
            'queue_name': qname,
            'jobId': job_id or os.path.basename(folder_path.rstrip(os.sep)),
            'command': cmd,
            'gpu_line': gpu_line,
            'gpus': gpus,
            'cpus': cpus,
            'working_dir': self.path,
            'job_out': os.path.join(folder_path, 'run.out'),
            'job_err': os.path.join(folder_path, 'run.err')
        })

        with open(queue['template'], 'r') as f:
            template = f.read()

        script_content = template.format(**qparams)
        mapped_script = self.__fixMapping(queue, script_file)
        submit_cmd = queue['submit'].format(job_script=mapped_script)

        return {
            'queue': queue,
            'script_file': script_file,
            'script_log': script_log,
            'script_content': script_content,
            'submit_cmd': submit_cmd,
        }

    def _executeQueueSubmission(self, submission):
        script_file = submission['script_file']
        script_log = submission['script_log']
        submit_cmd = submission['submit_cmd']

        self._log(f"Writing CLUSTER submission script: {script_file}",
                  jobFile=script_log, flush=True)
        with open(script_file, 'w') as f:
            f.write(submission['script_content'])

        self._log(f"Executing CLUSTER submit command: {Color.green(submit_cmd)}",
                  jobFile=script_log, flush=True)
        try:
            result = subprocess.run(shlex.split(submit_cmd), check=True,
                                    capture_output=True, text=True)
            job_id = result.stdout.strip()
            if not job_id.isdigit():
                raise Exception(f"Unexpected submission output: {result.stdout}")
            self._log(f"Submission successful, JOB_ID: {job_id}",
                      jobFile=script_log, flush=True)
            with open(script_file.replace('.script', '.id'), 'w') as f:
                f.write(job_id)
            return job_id
        except subprocess.CalledProcessError as e:
            self._log("ERROR: Submission to cluster failed", jobFile=script_log)
            self._log(f"  Error: '{e.stderr.rstrip()}'", jobFile=script_log)
            self._log("  Maybe try to run locally?\n", jobFile=script_log, flush=True)

    def _runLocalCmd(self, cmd, folder_path, wait=False):
        args = shlex.split(cmd)
        stdout = open(os.path.join(folder_path, 'run.out'), 'a')
        stderr = open(os.path.join(folder_path, 'run.err'), 'a')
        logged_cmd = self.log(f"{Color.green(args[0])} {Color.bold(' '.join(args[1:]))}")
        stdout.write(f"\n\n{logged_cmd}\n")
        stdout.flush()

        p = subprocess.Popen(args, cwd=self.path,
                             stdout=stdout, stderr=stderr, close_fds=True)
        if wait:
            p.wait()

    def submitJob(self, job_type, params, output_folder, dry=False):
        """Submit a job outside the project workflow.

        When dry=False, write job.star in output_folder and either run locally
        or submit to a cluster queue based on queue parameters in params.
        When dry=True, only print the run or queue submission commands.
        """
        if isinstance(params, str):
            params = ProcessingPipeline.loadParams(params)

        job_conf = ProcessingConfig.get_job_conf(job_type)
        if job_conf is None:
            raise Exception(f"Unknown job type: {job_type}.")

        launcher = ProcessingConfig.get_job_launcher(job_type)
        if not launcher:
            raise Exception(f"Invalid launcher for job type: {job_type}")

        folder_path = self._resolveOutputFolder(output_folder)
        job_star = os.path.join(output_folder, 'job.star')
        cmd = f"{launcher} -i {job_star} -o {output_folder}"

        submission = self._prepareQueueSubmission(cmd, params, folder_path)

        if dry:
            if submission:
                print('COMMAND:', submission['submit_cmd'])
                print('SCRIPT:', submission['script_content'])
            else:
                print('COMMAND:', cmd)
            return cmd

        os.makedirs(folder_path, exist_ok=True)
        self._writeJobStarFile(job_type, params, os.path.join(folder_path, 'job.star'))

        if submission:
            return self._executeQueueSubmission(submission)

        self._runLocalCmd(cmd, folder_path)
        return cmd

    def _runCmd(self, cmd, jobId, wait=False, job_params=None):
        self._saveCmd(cmd, jobId)
        folder_path = self.join(jobId)
        if submission := self._prepareQueueSubmission(cmd, job_params, folder_path,
                                                      job_id=jobId):
            # FIXME Implement the wait option when submitting to a cluster
            self._executeQueueSubmission(submission)
        else:
            self._runLocalCmd(cmd, folder_path, wait=wait)

    def get_workflow(self):
        return self._wf

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
        jobTypeFolder = jobConf.get('output', 'External')
        jobId = f'{jobTypeFolder}/job{jobIndex:03}'
        self.mkdir(jobId)

        # Register the new job in the workflow dict
        # and write updated pipeline_star
        job = self._wf.registerJob(jobId,
                                   status=ProjectData.STATUS_SAVED,
                                   alias='None',
                                   jobtype=jobType)

        # Write job.star file
        self._writeJobParams(job, params)

        if update:
            self._data.setJobStatus(job.id, ProjectData.STATUS_SAVED)
            self._save_workflow_data()

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
        # if jobInfo := self.loadJobInfo(job):
        #     filesDict = {o['files'][0][0]: o for o in jobInfo['outputs'].values()}
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

        # list is None unless -l/--list appears; bare -l uses const 'jobs'.
        g.add_argument('--list', '-l', nargs='?', default=None,
                       metavar='JOB_ID', const='',
                       help="List all jobs or the details of a given one if JOB_ID is passed.")

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

        g.add_argument('--stop', '-t', metavar='JOB_ID',
                       help="Stop a launched or running job.")

        g.add_argument('--workflow', '-w', metavar='WORKFLOW_FILE',
                       help="Load a workflow from a JSON file and create its jobs.")

        g.add_argument('--submit', nargs=3,
                       metavar=('JOB_TYPE', 'PARAMS_OR_FILE', 'OUTPUT_FOLDER'),
                       help="Submit a job: write job.star and run locally or "
                            "submit to a cluster queue. PARAMS_OR_FILE is a "
                            "JSON string or a path to a .json or .star file. "
                            "Use --dry to only print the commands.")

        g.add_argument('-k', '--check', action='count', default=0,
                       help='Check and/or kill processes related to this project.'
                            'Pass more than one -k to kill processes.')

        p.add_argument('-v', '--verbose', action='count', default=0,
                       help='Increase verbosity (-v or -vv).')

        p.add_argument('--dry', action='store_true',
                       help="With --submit, print the run or queue submission "
                            "commands without writing files or executing.")

        p.add_argument('--wait', action='store_true',
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
            pipeline_star = os.path.join(args.path, 'default_pipeline.star')
            create = ((n == 2 and args.clean)
                      or (args.workflow and not os.path.exists(pipeline_star)))
            pm = ProjectManager(args.path, create=create, verbose=args.verbose)

        def _params(params, i):
            n = len(params)
            return ProcessingPipeline.loadParams(params[i]) if i < n else None

        if args.update:
            pm.update()

        elif args.list is not None:  # only when -l / --list is on the command line
            if inputId := args.list:  # it can be empty string when no value is passed
                w = pm.get_workflow()
                if job := w.getJob(inputId):
                    pm.listJobDetails(job)
                elif w.hasData(Path.rmslash(inputId)):
                    pm.listOutputDetails(w.getData(Path.rmslash(inputId)))
            else:
                pm.listJobs()

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

        elif args.stop:
            pm.stopJob(args.stop)

        elif args.workflow:
            if args.clean:
                pm.clean()
            id_map = pm.loadWorkflow(workflow_file=args.workflow)
            pm.log(f"Loaded workflow from {args.workflow}: "
                   f"{len(id_map)} job(s) created")
            for old_id, new_id in id_map.items():
                pm.log(f"  {old_id} -> {new_id}")

        elif args.submit:
            params = ProcessingPipeline.loadParams(args.submit[1])
            pm.submitJob(args.submit[0], params, args.submit[2], dry=args.dry)

        elif args.delete:
            pm.deleteJobs(args.delete)

        elif args.check > 0:
            kill = args.check > 1
            folderPath = os.path.abspath(pm.path)
            Process.checkChilds('emw', folderPath, kill=kill, verbose=True)

