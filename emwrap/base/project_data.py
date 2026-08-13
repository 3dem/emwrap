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
import json
from datetime import datetime

from emtools.utils import FolderManager, Color, Pretty, Path
from emtools.metadata import StarFile, RelionStar
from emtools.image import Image

from .config import ProcessingConfig
from .processing_pipeline import ProcessingPipeline
from .project_lock import atomic_write_json


class ProjectData(FolderManager):
    """ Class to manage additional information about jobs and outputs of a project. 

    Despite the project structure is compatible with Relion, we need to handle additional information
    that is not part of the Relion pipeline.star file. We will store output's type and info, together
    with extended job information, such as some status that are not supported by Relion. 

    The simplest implementation will use a project.json file to store the additional information,
    but later we might want to use a database (e.g. Redis) to store the information.
    """
    STATUS_LAUNCHED = 'Launched'
    STATUS_RUNNING = 'Running'
    STATUS_SUCCEEDED = 'Succeeded'
    STATUS_FAILED = 'Failed'
    STATUS_ABORTED = 'Aborted'
    STATUS_SAVED = 'Saved'
    STATUS_SCHEDULED = 'Scheduled'

    JOB_STATUS_FILES = {
        'RELION_JOB_RUNNING': STATUS_RUNNING,
        'RELION_JOB_EXIT_SUCCESS': STATUS_SUCCEEDED,
        'RELION_JOB_EXIT_FAILURE': STATUS_FAILED,
        'RELION_JOB_EXIT_ABORTED': STATUS_ABORTED
    }

    JOB_STATUS_ACTIVE = [STATUS_LAUNCHED, STATUS_RUNNING, STATUS_SCHEDULED]

    JOB_STATUS_TERMINAL = [STATUS_SUCCEEDED, STATUS_FAILED, STATUS_ABORTED]

    # Emwrap-specific statuses Relion stores as Scheduled in default_pipeline.star
    JOB_STATUS_EMWRAP = [STATUS_SAVED, STATUS_LAUNCHED]

    # Check terminal RELION markers before RUNNING; stale RUNNING must not
    # mask EXIT_* files left by a finished or aborted worker process.
    JOB_STATUS_FILE_ORDER = (
        'RELION_JOB_EXIT_SUCCESS',
        'RELION_JOB_EXIT_FAILURE',
        'RELION_JOB_EXIT_ABORTED',
        'RELION_JOB_RUNNING',
    )

    def __init__(self, project):
        FolderManager.__init__(self, project.path)
        self._project = project
        self._project_json_path = self.join('project.json')
        self._wf = project.get_workflow()  # FIXME: It might be the other way around, i.e. the project has the workflow and the data manager has the project

        self._data = {'jobs': {}, 'outputs': {}}

        if os.path.exists(self._project_json_path):
            try:
                with open(self._project_json_path, 'r') as f:
                    self._data = json.load(f)
            except Exception as e:
                self._debug(f"Error loading project data from {Color.bold(self._project_json_path)}: {e}")

        self._jobs = self._data.get('jobs', {})
        self._outputs = self._data.get('outputs', {})
        self.restoreJobStatuses()

    def reload(self):
        """Reload project.json from disk and re-sync workflow job statuses."""
        self._wf = self._project.get_workflow()
        self._data = {'jobs': {}, 'outputs': {}}

        if os.path.exists(self._project_json_path):
            try:
                with open(self._project_json_path, 'r') as f:
                    self._data = json.load(f)
            except Exception as e:
                self._debug(
                    f"Error loading project data from {Color.bold(self._project_json_path)}: {e}")

        self._jobs = self._data.get('jobs', {})
        self._outputs = self._data.get('outputs', {})
        self.restoreJobStatuses()

    def _statusFromRelionFiles(self, job_id):
        for statusFile in self.JOB_STATUS_FILE_ORDER:
            if self.exists(job_id, statusFile):
                return self.JOB_STATUS_FILES[statusFile]
        return None

    def _resolveJobStatus(self, job):
        """ Resolve canonical status: project.json intent > RELION files > pipeline.star."""
        cached_status = self._jobs.get(job.id, {}).get('status')
        relion_status = self._statusFromRelionFiles(job.id)
        pipeline_status = job['status']

        # Saved in project.json overrides stale terminal RELION markers after re-save
        if cached_status == self.STATUS_SAVED:
            if relion_status == self.STATUS_RUNNING:
                return relion_status
            return cached_status

        # Actual completion markers on disk always win over cached active status
        if relion_status in self.JOB_STATUS_TERMINAL:
            return relion_status

        # User-set terminal status beats a stale RELION_JOB_RUNNING left behind
        # when a worker exited without updating its marker files.
        if cached_status in self.JOB_STATUS_TERMINAL:
            if relion_status == self.STATUS_RUNNING or relion_status is None:
                return cached_status

        if relion_status:
            return relion_status

        # Relion pipeline stores Saved/Launched as Scheduled; recover emwrap status.
        if pipeline_status == self.STATUS_SCHEDULED:
            if cached_status in self.JOB_STATUS_EMWRAP:
                return cached_status
            if self.exists(job.id, 'job.star'):
                return self.STATUS_SAVED

        return pipeline_status

    def _clearJobStatusFiles(self, job_id):
        for statusFile in self.JOB_STATUS_FILES:
            path = self.join(job_id, statusFile)
            if os.path.exists(path):
                os.remove(path)

    def _signalJobAbort(self, job_id):
        """Ask a running worker to abort using Relion's marker convention."""
        job_dir = self.join(job_id)
        if os.path.isdir(job_dir):
            with open(os.path.join(job_dir, 'RELION_JOB_ABORT_NOW'), 'w'):
                pass

    def _writeJobRelionStatus(self, job_id, suffix):
        """Replace RELION job status markers with a single terminal marker."""
        job_dir = self.join(job_id)
        if os.path.isdir(job_dir):
            ProcessingPipeline.output_file(suffix, job_dir)

    def _removeJobOutput(self, job, output_id):
        """Remove one output from the workflow graph and project.json cache."""
        job.removeOutput(output_id)
        if output_id in self._outputs:
            del self._outputs[output_id]

    def removeJobFromWorkflow(self, job):
        """Remove a job from the workflow graph and project.json cache."""
        job_id = job.id
        output_ids = set(job._outputs.keys())
        job_prefix = f'{job_id}/'

        for other in self._wf.jobs():
            if other.id == job_id:
                continue
            for input_id in list(other._inputs.keys()):
                if input_id in output_ids or input_id.startswith(job_prefix):
                    data = other._inputs.pop(input_id)
                    if other in data.childs:
                        data.childs.remove(other)

        self._wf.deleteJob(job)

        if job_id in self._jobs:
            del self._jobs[job_id]

        for output_id in list(self._outputs.keys()):
            if output_id == job_id or output_id.startswith(job_prefix):
                del self._outputs[output_id]

        return True

    def removeMissingJobs(self):
        """Remove pipeline jobs whose on-disk folder no longer exists."""
        removed = []
        for job in list(self._wf.jobs()):
            if not self.exists(job.id):
                self._project.log(
                    f"Removing job {job.id} from pipeline (missing folder)")
                self.removeJobFromWorkflow(job)
                removed.append(job.id)
        return removed

    def clearJobOutputs(self, job_id):
        """Clear workflow outputs and project.json output cache for a job."""
        job = self._wf.getJob(job_id, None)
        if job:
            for output_id in list(job._outputs.keys()):
                self._removeJobOutput(job, output_id)

        if job_id in self._jobs:
            info = dict(self._jobs[job_id])
            info['outputs'] = []
            self._set_info(self._jobs, job_id, info)

    def resetJobForSave(self, job_id):
        """Reset a re-saved job: drop stale RELION markers, outputs, and cache."""
        self._clearJobStatusFiles(job_id)
        output_nodes = self.join(job_id, 'RELION_OUTPUT_NODES.star')
        if os.path.exists(output_nodes):
            os.remove(output_nodes)
        self.clearJobOutputs(job_id)

    def setJobStatus(self, job_id, status):
        """ Persist status in project.json and the in-memory workflow."""
        job = self._wf.getJob(job_id, None)
        if job:
            job['status'] = status
        info = dict(self._jobs.get(job_id, {}))
        info['status'] = status
        self._set_info(self._jobs, job_id, info)

    def restoreJobStatuses(self):
        """ Apply statuses from project.json after loading the Relion pipeline."""
        for job in self._wf.jobs():
            job['status'] = self._resolveJobStatus(job)

    def _debug(self, message, **kwargs):
        self._project._print(f">>> ProjectData:: {message}", level=2)

    def _outputParentJob(self, output_id):
        if self._wf.hasData(output_id):
            return self._wf.getData(output_id).parent
        return None

    def _pendingOutputInfo(self, output_id):
        datatype = 'File'
        if self._wf.hasData(output_id):
            datatype = self._wf.getData(output_id).get('datatype', datatype)
        return {'type': datatype, 'info': 'Pending'}

    def _isPendingOutput(self, output_id):
        job = self._outputParentJob(output_id)
        filepath = self.join(output_id)
        return (job is not None and self.isActiveJob(job)
                and not os.path.exists(filepath))

    def _outputTarget(self, output_id, filepath):
        return output_id or self._projectPath(filepath)

    def _fileNoInfo(self, output_id, filepath, reason='No-info'):
        target = self._outputTarget(output_id, filepath)
        return {'type': 'File', 'info': f'{reason}: {target}'}

    def _computeOutputTypeInfo(self, output_id, outputFiles):
        filepath = outputFiles[0]
        self._debug(f"{Color.warn('OUTPUT')}: {Color.red('Computing')} info for {Color.bold(filepath)}")

        if not os.path.exists(filepath):
            if self._isPendingOutput(output_id):
                return self._pendingOutputInfo(output_id)
            return self._fileNoInfo(output_id, filepath, reason='Missing')

        info = 'No-info'

        if filepath.endswith('.star'):            
            try:
                if '_series' in filepath:
                    
                        datatype = 'TiltSeriesMovies'
                        global_table = StarFile.getTableFromFile('global', filepath)
                        first = global_table[0]
                        ts_table = StarFile.getTableFromFile(first.rlnTomoName, self.join(first.rlnTomoTiltSeriesStarFile))

                        # Validate that have tilt columns
                        if not ts_table.hasAllColumns(RelionStar.TOMO_FRAME_SERIES_COLUMNS):
                            raise ValueError(f"Tilt series {filepath} does not have the required columns:  "
                                            f"{RelionStar.TOMO_FRAME_SERIES_COLUMNS}")

                        # 'info': f"{len(self.allTsTable)} items, {x} x {y} x {n} x {N}, {ps:0.3f} Å/px",

                        if ts_table.hasColumn('rlnMicrographName'):
                            datatype = 'TiltSeries'

                        if ts_table.hasAllColumns(RelionStar.TOMO_ALIGNMENT_COLUMNS):
                            datatype = 'TiltSeriesAligned'
                        return {
                            'type': datatype,
                            'info': f'{len(global_table)} items'
                    }
                    
                elif filepath.endswith('tomograms.star'):
                    global_table = StarFile.getTableFromFile('global', filepath)
                    first = global_table[0]
                    datatype = 'Tomograms'
                    n = len(global_table)
                    ps = RelionStar.getTomoPixelSize(first)
                    binning = RelionStar.getTomoBinning(first)
                    return {
                        'type': datatype,
                        'info': f'{n} items, {ps:0.1f} Å/px, bin: {binning:0.1f}'
                    }
                elif filepath.endswith('optimisation_set.star'):
                    if not RelionStar.isTomoOptimisationSet(filepath):
                        raise Exception(
                            f"{filepath} is not a compliant tomography "
                            "optimisation_set STAR file."
                        )
                    t = RelionStar.readTomoOptimisationSet(filepath)

                    cols = {
                        'rlnTomoParticlesFile': 'particles',
                        'rlnTomoTomogramsFile': 'global'
                    }
                    info = {}
                    for col, tableName in cols.items():
                        linked_path = getattr(t[0], col)
                        star_path = self.join(linked_path)
                        with StarFile(star_path, 'r') as sf:
                            info[col] = {
                                'size': sf.getTableSize(tableName),
                                'table': sf.getTableInfo(tableName),
                            }

                    # TODO: Check if there are TomoParticles
                    ptsInfo = info['rlnTomoParticlesFile']
                    datatype = 'TomoParticles' if ptsInfo['table'].hasColumn('rlnTomoParticleName') else 'TomoCoordinates'
                    return {
                        'type': datatype,
                        'info': f'{ptsInfo["size"]} items, Tomograms: {info["rlnTomoTomogramsFile"]["size"]}'
                    }
            except Exception as e:
                self._debug(
                    f"Error computing {Color.warn('OUTPUT')} info for "
                    f"{Color.bold(filepath)}: {e}")
                if self._isPendingOutput(output_id):
                    return self._pendingOutputInfo(output_id)
                info = f'Error: {str(e)}'

        elif filepath.endswith('.mrc') or filepath.endswith('.mrcs'):
            try:
                meta = Image.get_metadata(filepath)
                if meta:
                    return {
                        'type': meta.get('dataType', 'File'),
                        'info': meta['info'],
                    }
            except Exception as e:
                self._debug(
                    f"Error computing {Color.warn('OUTPUT')} info for "
                    f"{Color.bold(filepath)}: {e}")
                if self._isPendingOutput(output_id):
                    return self._pendingOutputInfo(output_id)
                info = f'Error: {str(e)}'

        if info == 'No-info':
            return self._fileNoInfo(output_id, filepath)

        return {
            'type': 'File',
            'info': info
        }

    def _collectJobOutputIds(self, job_id, job=None):
        """Gather output node ids from the workflow graph, RELION star, and cache."""
        job = job or self._wf.getJob(job_id)
        outputs = [o.id for o in job.outputs]

        outputs_star = self.join(job_id, 'RELION_OUTPUT_NODES.star')
        if os.path.exists(outputs_star):
            output_table = StarFile.getTableFromFile('pipeline_nodes', outputs_star)
            for row in output_table:
                if row.rlnPipeLineNodeName not in outputs:
                    outputs.append(row.rlnPipeLineNodeName)

        for output_id in self._jobs.get(job_id, {}).get('outputs', []):
            if output_id not in outputs:
                outputs.append(output_id)

        return outputs

    def _extendJobInfoOutputs(self, job_id, jobInfo, job=None):
        """Merge the latest output ids for an active job without invalidating cache."""
        outputs = self._collectJobOutputIds(job_id, job=job)
        if outputs == jobInfo.get('outputs'):
            return jobInfo
        extended = dict(jobInfo)
        extended['outputs'] = outputs
        return extended

    def _shouldRefreshOutputInfo(self, output_id, job):
        """Return True when a running job likely has newer output metadata."""
        if job is None or not self.isActiveJob(job):
            return False

        cached = self._outputs.get(output_id)
        cached_ts = cached.get('ts', 0) if cached else 0

        output_path = self.join(output_id)
        if os.path.exists(output_path):
            if os.stat(output_path).st_mtime > cached_ts:
                return True

        run_out = self.join(job.id, 'run.out')
        if os.path.exists(run_out):
            if os.stat(run_out).st_mtime > cached_ts:
                return True

        return cached is None

    def _computeJobInfo(self, jobId, jobFiles):
        jobStarFile = jobFiles[0]

        if not os.path.exists(jobStarFile):
            return {
                'inputs': [],
                'outputs': [],
                'status': self.STATUS_FAILED
            }

        self._debug(f"{Color.cyan('JOB')}: {Color.red('Computing')} info for {Color.bold(jobStarFile)}")

        params = RelionStar.read_jobstar(jobStarFile)
        job = self._wf.getJob(jobId)
        all_job_ids = [job.id for job in self._project.get_workflow().jobs()]

        # Detect inputs not already in the job
        inputs = []

        for key, value in params.items():
            if not value or not isinstance(value, str):
                continue

            rel_value = self._project.relpath(value) if os.path.isabs(value) else value

            for pid in sorted(all_job_ids, key=len, reverse=True):
                if self._project._param_references_job(rel_value, pid):                    
                    inputs.append((pid, rel_value))

        # Compute outputs info
        outputs = self._collectJobOutputIds(jobId, job=job)

        # Resolve status: RELION marker files, then project.json, then pipeline
        status = self._resolveJobStatus(job)
        job['status'] = status

        return {
            'inputs': inputs,
            'outputs': outputs,
            'status': status
        }

    def _set_info(self, info_dict, item_id, info):
        info['ts'] = datetime.now().timestamp()
        self._debug(f"Setting info for {item_id}, ts: {Pretty.timestamp(info['ts'])}")
        info_dict[item_id] = info

    def _get_info(self, info_dict, item_id, info_files, compute_info_func):
        info, computed = info_dict.get(item_id, None), False
        force = self._project.force

        if info and not force:
            # Check if the info is up to date by checking the timestamp of the info files
            for info_file in info_files:
                if os.path.exists(info_file):
                    s = os.stat(info_file)
                    ts = s.st_mtime
                    if ts > info['ts']:
                        info = None
                        break
            if info:
                return info, computed

        computed = True
        info = compute_info_func(item_id, info_files)
        self._set_info(info_dict, item_id, info)
        return info, computed

    def recomputeAllInfos(self):
        """Recompute and persist metadata for all jobs and outputs."""
        seen_outputs = set()
        for job in self._wf.jobs():
            self._getJobInfo(job.id)
            for data in list(job.inputs) + list(job.outputs):
                if data.id in seen_outputs:
                    continue
                seen_outputs.add(data.id)
                self.getOutputInfo(data.id)
        self.save()

    def _updateJob(self, job, jobInfo, prune_outputs=False):
        """ Update the job data base on the updated info. """
        def _update_data(data, data_id):
            info = self.getOutputInfo(data_id)
            data['datatype'] = info['type']
            data['info'] = info['info']

        inputs = jobInfo.get('inputs', [])
        for pid, rel_value in inputs:
            if job.hasInput(rel_value):
                data = job.getInput(rel_value)
            else:
                parent_job = self._wf.getJob(pid)                
                if not parent_job.hasOutput(rel_value):
                    data = parent_job.registerOutput(rel_value)
                else:
                    data = parent_job.getOutput(rel_value)
                job.addInputs([data])

            _update_data(data, rel_value)

        outputs = jobInfo.get('outputs', [])
        output_ids = set(outputs)
        for o in outputs:
            data = job.getOutput(o) if job.hasOutput(o) else job.registerOutput(o)
            _update_data(data, o)

        if prune_outputs and not self.isActiveJob(job):
            for output in list(job.outputs):
                if output.id not in output_ids:
                    self._removeJobOutput(job, output.id)

        status = self._resolveJobStatus(job)
        cached = self._jobs.get(job.id, {})
        cached_status = cached.get('status')
        if (status == self.STATUS_SCHEDULED
                and cached_status in self.JOB_STATUS_EMWRAP):
            status = cached_status
        job['status'] = status
        info_changed = (
            cached.get('outputs') != jobInfo.get('outputs')
            or cached.get('inputs') != jobInfo.get('inputs')
        )
        if cached.get('status') != status or info_changed:
            info = dict(cached) if cached else dict(jobInfo)
            info['status'] = status
            info['inputs'] = jobInfo.get('inputs', info.get('inputs', []))
            info['outputs'] = jobInfo.get('outputs', info.get('outputs', []))
            self._set_info(self._jobs, job.id, info)
            return True
        return False

    def updateWorkflow(self):
        updated = bool(self.removeMissingJobs())
        for job in self._wf.jobs():
            info, computed = self._getJobInfo(job.id)
            active = self.isActiveJob(job)
            if active:
                extended = self._extendJobInfoOutputs(job.id, info, job=job)
                if extended['outputs'] != info.get('outputs'):
                    info = extended
                    computed = True
            if (self._updateJob(job, info, prune_outputs=computed and not active)
                    or computed):
                updated = True
            else:
                self._debug(f"{Color.cyan('JOB')}: Info for {Color.bold(job.id)} is up to date")

        return updated

    def _getJobInfo(self, job_id):
        self._debug(f"{Color.cyan('JOB')}: Getting info for {Color.bold(job_id)}")
        # run.out grows during execution; exclude it so log writes do not
        # invalidate output metadata and trigger pruning on every refresh.
        jobFiles = [self.join(job_id, fn) for fn in [
            'job.star', 'RELION_OUTPUT_NODES.star',
            *self.JOB_STATUS_FILES.keys(),
        ]]
        jobFiles.extend([self._project.pipeline_star, self._project_json_path])
        return self._get_info(self._jobs, job_id, jobFiles, self._computeJobInfo)

    def getJobInfo(self, job_id):
        info, computed = self._getJobInfo(job_id)
        return info

    def setJobInfo(self, job_id, job_info):
        self._set_info(self._jobs, job_id, job_info)

    def _annotationPath(self, jobId):
        return self.join('.emhub', Path.rmslash(jobId), 'annotation.json')

    def getJobAnnotation(self, jobId):
        """Return run name and comment stored for a workflow job."""
        path = self._annotationPath(jobId)
        if not os.path.isfile(path):
            return {'runName': '', 'comment': ''}

        try:
            with open(path) as f:
                data = json.load(f)
        except (OSError, json.JSONDecodeError):
            return {'runName': '', 'comment': ''}

        if not isinstance(data, dict):
            return {'runName': '', 'comment': ''}

        return {
            'runName': str(data.get('runName') or data.get('run_name') or ''),
            'comment': str(data.get('comment') or ''),
        }

    def saveJobAnnotation(self, jobId, runName='', comment=''):
        """Persist run name and comment for a workflow job."""
        jobId = Path.rmslash(str(jobId))
        if not self._wf.hasJob(jobId):
            raise Exception(f"There is not job with id: '{jobId}'")

        path = self._annotationPath(jobId)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        payload = {
            'runName': str(runName or ''),
            'comment': str(comment or ''),
        }
        with open(path, 'w') as f:
            json.dump(payload, f, indent=2)
            f.write('\n')
        return payload

    def getOutputInfo(self, output_id):
        self._debug(f"{Color.warn('OUTPUT')}: Getting info for {Color.bold(output_id)}")
        job = self._outputParentJob(output_id)
        force = self._project.force

        if self._isPendingOutput(output_id):
            if not force:
                if not (job and self.isActiveJob(job)):
                    cached = self._outputs.get(output_id)
                    if cached and not str(cached.get('info', '')).startswith('Error:'):
                        return cached
                pending = self._pendingOutputInfo(output_id)
                if job and self.isActiveJob(job):
                    self._set_info(self._outputs, output_id, pending)
                return pending

        if force or self._shouldRefreshOutputInfo(output_id, job):
            info = self._computeOutputTypeInfo(output_id, [self.join(output_id)])
            self._set_info(self._outputs, output_id, info)
            if (self._isPendingOutput(output_id)
                    and str(info.get('info', '')).startswith('Error:')):
                return self._pendingOutputInfo(output_id)
            return info

        outputFiles = [self.join(output_id)]
        info, computed = self._get_info(self._outputs, output_id, outputFiles,
                                        self._computeOutputTypeInfo)
        if (self._isPendingOutput(output_id)
                and str(info.get('info', '')).startswith('Error:')):
            return self._pendingOutputInfo(output_id)
        return info

    def setOutputInfo(self, output_id, output_info):
        self._set_info(self._outputs, output_id, output_info)

    def save(self):
        atomic_write_json(self._project_json_path, self._data)

    def isActiveJob(self, job):
        return job['status'] in self.JOB_STATUS_ACTIVE

    def _projectPath(self, *parts):
        """ Return a path relative to the project root. """
        if len(parts) == 1 and not os.path.isabs(parts[0]):
            return Path.rmslash(parts[0])
        return Path.rmslash(self.relpath(self.join(*parts)))

    def _printRunLogs(self, jobId, tail_lines=10):
        """ Print run.err summary and last lines of run.out. """
        err_path = self.join(jobId, 'run.err')
        out_path = self.join(jobId, 'run.out')
        err_rel = self._projectPath(jobId, 'run.err')
        out_rel = self._projectPath(jobId, 'run.out')

        print("RUN LOGS:")

        if os.path.exists(err_path):
            s = os.stat(err_path)
            if s.st_size > 0:
                print(f"  {err_rel}: {Color.red(Pretty.size(s.st_size))}, "
                      f"{Pretty.elapsed(s.st_mtime)}")
                with open(err_path) as f:
                    err_lines = f.readlines()
                for line in err_lines[-min(5, len(err_lines)):]:
                    print(f"    {Color.red(line.rstrip())}")
            else:
                print(f"  {err_rel}: empty")
        else:
            print(f"  {err_rel}: missing")

        if os.path.exists(out_path):
            s = os.stat(out_path)
            print(f"  {out_rel}: {Pretty.size(s.st_size)}, "
                  f"{Pretty.elapsed(s.st_mtime)}")
            if s.st_size > 0:
                with open(out_path) as f:
                    lines = f.readlines()
                for line in lines[-tail_lines:]:
                    print(f"    {line.rstrip()}")
        else:
            print(f"  {out_rel}: missing")

    def listJobDetails(self, job, update=True, tail_lines=10):
        """ Print status, inputs, outputs and run logs for a single job. """
        if isinstance(job, str):
            job = self._project._getJob(job)

        if update:
            self._project.update()

        print(f"JOB:     {job.id}")
        print(f"TYPE:    {job['jobtype']}")
        print(f"STATUS:  {job['status']}")
        print()

        print("INPUTS:")
        if job.inputs:
            for i in job.inputs:
                info = self.getOutputInfo(i.id)
                print(f"  {i.id:<45} {info['type']:<20} {info['info']}")
        else:
            print("  (none)")
        print()

        print("OUTPUTS:")
        if job.outputs:
            for o in job.outputs:
                info = self.getOutputInfo(o.id)
                print(f"  {o.id:<45} {info['type']:<20} {info['info']}")
        else:
            print("  (none)")
        print()

        self._printRunLogs(job.id, tail_lines=tail_lines)

    def listOutputDetails(self, output, update=True):
        """ Print details for a single workflow output/data node. """
        if isinstance(output, str):
            output_id = Path.rmslash(output)
            if not self._wf.hasData(output_id):
                raise Exception(f"There is no output with id: {output_id}.")
            output = self._wf.getData(output_id)

        if update:
            self._project.update()

        job = output.parent
        info = self.getOutputInfo(output.id)
        file_path = self.join(output.id)

        print(f"OUTPUT:  {output.id}")
        print(f"TYPE:    {info['type']}")
        print(f"INFO:    {info['info']}")
        print()
        print(f"JOB:     {job.id}")
        print(f"         {job['jobtype']}, {job['status']}")
        print()

        print("FILE:")
        if os.path.exists(file_path):
            s = os.stat(file_path)
            print(f"  {output.id}")
            print(f"  {Pretty.size(s.st_size)}, {Pretty.elapsed(s.st_mtime)}")
        else:
            print(f"  {Color.red('missing')}: {output.id}")
        print()

        print("USED BY:")
        if output.childs:
            for child in output.childs:
                print(f"  {child.id:<25} {child['jobtype']:<20} {child['status']}")
        else:
            print("  (none)")

    def listJobs(self, update=True):
        """ List current jobs. """
        if update:
            self._project.update()

        header = ["JOB_ID", "JOB_TYPE", "JOB_STATUS", "OUTPUTS", "INPUTS"]
        format = u'{:<25}{:<30}{:<15}{:<35}{:<45}'
        print(format.format(*header))

        def _data_id(data_list, index):
            return data_list[index].id if data_list and index < len(data_list) else ''

        def _output(job_id, input_value):
            return input_value.replace(f'{job_id}/', '') if input_value else ''

        for job in self._wf.jobs():
            inputs = list(job.inputs)
            outputs = list(job.outputs)
            first_input = _data_id(inputs, 0)
            first_output = _output(job.id, _data_id(outputs, 0))
            print(format.format(job.id, job['jobtype'], job['status'],
                                first_output, first_input))
            max_length = max(len(inputs), len(outputs))
            for i in range(1, max_length):
                input = _output(job.id, _data_id(inputs, i))
                output = _output(job.id, _data_id(outputs, i))
                print(format.format('', '', '', output, input))

    def listOutputs(self):
        """ List outputs for all jobs. """
        self._project.update()

        header = ["JOB_ID", "OUTPUT", "DATATYPE", "INFO"]
        format = u'{:<20}{:<55}{:<45}{:<45}'
        print(format.format(*header))

        for job in self._wf.jobs():
            for o in job.outputs:
                oInfo = self.getOutputInfo(o.id)
                print(format.format(job.id, o.id, oInfo['type'], oInfo['info']))

    def listInputs(self):
        """ List job inputs detected from job.star parameters. """
        header = ["JOB_ID", "KEY", "INPUT", "DATATYPE", "INFO"]
        format = u'{:<20}{:25}{:<45}{:<35}{:<45}'

        for job in self._wf.jobs():
            params = self._project._readJobParams(job)
            for k, v in params.items():
                if not isinstance(v, str):
                    continue
                if os.path.isabs(v):
                    v = self._project.relpath(v)
                v = Path.rmslash(v)
                if not self._wf.hasData(v):
                    continue
                info = self.getOutputInfo(v)
                print(format.format(job.id, k, v, info['type'], info['info']))
                if not job.hasInput(v):
                    job.addInputs([self._wf.getData(v)])

        self._project._save_workflow_data(message='sync job inputs')
