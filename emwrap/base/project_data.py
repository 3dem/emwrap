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

from emtools.utils import FolderManager, Color, Pretty
from emtools.metadata import StarFile, RelionStar
from emtools.image import Image

from .config import ProcessingConfig
from .processing_pipeline import ProcessingPipeline


class ProjectData(FolderManager):
    """ Class to manage additional information about jobs and outputs of a project. 

    Despite the project structure is compatible with Relion, we need to handle additional information
    that is not part of the Relion pipeline.star file. We will store output's type and info, together
    with extended job information, such as some status that are not supported by Relion. 

    The simplest implementation will use a project.json file to store the additional information,
    but later we might want to use a database (e.g. Redis) to store the information.
    """

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

    def _debug(self, message, **kwargs):
        self._project._print(f">>> ProjectData:: {message}", level=2)
        
    def _computeOutputTypeInfo(self, output_id, outputFiles):
        filepath = outputFiles[0]
        self._debug(f"{Color.warn('OUTPUT')}: {Color.red('Computing')} info for {Color.bold(filepath)}")

        if filepath.endswith('.star'):
            if '_series' in filepath:
                try:
                    datatype = 'FramesSeries'
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
                except Exception as e:
                    self._debug(
                        f"Error computing {Color.warn('OUTPUT')} info for "
                        f"{Color.bold(filepath)}: {e}")
            elif 'tomograms' in filepath:
                datatype = 'Tomograms'
                return {
                    'type': datatype,
                    'info': 'No-info'
                }
        elif filepath.endswith('.mrc'):
            try:
                dims = Image.get_dimensions(filepath)
                if (isinstance(dims, (list, tuple)) and len(dims) >= 3
                        and dims[0] == dims[1] == dims[2]):
                    return {
                        'type': 'Volume',
                        'info': f'{dims[0]} x {dims[1]} x {dims[2]} px'
                    }
            except Exception as e:
                self._debug(
                    f"Error computing {Color.warn('OUTPUT')} info for "
                    f"{Color.bold(filepath)}: {e}")

        return {
            'type': 'File',
            'info': 'No-info'
        }

    def _computeJobInfo(self, jobId, jobFiles):
        jobStarFile = jobFiles[0]
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
        outputs = [o.id for o in job.outputs]
         
        # Detect outputs not already in the job
        # FIXME: Check if job_pipeline.star is used or not, or RELION_OUTPUT_NODES.star is enough.
        outputsStarFile = jobFiles[1]
        # for fn in ['RELION_OUTPUT_NODES.star']: #, 'job_pipeline.star']:
        #     output_path = self.join(jobId, fn)
        #     if os.path.exists(output_path):
        #         outputsFile = output_path
        #         break

        if os.path.exists(outputsStarFile):
            self._debug(f"{Color.cyan('JOB')}: {Color.red('Reading')} star from {Color.bold(outputsStarFile)}")
            output_table = StarFile.getTableFromFile('pipeline_nodes', outputsStarFile)
            for row in output_table:
                if row.rlnPipeLineNodeName not in outputs:
                    outputs.append(row.rlnPipeLineNodeName)

        return {
            'inputs': inputs,
            'outputs': outputs
        }

    def _set_info(self, info_dict, item_id, info):
        info['ts'] = datetime.now().timestamp()
        self._debug(f"Setting info for {item_id}, ts: {Pretty.timestamp(info['ts'])}")
        info_dict[item_id] = info

    def _get_info(self, info_dict, item_id, info_files, compute_info_func):
        info = info_dict.get(item_id, None)
        if info:
            # Check if the info is up to date by checking the timestamp of the info files
            for info_file in info_files:
                if os.path.exists(info_file):
                    s = os.stat(info_file)
                    ts = s.st_mtime
                    if ts > info['ts']:
                        info = None
                        break
            if info:
                return info

        info = compute_info_func(item_id, info_files)
        self._set_info(info_dict, item_id, info)
        return info

    def _updateJob(self, job, jobInfo):
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
        for o in outputs:
            data = job.getOutput(o) if job.hasOutput(o) else job.registerOutput(o)
            _update_data(data, o)

    def updateWorkflow(self):
        for job in self._wf.jobs():
            info = self.getJobInfo(job.id)
            self._updateJob(job, info)

    def getJobInfo(self, job_id):
        self._debug(f"{Color.cyan('JOB')}: Getting info for {Color.bold(job_id)}")
        jobFiles = [self.join(job_id, fn) for fn in ['job.star', 'RELION_OUTPUT_NODES.star']]
        return self._get_info(self._jobs, job_id, jobFiles, self._computeJobInfo)

    def setJobInfo(self, job_id, job_info):
        self._set_info(self._jobs, job_id, job_info)

    def getOutputInfo(self, output_id):
        self._debug(f"{Color.warn('OUTPUT')}: Getting info for {Color.bold(output_id)}")
        outputFiles = [self.join(output_id)]
        return self._get_info(self._outputs, output_id, outputFiles, self._computeOutputTypeInfo)

    def setOutputInfo(self, output_id, output_info):
        self._set_info(self._outputs, output_id, output_info)

    def save(self):
        with open(self._project_json_path, 'w') as f:
            json.dump(self._data, f, indent=4)