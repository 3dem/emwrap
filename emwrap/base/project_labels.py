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
import uuid

from emtools.utils import Path

from .project_lock import atomic_write_json


class ProjectLabels:
    """ Project labels (shown as "tags" in the workflow GUI).

    Labels are stored in a JSON file inside the project (usually
    .emhub/labels.json) with the following structure:

        {
            "labels": [{"id": "...", "title": "...", "color": "...", "description": "..."}],
            "jobs": {"Import/job001": ["<label_id>", ...]}
        }

    The file is always read fresh from disk, so long-lived instances never
    overwrite changes made by other processes. This class does not lock;
    callers (ProjectManager) must hold the project lock for write operations.
    """
    def __init__(self, path):
        self._path = path

    @property
    def path(self):
        return self._path

    def exists(self):
        return os.path.exists(self._path)

    # ---------------------------- Read -------------------------------------
    def load(self):
        """ Return the labels data from disk (empty if missing or invalid). """
        data = {}
        if self.exists():
            try:
                with open(self._path) as f:
                    data = json.load(f)
            except (OSError, json.JSONDecodeError):
                data = {}
        if not isinstance(data, dict):
            data = {}
        labels = data.get('labels')
        jobs = data.get('jobs')
        return {
            'labels': labels if isinstance(labels, list) else [],
            'jobs': jobs if isinstance(jobs, dict) else {}
        }

    def getLabels(self):
        """ Return the list of label definitions. """
        return self.load()['labels']

    def getJobLabels(self, jobId=None):
        """ Return the label ids of a job, or the full job -> label ids map. """
        jobs = self.load()['jobs']
        if jobId is None:
            return jobs
        return jobs.get(Path.rmslash(str(jobId)), [])

    # ---------------------------- Write ------------------------------------
    def _save(self, data):
        os.makedirs(os.path.dirname(self._path), exist_ok=True)
        atomic_write_json(self._path, data)

    def saveLabel(self, label):
        """ Create (no id) or update (existing id) a label definition. """
        if not isinstance(label, dict):
            raise Exception("Label should be a dictionary.")
        label = dict(label)
        if not label.get('id'):
            label['id'] = str(uuid.uuid4())
        label['id'] = str(label['id'])

        data = self.load()
        labels = data['labels']
        for i, other in enumerate(labels):
            if other.get('id') == label['id']:
                labels[i] = label
                break
        else:
            labels.append(label)
        self._save(data)
        return label

    def deleteLabel(self, labelId):
        """ Remove a label definition and unassign it from all jobs. """
        labelId = str(labelId)
        data = self.load()
        n = len(data['labels'])
        data['labels'] = [l for l in data['labels'] if l.get('id') != labelId]
        if len(data['labels']) == n:
            return False
        for jobId in list(data['jobs']):
            ids = [i for i in data['jobs'][jobId] if i != labelId]
            if ids:
                data['jobs'][jobId] = ids
            else:
                del data['jobs'][jobId]
        self._save(data)
        return True

    def setJobLabels(self, jobId, labelIds):
        """ Set the labels of a job. Unknown label ids are ignored.
        Return the list of label ids assigned to the job.
        """
        jobId = Path.rmslash(str(jobId))
        data = self.load()
        known = {l.get('id') for l in data['labels']}
        ids = []
        for i in labelIds or []:
            i = str(i)
            if i in known and i not in ids:
                ids.append(i)
        if ids:
            data['jobs'][jobId] = ids
        else:
            data['jobs'].pop(jobId, None)
        self._save(data)
        return ids

    def removeJobs(self, jobIds):
        """ Remove label assignments of the given jobs (e.g. deleted jobs). """
        if not self.exists():
            return False
        data = self.load()
        changed = False
        for jobId in jobIds:
            if data['jobs'].pop(Path.rmslash(str(jobId)), None) is not None:
                changed = True
        if changed:
            self._save(data)
        return changed

    def clearJobs(self):
        """ Remove all job assignments, keeping the label definitions. """
        if not self.exists():
            return
        data = self.load()
        if data['jobs']:
            data['jobs'] = {}
            self._save(data)

    def importLabels(self, labels, jobs, validJobIds=None):
        """ Initialize the labels file from existing data (e.g. migration
        from the EMhub database). Does nothing if the file already exists.
        Return True if the file was written.
        """
        if self.exists():
            return False
        labels = [dict(l) for l in (labels or []) if isinstance(l, dict) and l.get('id')]
        known = {str(l['id']) for l in labels}
        newJobs = {}
        for jobId, ids in (jobs or {}).items():
            jobId = Path.rmslash(str(jobId))
            if validJobIds is not None and jobId not in validJobIds:
                continue
            ids = [str(i) for i in (ids or []) if str(i) in known]
            if ids:
                newJobs[jobId] = ids
        self._save({'labels': labels, 'jobs': newJobs})
        return True
