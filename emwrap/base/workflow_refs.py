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

"""
Job references between the jobs of a workflow.

A param value references a job when it is the job id (<Folder>/jobNNN) or a
path inside its folder. Values may be nested in lists and dicts (e.g. the
'sources' table of Warp M create population). These pure functions are shared
by ProjectManager (jobs of a project) and the EMhub WorkflowEditor (jobs of a
workflow template).

Path handling is given with an optional ``relpath`` callable, applied to the
absolute strings only (e.g. ProjectManager.relpath). Without it, absolute
paths never reference a job.
"""

import os
import re

from .job_form import iter_param_strings, map_param_strings

# A param pointing at a job folder or its files: <JobType>/jobNNN[/...]
JOB_REF_RE = re.compile(r'^([^/\s]+/job\d+)(?:/|$)')

EXTERNAL_INPUTS = ('keep', 'clear')


def normalize_id(job_id):
    """ Return the job id without a trailing slash (as Path.rmslash). """
    job_id = str(job_id)
    return job_id[:-1] if job_id.endswith('/') else job_id


def normalize_value(value, relpath=None):
    """ Return the string relative to the project when it is absolute and
    relpath is given, otherwise the same string. """
    if relpath is not None and os.path.isabs(value):
        return relpath(value)
    return value


def job_ref(value, relpath=None):
    """ Return the job id a string points at (from its <Folder>/jobNNN
    prefix), or None. """
    if not isinstance(value, str) or not value:
        return None
    if m := JOB_REF_RE.match(normalize_value(value, relpath)):
        return m.group(1)
    return None


def string_references_job(value, job_id, relpath=None):
    """ Return True when a string is the job id or a path inside its folder. """
    if not isinstance(value, str):
        return False
    value = normalize_value(value, relpath)
    return value == job_id or value.startswith(f'{job_id}/')


def param_references_job(value, job_id, relpath=None):
    """ Return True when a param value (also nested in lists and dicts)
    references the job. """
    return any(string_references_job(s, job_id, relpath)
               for s in iter_param_strings(value))


def _longest_first(job_ids):
    return sorted(job_ids, key=len, reverse=True)


def referenced_jobs(value, job_ids, relpath=None):
    """ Return the ids in job_ids referenced by a param value (also nested
    in lists and dicts), longest id first. """
    strings = list(iter_param_strings(value))
    return [jid for jid in _longest_first(job_ids)
            if any(string_references_job(s, jid, relpath) for s in strings)]


def job_refs(params, job_ids, relpath=None):
    """ Yield (key, string, job_id) for each string of params (also nested)
    referencing a job in job_ids, longest id first for each string. """
    ordered = _longest_first(job_ids)
    for key, value in (params or {}).items():
        for s in iter_param_strings(value):
            if not s:
                continue
            for jid in ordered:
                if string_references_job(s, jid, relpath):
                    yield key, s, jid


def external_ref(value, job_ids, relpath=None):
    """ Return the job id referenced by a string when that job is not in
    job_ids, otherwise None. """
    ref = job_ref(value, relpath)
    return None if ref is None or ref in job_ids else ref


def clear_external(params, job_ids, cleared=None, job_id=None, relpath=None):
    """ Return a copy of params where the strings referencing jobs not in
    job_ids (also nested in lists and dicts) are cleared, appending
    {'jobid', 'param', 'value'} to cleared for each one. """
    new_params = {}
    for key, value in (params or {}).items():
        def _clear(s, key=key):
            if external_ref(s, job_ids, relpath):
                if cleared is not None:
                    cleared.append({'jobid': job_id, 'param': key, 'value': s})
                return ''
            return s
        new_params[key] = map_param_strings(value, _clear)
    return new_params


def build_graph(job_dict, relpath=None):
    """ Return {job_id: {'parents': set, 'children': set}} from the params
    references between the jobs of job_dict ({job_id: {'params', ...}}). """
    graph = {jid: {'parents': set(), 'children': set()} for jid in job_dict}
    for jid, info in job_dict.items():
        for _, _, pid in job_refs(info.get('params'), job_dict, relpath):
            if pid != jid:
                graph[jid]['parents'].add(pid)
                graph[pid]['children'].add(jid)
    return graph


def remap_string(value, id_map, parents, relpath=None):
    """ Replace the parent id referenced by a string with its new id (longest
    parent id first). A reference to a parent without new id is cleared. """
    for pid in _longest_first(parents):
        if string_references_job(value, pid, relpath):
            return value.replace(pid, id_map[pid], 1) if pid in id_map else ''
    return value


def remap_params(params, id_map, parents, relpath=None):
    """ Return a copy of params with the references to parents (also nested
    in lists and dicts) remapped to their new ids (see remap_string). """
    return {k: map_param_strings(v, lambda s: remap_string(s, id_map, parents,
                                                          relpath))
            for k, v in (params or {}).items()}


def instantiation_order(job_dict, graph):
    """ Return the job ids so that each job comes after its parents. Jobs are
    taken in rounds of the ones whose parents are done, in job_dict order. """
    done = set()
    order = []
    while len(done) < len(job_dict):
        ready = [jid for jid in job_dict
                 if jid not in done and graph[jid]['parents'] <= done]
        if not ready:
            raise Exception(
                "Workflow job dependency cycle or missing parent references.")
        order.extend(ready)
        done.update(ready)
    return order


def instantiate(job_dict, create_job, external_inputs='keep', cleared=None,
                relpath=None):
    """ Create new jobs from job_dict ({job_id: {'jobtype', 'params'}}),
    with their references to other jobs of job_dict remapped to new ids.

    Args:
        create_job: function(old_id, jobtype, params) returning the new id.
        external_inputs: what to do with the references to jobs not in
            job_dict: 'keep' them (same project, e.g. duplicate) or 'clear'
            them (jobs coming from another project, whose ids mean other jobs).
        cleared: optional list, filled with {'jobid', 'param', 'value'} of
            each cleared param (jobid is the one in job_dict).
    Returns:
        {old_id: new_id}. job_dict is not modified.
    """
    if external_inputs not in EXTERNAL_INPUTS:
        raise Exception(f"Invalid external_inputs: {external_inputs}")

    params = {jid: info.get('params') or {} for jid, info in job_dict.items()}
    if external_inputs == 'clear':
        params = {jid: clear_external(p, job_dict, cleared, jid, relpath)
                  for jid, p in params.items()}

    graph = build_graph({jid: {'params': p} for jid, p in params.items()},
                        relpath)
    id_map = {}
    for jid in instantiation_order(job_dict, graph):
        new_params = remap_params(params[jid], id_map, graph[jid]['parents'],
                                  relpath)
        id_map[jid] = create_job(jid, job_dict[jid]['jobtype'], new_params)
    return id_map
