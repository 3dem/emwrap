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

import sys
import os
import json
import argparse
from pprint import pprint

from emtools.utils import Pretty, Color


class ProcessingConfig:
    _config = None
    _forms_dict = {}

    @classmethod
    def _get_config(cls, key='', default=None):
        if cls._config is None:
            cls._config = json.loads(os.environ.get('EMWRAP_CONFIG', '{}'))

        return cls._config.get(key, default or {}) if key else cls._config

    @classmethod
    def get_jobs(cls):
        return cls._get_config('jobs')

    @classmethod
    def get_programs(cls):
        return cls._get_config('programs')

    @classmethod
    def get_cluster(cls):
        return cls._get_config('cluster')

    @classmethod
    def get_cluster_template(cls):
        return cls.get_cluster().get('template', None)

    @classmethod
    def get_cluster_submit(cls):
        cls.get_cluster().get('submit', None)

    @classmethod
    def get_job_conf(cls, jobtype):
        return cls.get_jobs().get(jobtype)

    @classmethod
    def get_job_form_file(cls, jobtype):
        return os.path.join(cls._get_config('forms'), f'{jobtype}.json')

    @classmethod
    def get_job_form(cls, jobtype):
        if jobtype in cls.get_jobs():
            jsonFile = cls.get_job_form_file(jobtype)
            if os.path.exists(jsonFile):
                with open(jsonFile) as f:
                    return json.load(f)
            else:
                Pretty.dprint(Color.red(f"Form file not found: {jsonFile}"))
        else:
            Pretty.dprint(Color.red(f"Job type not found: {jobtype}"))

        return None

    @classmethod
    def get_workflow_file(cls, workflowId):
        return os.path.join(cls._get_config('workflows'), f'{workflowId}.json')

    @classmethod
    def get_workflow(cls, workflowId):
        workflowFile = cls.get_workflow_file(workflowId)
        if not os.path.exists(workflowFile):
            raise Exception(f"Workflow file: {Color.red(workflowFile)} does not exists.")
            
        with open(workflowFile) as f:
            return json.load(f)

    @classmethod
    def get_job_launcher(cls, jobtype):
        return cls.get_job_conf(jobtype).get('launcher', None)

    @classmethod
    def iter_form_params(cls, jobForm):
        """ Iterate over all params in sections, groups, lines of
        the form definition. """
        def _iter_params(containerDef):            
            if params := containerDef.get('params', None):
                for p in params:
                    for paramDef in _iter_params(p):
                        yield paramDef
            else:
                yield containerDef


        for sectionDef in jobForm['sections']:
            for paramDef in _iter_params(sectionDef):
                yield paramDef

    @classmethod
    def get_form_values(cls, jobForm, all=False):
        """ Iterate over all params in the form and get a dict
        with their name as key and default values.

        Args:
            all: if True, the full dict will be returned,
            including params with None value.
        """
        values = {}
        for paramDef in cls.iter_form_params(jobForm):
            v = paramDef.get('default', None)
            name = paramDef.get('name', None)
            if name and (v or all):
                values[name] = v
        return values

    @classmethod
    def print_config(cls):
        print(json.dumps(cls._get_config(), indent=4))

    @classmethod
    def main(cls):
        p = argparse.ArgumentParser(
            prog='emw',
            description='emwrap config manager')

        g = p.add_mutually_exclusive_group()

        g.add_argument('--print', '-p', action='store_true',
                       help="Print the existing configuration.")
        g.add_argument('--form', '-f', metavar='JOB_TYPE',
                       help="Print the corresponding form for this job type.")

        args = p.parse_args()
        n = len(sys.argv)

        if n == 1 or args.print:
            cls.print_config()

        elif jobtype := args.form:
            if not jobtype in ProcessingConfig.get_jobs():
                raise Exception(f"Job type: {jobtype} not found in EMWRAP_CONFIG['jobs']")
            formFile = ProcessingConfig.get_job_form_file(jobtype)
            if not os.path.exists(formFile):
                raise Exception(f"Form file: {Color.red(formFile)} does not exists.")
            form = ProcessingConfig.get_job_form(jobtype)
            print(json.dumps(form, indent=4))
