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

from emtools.utils import FolderManager


class ProcessingConfig:
    _path = os.path.join(os.environ['EMCONFIG_FOLDER'])
    _fm = FolderManager(_path)
    _config = {}
    _jobs_dict = {}
    _forms_dict = {}

    @classmethod
    def load_config(cls):
        if cls._fm.exists('config.json'):
            with open(cls._fm.join('config.json')) as f:
                cls._config = json.load(f)

            cls._jobs_dict = {job['type']: job for job in cls.get_jobs()}

    @classmethod
    def get_menu(cls):
        return cls._config['menu']

    @classmethod
    def get_jobs(cls):
        return cls._config['jobs']

    @classmethod
    def get_cluster(cls):
        return cls._config.get('cluster', {})

    @classmethod
    def get_cluster_template(cls):
        if cluster := cls.get_cluster():
            return cls._fm.join(cluster['template'])
        else:
            return None

    @classmethod
    def get_cluster_submit(cls):
        cls.get_cluster().get('submit', None)

    @classmethod
    def get_jobs_dict(cls):
        return cls._jobs_dict

    @classmethod
    def get_job_conf(cls, jobtype, default=None):
        return cls._jobs_dict.get(jobtype, default)

    @classmethod
    def get_job_form(cls, jobtype):
        if jobtype not in cls._forms_dict:
            form = None
            formJson = cls._jobs_dict.get(jobtype, {}).get('form', '')
            if formJson and cls._fm.exists(formJson):
                with open(cls._fm.join(formJson)) as f:
                    form = json.load(f)
            cls._forms_dict[jobtype] = form

        return cls._forms_dict[jobtype]

    @classmethod
    def get_job_launcher(cls, jobtype):
        jobConf = cls.get_job_conf(jobtype)
        if launcher := jobConf.get('launcher', None):
            launcher_path = cls._fm.join(launcher)
            if os.path.exists(launcher_path):
                return launcher_path
        return None

    @classmethod
    def iter_form_params(cls, jobForm):
        """ Iterate over all params in sections, groups, lines of
        the form definition. """
        for sectionDef in jobForm['sections']:
            for paramDef in sectionDef['params']:
                if paramDef.get('paramClass', '') == 'Line':
                    for paramDef2 in paramDef['params']:
                        yield paramDef2
                else:
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


ProcessingConfig.load_config()
