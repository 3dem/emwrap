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
    # AVAILABLE JOBS in EMWRAP
    _jobs = {
        "emw-import-ts": {
            "launcher": "emwrap.base.import_ts",
            "label": "EMwrap Import TS",
            "output": "EMwrap"
        },
        "emw-subset-ts": {
            "launcher": "emwrap.base.subset_ts",
            "label": "EMwrap Subset TS",
            "output": "EMwrap"
        },
        "emw-merge-sets": {
            "launcher": "emwrap.base.merge_sets",
            "label": "EMwrap Merge Sets",
            "output": "EMwrap"
        },
        "emw-warp-mctf": {
            "launcher": "emwrap.warp.warp_mctf",
            "label": "Warp Motion and CTF",
            "output": "WarpMctf"
        },
        "emw-warp-tsalign": {
            "launcher": "emwrap.warp.warp_tsalign",
            "label": "Warp Tilt Series Alignment",
            "output": "WarpTsAlign"
        },
        "emw-missalignment": {
            "launcher": "emwrap.missalignment.miss_alignment",
            "label": "MissAlignment",
            "output": "MissAlignment"
        },
        "emw-warp-ctfrec": {
            "launcher": "emwrap.warp.warp_ctfrec",
            "label": "Warp CTF and Reconstruction",
            "output": "WarpCtfRec"
        },
        "emw-warp-otf": {
            "launcher": "emwrap.warp.warp_otf",
            "label": "Warp OTF Preprocessing",
            "output": "WarpOtf"
        }, 
        "emw-pytom-create_template": {
            "launcher": "emwrap.pytom.pytom_create_template",
            "label": "PyTom Create Template",
            "output": "PyTom"
        },
        "emw-pytom": {
            "launcher": "emwrap.pytom.pytom_pipeline",
            "label": "PyTom Template Matching",
            "output": "PyTom"
        },
        "emw-warp-export_particles": {
            "launcher": "emwrap.warp.warp_export_particles",
            "label": "Warp Export Particles",
            "output": "WarpExportParticles"
        },
        "emw-pytme": {"launcher": "emwrap.pytme", "visible": False},        
        "emw-relion-symmetrize_volume": {"launcher": "emwrap.relion.symmetrize_volume"},
        "emw-relion-mask_create": {"launcher": "emwrap.relion.mask_create"},
        "emw-warp-mtools_create": {
            "launcher": "emwrap.warp.warp_mtools_create",
            "label": "Warp M Create Population",
            "output": "WarpMCreatePopulation"
        },
        "emw-warp-mcore": {"launcher": "emwrap.warp.warp_mcore"},
        "emw-warp-estimate_weights": {"launcher": "emwrap.warp.warp_estimate_weights"},
        "emw-warp-mtools_resample": {"launcher": "emwrap.warp.warp_mtools_resample"},
        "relion.reconstructtomograms": {
            "launcher": "emwrap.relion.native",
            "label": "Relion Reconstruct Tomograms",
            "output": "Tomograms"
        },
        "relion.pseudosubtomo": {
            "launcher": "emwrap.relion.native",
            "label": "Relion Extract Subtomograms",
            "output": "Extract",
            "tomo": True
        },
        "relion.reconstructparticletomo": {
            "launcher": "emwrap.relion.native",
            "label": "Relion Tomo Reconstruct Particles",
            "output": "Reconstruct"
        },
        "relion.initialmodel.tomo": {
            "launcher": "emwrap.relion.native",
            "label": "Relion Tomo Initial Volume",
            "output": "InitialModel",
            "tomo": True
        },
        "relion.class3d.tomo": {
            "launcher": "emwrap.relion.native",
            "label": "Relion Tomo 3D Classification",
            "output": "Class3D",
            "tomo": True
        },
        "relion.refine3d.tomo": {
            "launcher": "emwrap.relion.native",
            "label": "Relion Tomo Refine",
            "output": "Refine3D",
            "tomo": True
        },
        "emw-aretomo3": {
            "launcher": "emwrap.aretomo.aretomo3_pipeline",
            "label": "Aretomo3",
            "output": "Aretomo3"
        },
        "emw-denoiset": {
            "launcher": "emwrap.aretomo.denoiset_pipeline",
            "label": "DenoisET Pipeline",
            "output": "DenoisET"
        }
    }

    # PACKAGES to group the jobs depending on their prefixes
    _packages = [
        { "name": "emwrap"},
        { "name": "warp", "prefixes": ["emw-warp"] },
        { "name": "relion", "prefixes": ["emw-relion", "relion."] },
        { "name": "pytom", "prefixes": ["emw-pytom"] },
        { "name": "aretomo", "prefixes": ["emw-aretomo", "emw-denoiset"] }
    ]

    @classmethod
    def _get_config(cls, key='', default=None):
        if cls._config is None:
            cls._config = json.loads(os.environ.get('EMWRAP_CONFIG', '{}'))

        return cls._config.get(key, default or {}) if key else cls._config

    @classmethod
    def get_jobs(cls):
        return cls._jobs

    @classmethod
    def get_programs(cls):
        return cls._get_config('programs')

    @classmethod
    def get_testdata(cls, name):
        return cls._get_config('testdata', {}).get(name, {})

    @classmethod
    def get_testdata_path(cls, name, validate=False):
        data_path = cls.get_testdata(name).get('path', '')
        if validate:
            if not os.path.exists(data_path):
                raise FileNotFoundError(f"Test data folder does not exist: {data_path}")
            if not os.path.isdir(data_path):
                raise NotADirectoryError(f"Test data folder is not a directory: {data_path}")
        return data_path

    @classmethod
    def get_packages(cls):
        return cls._packages

    @classmethod
    def get_queues(cls):
        return cls._get_config('queues')

    @classmethod
    def get_queues_dict(cls):
        return {q['name']: q for q in cls.get_queues()}

    @classmethod
    def get_queue(cls, queue_name):
        return cls.get_queues_dict().get(queue_name, None)

    @classmethod
    def get_job_conf(cls, jobtype):
        return cls.get_jobs().get(jobtype)

    @classmethod
    def is_job_visible(cls, jobtype):
        """Return whether a job appears in the processing menu (default: True)."""
        job_conf = cls.get_job_conf(jobtype) or {}
        return job_conf.get('visible', True)

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
    def get_workflows_dir(cls):
        return cls._get_config('workflows', '')

    @classmethod
    def list_workflows(cls):
        """Return title and description metadata for each configured workflow."""
        workflows_dir = cls.get_workflows_dir()
        if not workflows_dir or not os.path.exists(workflows_dir):
            return []

        workflows = []
        for workflow_file in sorted(w for w in os.listdir(workflows_dir)
                                     if w.endswith('.json')):
            workflow_id = os.path.splitext(workflow_file)[0]
            try:
                workflow_def = cls.get_workflow(workflow_id)
                title = workflow_def.get('title') or workflow_def.get('name', workflow_id)
                description = workflow_def.get('description', '')
            except Exception:
                title = workflow_id
                description = ''

            workflows.append({
                'id': workflow_id,
                'file': workflow_file,
                'title': title,
                'description': description,
            })

        return workflows

    @classmethod
    def save_workflow(cls, workflowId, workflowDef):
        workflowFile = cls.get_workflow_file(workflowId)

        if not os.path.exists(workflowFile):
            raise Exception(f"Workflow file: {Color.red(workflowFile)} does not exists.")

        with open(workflowFile, 'w') as f:
            json.dump(workflowDef, f, indent=4)
            f.write('\n')

        return workflowFile

    @classmethod
    def resolve_launcher(cls, launcher):
        """Expand job launcher specs that reference emwrap modules.

        If the launcher value starts with 'emwrap.' it is treated as a Python
        module path and prefixed with the EMWRAP program launcher from config.
        """
        if not launcher or not launcher.startswith('emwrap.'):
            return launcher

        emwrap_launcher = cls.get_programs().get('EMWRAP', {}).get('launcher', '')
        if not emwrap_launcher:
            raise Exception(
                "EMWRAP launcher not found in EMWRAP_CONFIG['programs'] "
                f"but required for job launcher: {launcher}")

        return f"{emwrap_launcher} {launcher}"

    @classmethod
    def get_job_launcher(cls, jobtype):
        job_conf = cls.get_job_conf(jobtype)
        if not job_conf:
            return None

        launcher = job_conf.get('launcher', None)
        if launcher:
            return cls.resolve_launcher(launcher)
        return None

    @classmethod
    def print_config(cls):
        print(json.dumps(cls._get_config(), indent=4))

    @classmethod
    def get_scripts_dir(cls):
        cls.scripts_dir = os.environ.get('SCRIPTS', '')
        return cls.scripts_dir

    @classmethod
    def get_launcher_info(cls, item):
        raw_launcher = item.get('launcher', '')
        launcher = raw_launcher

        if not launcher:
            return {
                'launcher': '',
                'program': '',
                'arguments': '',
                'display_program': '',
                'display': 'MISSING launcher.',
                'exists': False,
                'status': 'error',
                'status_label': 'Missing'
            }

        if launcher.startswith('emwrap.'):
            try:
                launcher = cls.resolve_launcher(launcher)
            except Exception as exc:
                return {
                    'launcher': raw_launcher,
                    'program': '',
                    'arguments': '',
                    'display_program': '',
                    'display': str(exc),
                    'exists': False,
                    'status': 'error',
                    'status_label': 'Unresolved'
                }

        parts = launcher.split()
        program = parts[0]
        arguments = ' '.join(parts[1:])
        display_program = program
        scripts_dir = cls.get_scripts_dir()

        if scripts_dir and program.startswith(scripts_dir):
            display_program = program.replace(scripts_dir, '$SCRIPTS')

        exists = os.path.exists(program)

        return {
            'launcher': launcher,
            'program': program,
            'arguments': arguments,
            'display_program': display_program,
            'display': ' '.join(p for p in [display_program, arguments] if p),
            'exists': exists,
            'status': 'ok' if exists else 'error',
            'status_label': 'OK' if exists else 'Missing executable'
        }

    @classmethod
    def get_config_report(cls):
        conf = cls._get_config()

        if not conf:
            raise Exception("Configuration is not valid.")

        scripts_dir = cls.get_scripts_dir()
        workflows_dir = conf.get('workflows', '')
        workflows_exists = bool(workflows_dir) and os.path.exists(workflows_dir)
        workflow_files = []
        workflow_rows = []

        if workflows_exists:
            workflow_files = sorted(w for w in os.listdir(workflows_dir)
                                    if w.endswith('.json'))
            workflow_rows = [
                {
                    'name': workflow_file,
                    'workflow_id': os.path.splitext(workflow_file)[0]
                }
                for workflow_file in workflow_files
            ]

        required_keys = ['jobs', 'programs', 'forms']
        for key in required_keys:
            if not conf.get(key, None):
                raise Exception(f"Configuration is not valid: '{key}' is required.")

        forms_dir = conf.get('forms', '')
        forms_exists = bool(forms_dir) and os.path.exists(forms_dir)

        def _count_value(value):
            if isinstance(value, (dict, list, tuple, set)):
                return len(value)
            return 0

        summary = [
            {
                'label': 'SCRIPTS',
                'value': scripts_dir or 'NO SCRIPTS DIR SET',
                'status': 'ok' if scripts_dir else 'warning',
                'status_label': 'Configured' if scripts_dir else 'Unset',
                'validation': 'Displayed by check_config',
                'details': 'Used to shorten launcher paths.'
            },
            {
                'label': 'WORKFLOWS',
                'value': workflows_dir or 'NO WORKFLOWS DIR SET',
                'status': 'ok' if workflows_exists else 'error' if workflows_dir else 'warning',
                'status_label': 'OK' if workflows_exists else 'Missing dir' if workflows_dir else 'Unset',
                'validation': 'Directory must exist when configured',
                'details': f'{len(workflow_files)} workflow files found' if workflows_exists
                           else 'No workflows directory configured' if not workflows_dir
                           else 'WORKFLOWS DIR DOES NOT EXIST'
            },
            {
                'label': 'FORMS',
                'value': forms_dir or 'MISSING',
                'status': 'ok' if forms_exists else 'warning' if forms_dir else 'error',
                'status_label': 'OK' if forms_exists else 'Path missing' if forms_dir else 'Missing',
                'validation': 'Required by check_config',
                'details': 'Directory existence is shown for convenience.'
            },
            {
                'label': 'JOBS',
                'value': f"{_count_value(conf.get('jobs', {}))} configured",
                'status': 'ok',
                'status_label': 'OK',
                'validation': 'Required by check_config',
                'details': 'Configured processing job types.'
            },
            {
                'label': 'PROGRAMS',
                'value': f"{_count_value(conf.get('programs', {}))} configured",
                'status': 'ok',
                'status_label': 'OK',
                'validation': 'Required by check_config',
                'details': 'Configured external program launchers.'
            }
        ]

        job_rows = []
        for job_name, job_conf in sorted(conf.get('jobs', {}).items()):
            launcher_info = cls.get_launcher_info(job_conf)
            form_file = cls.get_job_form_file(job_name)
            job_rows.append({
                'name': job_name,
                'launcher': launcher_info['display'],
                'launcher_status': launcher_info['status'],
                'launcher_status_label': launcher_info['status_label'],
                'form_file': form_file,
                'form_exists': os.path.exists(form_file),
                'form_status': 'ok' if os.path.exists(form_file) else 'warning',
                'form_status_label': 'OK' if os.path.exists(form_file) else 'Missing form'
            })

        program_rows = []
        for program_name, program_conf in sorted(conf.get('programs', {}).items()):
            launcher_info = cls.get_launcher_info(program_conf)
            program_rows.append({
                'name': program_name,
                'launcher': launcher_info['display'],
                'launcher_status': launcher_info['status'],
                'launcher_status_label': launcher_info['status_label']
            })

        return {
            'summary': summary,
            'workflow_files': workflow_files,
            'workflow_rows': workflow_rows,
            'job_rows': job_rows,
            'program_rows': program_rows
        }
    
    @classmethod
    def check_config(cls):
        """ Check if the current configuration is valid. """
        conf = cls._get_config()

        if not conf:
            raise Exception("Configuration is not valid.")

        report = cls.get_config_report()
        summary = {row['label']: row for row in report['summary']}

        if scripts_dir := cls.get_scripts_dir():
            print(f"\n{Color.cyan('SCRIPTS')}={Color.bold(scripts_dir)}")
        else:
            print(f"\n{Color.cyan('SCRIPTS')}={Color.red(summary['SCRIPTS']['value'])}")

        workflows_dir = conf.get('workflows', 'NO WORKFLOWS DIR SET')   
        if workflows_dir:
            if os.path.exists(workflows_dir):
                print(f"\n{Color.cyan('WORKFLOWS')}={Color.bold(workflows_dir)}")
                for workflow in report['workflow_files']:
                    print(f"  {Color.blue(workflow)}")
            else:
                print(f"\n{Color.cyan('WORKFLOWS')}={Color.red('WORKFLOWS DIR DOES NOT EXIST')}")
        else:
            print(f"\n{Color.cyan('WORKFLOWS')}={Color.red('NO WORKFLOWS DIR SET')}")

        cls.check_job_launchers(conf)
        cls.check_programs(conf)

    @classmethod
    def _check_launcher(cls, item):
        launcher_info = cls.get_launcher_info(item)

        if launcher_info['launcher']:
            color = Color.green if launcher_info['exists'] else Color.red
            launcher_line = ' '.join(
                p for p in [
                    color(launcher_info['display_program']),
                    launcher_info['arguments']
                ] if p
            )
        else:
            launcher_line = Color.red(f"MISSING launcher.")

        return launcher_line

    @classmethod
    def check_job_launchers(cls, conf):
        print(f"\n>>> {Color.warn('JOB LAUNCHERS')}")


        headers = ["JOB", "LAUNCHER", "FORM"]
        format_str = u'{:<30}{:<70}{:<40}'
        print('\n' + format_str.format(*headers))

        for jobName, jobConf in conf.get('jobs', {}).items():
            launcher_line = cls._check_launcher(jobConf)
            print(format_str.format(jobName, launcher_line, ''))

    @classmethod
    def check_programs(cls, conf):
        print(f"\n>>> {Color.warn('PROGRAMS')}")

        headers = ["PROGRAM", "LAUNCHER"]
        format_str = u'{:<30}{:<70}'
        print('\n' + format_str.format(*headers))
        for programName, programConf in conf.get('programs', {}).items():
            launcher_line = cls._check_launcher(programConf)    
            print(format_str.format(programName, launcher_line))


    @classmethod
    def main(cls):
        p = argparse.ArgumentParser(
            prog='emw',
            description='emwrap config manager')

        g = p.add_mutually_exclusive_group()

        g.add_argument('--print', '-p', action='store_True',
                       help="Print the existing configuration.")
        g.add_argument('--form', '-f', metavar='JOB_TYPE',
                       help="Print the corresponding form for this job type.")
        g.add_argument('--check', '-c', action='store_True',
                       help="Check the current configuration is valid.")

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

        elif args.check:
            cls.check_config()
            print(Color.green("Configuration is valid."))
