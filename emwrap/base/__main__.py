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
import shutil
import argparse

from emtools.utils import Color

from .config import ProcessingConfig


class EMhubTomo:
    """ Entry point for the 'emh-tomo' command. """

    @classmethod
    def _run_config(cls, action):
        """ Run one of the config actions:
            'list'          -> print the current configuration
            'check'         -> validate the current configuration
            'form:JOB_TYPE' -> print the form for the given job type
            'update'        -> create ./scripts (if missing) and copy any
                                missing script templates into it
        """
        if action == 'list':
            ProcessingConfig.print_config()

        elif action == 'check':
            ProcessingConfig.check_config()
            print(Color.green("Configuration is valid."))

        elif action == 'update':
            cls._run_config_update()

        elif action.startswith('form:'):
            jobtype = action.split(':', 1)[1]
            if not jobtype:
                raise Exception("Missing JOB_TYPE in '--config form:JOB_TYPE'.")
            if jobtype not in ProcessingConfig.get_jobs():
                raise Exception(
                    f"Job type: {jobtype} is not a known emwrap job type "
                    "(see ProcessingConfig.get_jobs()).")
            formFile = ProcessingConfig.get_job_form_file(jobtype)
            if not os.path.exists(formFile):
                raise Exception(f"Form file: {Color.red(formFile)} does not exists.")
            form = ProcessingConfig.get_job_form(jobtype)
            print(json.dumps(form, indent=4))

        else:
            raise Exception(
                f"Invalid --config value: '{action}'. "
                "Expected one of: 'list', 'check', 'update', 'form:JOB_TYPE'.")

    @classmethod
    def _run_config_update(cls):
        """ Create the local 'scripts' folder (if it does not exist yet)
        and copy into it any script template, shipped with the code, that
        is not already present -- existing scripts are never overwritten.

        Prints one line per template: copied ones in green, skipped
        (already existing) ones in red.
        """
        templates_dir = ProcessingConfig.get_scripts_templates_dir()
        if not os.path.isdir(templates_dir):
            raise Exception(
                f"Scripts templates directory not found: {templates_dir}")

        target_dir = os.path.abspath('scripts')
        created_dir = not os.path.isdir(target_dir)
        if created_dir:
            os.makedirs(target_dir)

        print(f">>> Updating scripts in: {Color.bold(target_dir)}")
        print(f"    {Color.green('CREATED')} scripts folder" if created_dir
              else "    scripts folder already exists")

        template_files = sorted(
            f for f in os.listdir(templates_dir) if f.endswith('.template'))

        if not template_files:
            print(Color.red(f"    No script templates found in {templates_dir}"))
            return

        for template_file in template_files:
            script_name = template_file[:-len('.template')]
            src_file = os.path.join(templates_dir, template_file)
            dst_file = os.path.join(target_dir, script_name)

            if os.path.exists(dst_file):
                print(f"    {Color.red('EXISTS, skipped')}  {script_name}")
            else:
                shutil.copy2(src_file, dst_file)
                print(f"    {Color.green('COPIED')}           {script_name}")

    @classmethod
    def main(cls):
        p = argparse.ArgumentParser(
            prog='emh-tomo',
            description='emwrap tomography installer and configuration manager')

        p.add_argument('--config', '-c', metavar='ACTION',
                       help="Manage the emwrap configuration. ACTION is one of: "
                            "'list' (print the current configuration), "
                            "'check' (validate the current configuration), "
                            "'update' (create ./scripts if missing and copy "
                            "any missing script templates into it), or "
                            "'form:JOB_TYPE' (print the form for the given job "
                            "type).")

        args = p.parse_args()

        if args.config is not None:
            cls._run_config(args.config)
        else:
            p.print_help(sys.stderr)
            sys.exit(1)


if __name__ == '__main__':
    EMhubTomo.main()
