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
import socket
import getpass
import argparse
import subprocess

from emtools.utils import Color

from emwrap.base import ProcessingConfig


# Fixed location of the emhub instance used by 'emh-tomo --run'.
INSTANCE_DIR = os.path.expanduser('~/.emhub/instances/tomo')

# Source checkouts updated by 'emh-tomo --update', found under EMSTACK_HOME.
SOURCE_REPOS = ('emtools', 'emhub', 'emwrap')

# User automatically logged in when running 'emh-tomo --run', via emhub's
# EMHUB_LOGGED_USER environment variable (see emhub/emhub/__init__.py,
# '_login_user_from_env'). This is the special admin user created by
# emhub's DataManager.create_admin().
RUN_LOGGED_USER = 'admin'


class EMhubTomo:
    """ Entry point for the 'emh-tomo' command. """

    @classmethod
    def _run_config(cls, action):
        """ Run one of the config actions:
            'list'          -> print the current configuration
            'check'         -> validate the current configuration
            'form:JOB_TYPE' -> print the form for the given job type
        """
        if action == 'list':
            ProcessingConfig.print_config()

        elif action == 'check':
            ProcessingConfig.check_config()
            print(Color.green("Configuration is valid."))

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
                "Expected one of: 'list', 'check', 'form:JOB_TYPE'.")

    @classmethod
    def _copy_missing_templates(cls, templates_dir, target_dir):
        """ Copy every '*.template' file found directly under 'templates_dir'
        into 'target_dir', stripping the '.template' suffix. Existing files
        in 'target_dir' are never overwritten.

        Prints one line per template: copied ones in green, skipped
        (already existing) ones in red. Returns the list of template file
        names found.
        """
        template_files = sorted(
            f for f in os.listdir(templates_dir) if f.endswith('.template'))

        for template_file in template_files:
            name = template_file[:-len('.template')]
            src_file = os.path.join(templates_dir, template_file)
            dst_file = os.path.join(target_dir, name)

            if os.path.exists(dst_file):
                print(f"    {Color.red('EXISTS, skipped')}  {name}")
            else:
                shutil.copy2(src_file, dst_file)
                print(f"    {Color.green('COPIED')}           {name}")

        return template_files

    @classmethod
    def _update_config(cls):
        """ Set up the local configuration files and 'scripts' folder from
        the templates shipped with the code -- existing files are never
        overwritten:
          - copy 'emwrap.bashrc' into the current directory
          - create the local 'scripts' folder (if missing) and copy into it
            any missing script template

        Prints one line per template: copied ones in green, skipped
        (already existing) ones in red.
        """
        config_dir = ProcessingConfig.get_config_dir()
        if not os.path.isdir(config_dir):
            raise Exception(f"Config directory not found: {config_dir}")

        cwd = os.path.abspath('.')
        print(f">>> Updating configuration files in: {Color.bold(cwd)}")
        config_templates = cls._copy_missing_templates(config_dir, cwd)
        if not config_templates:
            print(Color.red(f"    No config templates found in {config_dir}"))

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

        script_templates = cls._copy_missing_templates(templates_dir, target_dir)
        if not script_templates:
            print(Color.red(f"    No script templates found in {templates_dir}"))

    @classmethod
    def _update_source(cls):
        """ Update (git pull --prune) each of the emtools/emhub/emwrap
        source checkouts found under EMSTACK_HOME (exported by the
        generated 'bashrc' activation script). This replaces the old
        standalone 'update.sh' script.
        """
        emstack_home = os.environ.get('EMSTACK_HOME')
        if not emstack_home:
            raise Exception(
                "EMSTACK_HOME is not set. Make sure the installation "
                "environment has been activated (e.g. 'source bashrc') "
                "before running 'emh-tomo --update'.")

        for repo in SOURCE_REPOS:
            repo_dir = os.path.join(emstack_home, repo)
            print(f">>> Updating {Color.green(repo)}...")
            if not os.path.isdir(repo_dir):
                print(f"    {Color.red('NOT FOUND, skipped')}  {repo_dir}")
                continue
            subprocess.run(['git', 'pull', '--prune'], cwd=repo_dir, check=True)

        print(Color.green("\nAll updates completed successfully!"))

    @classmethod
    def _run_update(cls):
        """ Run the '--update' action: set up/refresh the local
        configuration files and 'scripts' folder (previously done via
        '--config update'), and pull the latest changes for the
        emtools/emhub/emwrap source checkouts (previously done by the
        standalone 'update.sh' script).
        """
        cls._update_config()
        cls._update_source()

    @classmethod
    def _copy_processing_extras(cls, instance_dir):
        """ Copy the processing 'extra' files shipped with emhub (templates
        and blueprint code used by the tomography UI) into a freshly
        created instance. Requires EMSTACK_HOME to be set, which points to
        the folder containing the emtools/emhub/emwrap checkouts (exported
        by the generated 'bashrc' activation script).
        """
        emstack_home = os.environ.get('EMSTACK_HOME')
        if not emstack_home:
            raise Exception(
                "EMSTACK_HOME is not set. Make sure the installation "
                "environment has been activated (e.g. 'source bashrc') "
                "before running 'emh-tomo --run'.")

        extras_src = os.path.join(emstack_home, 'emhub', 'extras', 'processing')
        if not os.path.isdir(extras_src):
            raise Exception(f"Processing extras not found: {extras_src}")

        extras_dst = os.path.join(instance_dir, 'extra')
        print(f">>> Copying processing extras into: {Color.bold(extras_dst)}")
        shutil.copytree(extras_src, extras_dst, dirs_exist_ok=True)

    @classmethod
    def _find_free_port(cls):
        """ Return a free TCP port in the user/ephemeral range.

        Binds a throwaway socket to port 0 and reads back the port the OS
        assigned to it -- the standard, portable way to get an unused port
        without hardcoding or scanning a range. There is a small race
        between closing this socket and Flask binding the same port, but
        that is an accepted limitation of this approach.
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('', 0))
            return s.getsockname()[1]

    @classmethod
    def _print_port_banner(cls, port):
        """ Print a very visible banner announcing the port the server is
        about to use, including a ready-to-use ssh tunnel command for
        connecting from a client machine when this is run on a remote or
        HPC host.
        """
        host = socket.gethostname()
        user = getpass.getuser()
        sep = Color.bold('=' * 70)
        ssh_tunnel_cmd = f"ssh -NL {port}:localhost:{port} {user}@{host}"
        http_url = f"http://localhost:{port}"
        lines = [
            '',
            sep,
            f"  {Color.bold('emh-tomo')} server starting on port: {Color.green(port)}",
            f"  Auto-logged in as user: {RUN_LOGGED_USER}",
            '',
            "  Running on a remote/HPC host? Tunnel from your client with:",
            f"      {Color.green(ssh_tunnel_cmd)}",
            '',
            f"  Then open: {Color.cyan(http_url)}",
            sep,
            '',
        ]
        print('\n'.join(lines))

    @classmethod
    def _run_run(cls):
        """ Run the emh-tomo instance at INSTANCE_DIR
        (~/.emhub/instances/tomo). If the instance does not exist yet, it
        is first created as a minimal emhub instance (via
        'emh-data --create_minimal') and the processing 'extra' files
        shipped with emhub are copied into it.

        The server is started on a free port chosen automatically (see
        _find_free_port()), announced with a visible banner so the port
        can be used to set up an ssh tunnel if needed. The RUN_LOGGED_USER
        (admin) is automatically logged in for every request, via emhub's
        EMHUB_LOGGED_USER environment variable, so no login step is needed
        for this local/HPC use case.
        """
        if not os.path.exists(INSTANCE_DIR):
            print(f">>> Instance not found, creating a minimal instance at: "
                  f"{Color.bold(INSTANCE_DIR)}")
            subprocess.run(['emh-data', '--create_minimal', INSTANCE_DIR],
                           check=True)
            cls._copy_processing_extras(INSTANCE_DIR)
        else:
            print(f">>> Using existing instance at: {Color.bold(INSTANCE_DIR)}")

        run_script = os.path.join(INSTANCE_DIR, 'run.sh')
        if not os.path.exists(run_script):
            raise Exception(f"Instance run script not found: {run_script}")

        port = cls._find_free_port()
        cls._print_port_banner(port)

        print(f">>> Running instance: {Color.bold(INSTANCE_DIR)}")
        env = os.environ.copy()
        # 'flask run' (invoked with no explicit --port in run.sh) reads its
        # default port from FLASK_RUN_PORT when set.
        env['FLASK_RUN_PORT'] = str(port)
        # emhub's '_login_user_from_env' before_request hook (see
        # emhub/emhub/__init__.py) logs this user in automatically on every
        # request when EMHUB_LOGGED_USER is set, so no login page is shown
        # for this local/HPC use case.
        env['EMHUB_LOGGED_USER'] = RUN_LOGGED_USER
        sys.exit(subprocess.call(['bash', run_script], env=env))

    @classmethod
    def main(cls):
        p = argparse.ArgumentParser(
            prog='emh-tomo',
            description='emwrap tomography installer and configuration manager')

        g = p.add_mutually_exclusive_group()
        g.add_argument('--config', '-c', metavar='ACTION',
                       help="Manage the emwrap configuration. ACTION is one of: "
                            "'list' (print the current configuration), "
                            "'check' (validate the current configuration), or "
                            "'form:JOB_TYPE' (print the form for the given job "
                            "type).")
        g.add_argument('--update', '-u', action='store_true',
                       help="Copy any missing configuration file "
                            "(emwrap.bashrc) into the current directory, "
                            "create ./scripts if missing and copy any "
                            "missing script template into it, and pull the "
                            "latest changes (git pull --prune) for the "
                            "emtools/emhub/emwrap source checkouts.")
        g.add_argument('--run', '-r', action='store_true',
                       help=f"Run the emh-tomo instance at {INSTANCE_DIR}. "
                            "If it does not exist yet, it is created as a "
                            "minimal emhub instance and the processing extra "
                            "files shipped with emhub are copied into it.")

        args = p.parse_args()

        if args.run:
            cls._run_run()
        elif args.update:
            cls._run_update()
        elif args.config is not None:
            cls._run_config(args.config)
        else:
            p.print_help(sys.stderr)
            sys.exit(1)


if __name__ == '__main__':
    EMhubTomo.main()
