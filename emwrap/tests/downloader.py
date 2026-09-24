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

import shutil
import subprocess


class BaseDownloader:
    """Base for the online CryoEM database downloaders (EMPIAR, EMDB)."""

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def _wget(self, url, dest_dir, skip_existing=True):
        if shutil.which('wget') is None:
            raise RuntimeError('wget is required but was not found in PATH')

        # -c resumes partial downloads; with -N, complete up-to-date files are skipped.
        cmd = ['wget', '--show-progress', '-c']
        if skip_existing:
            cmd.append('-N')
        cmd.extend(['-q', '-nd', '-P', dest_dir, url])

        try:
            subprocess.run(cmd, check=True)
        except subprocess.CalledProcessError as err:
            raise RuntimeError(f'wget failed for {url} (exit code {err.returncode})') from err
