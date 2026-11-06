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
job.star of a job and its EMwrap backup.

relion_pipeliner rewrites the job.star of the Relion native jobs it runs,
dropping the options it does not know (e.g. queue.name, queue.param.*).
Before running it, the job keeps a copy of the EMwrap job.star
(job.star.emwrap) and restores it when the run ends. While the copy exists
(the job is running, or its run was killed before restoring it) it is the
one with the job params, so readers must use job_star_path().
"""

import os
import shutil

JOB_STAR = 'job.star'
JOB_STAR_BACKUP = 'job.star.emwrap'


def job_star_path(job_dir):
    """ job.star with the EMwrap params of the job in job_dir: its backup
    while it exists, job.star otherwise. """
    backup = os.path.join(job_dir, JOB_STAR_BACKUP)
    if os.path.exists(backup):
        return backup
    return os.path.join(job_dir, JOB_STAR)


def backup_job_star(job_dir):
    """ Keep a copy of job.star before running relion_pipeliner. A backup
    left by an interrupted run is restored first: nothing wrote job.star
    since then (writing it removes the backup), so the job.star there is
    the one stripped by relion_pipeliner. """
    restore_job_star(job_dir)
    shutil.copy2(os.path.join(job_dir, JOB_STAR),
                 os.path.join(job_dir, JOB_STAR_BACKUP))


def restore_job_star(job_dir):
    """ Put back the EMwrap job.star from its backup, if any. """
    backup = os.path.join(job_dir, JOB_STAR_BACKUP)
    if os.path.exists(backup):
        # copyfile, not copy2: job.star gets a new mtime, so the caches
        # keyed by it (e.g. ProjectData) see that it changed
        shutil.copyfile(backup, os.path.join(job_dir, JOB_STAR))
        os.remove(backup)


def clear_job_star_backup(job_dir):
    """ Remove the backup, after writing a new job.star (it is outdated). """
    backup = os.path.join(job_dir, JOB_STAR_BACKUP)
    if os.path.exists(backup):
        os.remove(backup)
