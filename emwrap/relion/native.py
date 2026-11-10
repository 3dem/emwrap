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

from emtools.jobs import Batch
from emtools.metadata import StarFile
from emtools.utils import Path

from emwrap.base.project_lock import wait_project_lock_free
from emwrap.base.job_star import backup_job_star, restore_job_star

from .relion_base import RelionBasePipeline


class RelionNative(RelionBasePipeline):
    """ Class to run native Relion pipelines, using the relion_pipeliner. """
    name = 'emw-relion-native'

    def prerun(self):
        # relion_pipeliner rewrites job.star without the options it does not
        # know (queue.*, ...): keep a copy, read instead of job.star while it
        # exists (job_star_path), and restore it when the run ends. On
        # SIGTERM (e.g. Stop) ProcessingPipeline exits via sys.exit, so the
        # finally below also runs.
        job_star = self.join('job.star')
        job_dir = os.path.dirname(job_star)
        backup_job_star(job_dir)

        try:
            job = StarFile.getTableFromFile('job', job_star)[0]

            batch = Batch(id=job.rlnJobTypeLabel, path=self.workingDir)

            # relion_pipeliner takes the project lock itself; wait until it
            # is released (stale locks are broken) instead of removing it
            wait_project_lock_free(self.workingDir)

            # Get Relion's version
            self.batch_execute('version', batch, {'relion_pipeliner': '--version'},
                               check=False)

            # Run the job via the relion_pipeliner
            jobId = Path.addslash(self.outputDir)
            self.batch_execute('pipeliner', batch, {'relion_pipeliner': '', '--RunJobs': jobId})

            self.updateBatchInfo(batch)
        finally:
            restore_job_star(job_dir)


if __name__ == '__main__':
    RelionNative.main()
