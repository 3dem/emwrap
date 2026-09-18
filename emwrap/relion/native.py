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
import shutil

from emtools.jobs import Batch
from emtools.metadata import StarFile
from emtools.utils import Path

from .relion_base import RelionBasePipeline


class RelionNative(RelionBasePipeline):
    """ Class to run native Relion pipelines, using the relion_pipeliner. """
    name = 'emw-relion-native'
    JOB_STAR_BACKUP = 'job.star.emwrap'

    def _restoreJobStar(self):
        """Restore EMwrap job.star after relion_pipeliner (drops unknown options)."""
        backup = self.join(self.JOB_STAR_BACKUP)
        job_star = self.join('job.star')
        if os.path.exists(backup):
            shutil.copy2(backup, job_star)
            os.remove(backup)

    def prerun(self):
        # Let's get the job name from the job.star
        job_star = self.join('job.star')
        backup = self.join(self.JOB_STAR_BACKUP)
        shutil.copy2(job_star, backup)

        try:
            job = StarFile.getTableFromFile('job', job_star)[0]

            batch = Batch(id=job.rlnJobTypeLabel, path=self.workingDir)

            # Remove .relion_lock directory
            if os.path.exists(self.join('.relion_lock')):
                shutil.rmtree(self.join('.relion_lock'))

            # Get Relion's version
            self.batch_execute('version', batch, {'relion_pipeliner': '--version'})

            # Run the job via the relion_pipeliner
            jobId = Path.addslash(self.outputDir)
            self.batch_execute('pipeliner', batch, {'relion_pipeliner': '', '--RunJobs': jobId})

            self.updateBatchInfo(batch)
        finally:
            self._restoreJobStar()


if __name__ == '__main__':
    RelionNative.main()
