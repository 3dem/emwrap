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

    def prerun(self):
        # Let's get the job name from the job.star
        job_star = self.join('job.star')
        job = StarFile.getTableFromFile('job', job_star)

        batch = Batch(id=job.rlnJobTypeLabel, path=self.workingDir)

        # Remove .relion_lock directory
        if os.path.exists(self.join('.relion_lock')):
            shutil.rmtree(self.join('.relion_lock'))

        self.batch_execute('relion_pipeliner', batch, '--version')
        jobId = Path.addslash(self.outputDir)
        self.batch_execute('relion_pipeliner', batch, f'--RunJobs {jobId}')
       
        self.updateBatchInfo(batch)


if __name__ == '__main__':
    RelionNative.main()
