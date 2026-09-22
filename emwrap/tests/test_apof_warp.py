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

import json
import os
import sys

from emtools.metadata import WarpPopulation
from emtools.utils import Color

from emwrap.base import ProjectManager
from .test_apof import TestApoF


class TestApoFWarp(TestApoF):
    workflow_template = TestApoF.get_workflow_template('apof-warp-tutorial-part1')

    # Jobs of each workflow size, in the order they need to be run.
    small_job_types = [
        'emw-import-ts',
        'emw-warp-mctf',
        'emw-warp-tsalign',
        'emw-warp-ctfrec',
    ]

    medium_job_types = [
        'emw-import-ts',
        'emw-pytom-create_template',
        'emw-warp-mctf',
        'emw-warp-tsalign',
        'emw-warp-ctfrec',
        'emw-pytom',
        'emw-warp-export_particles',
    ]

    large_job_types = medium_job_types + [
        'relion.initialmodel.tomo',
        'relion.refine3d.tomo',
        'emw-relion-mask_create',
    ]

    otf_job_types = [
        'emw-import-ts',
        'emw-warp-otf',
    ]

    job_types = small_job_types

    expected_outputs = {
        'emw-import-ts': 'tilt_series.star',
        'emw-pytom-create_template': ['emd_15854.mrc', 'emd_15854_mask.mrc'],
        'emw-warp-mctf': 'tilt_series.star',
        'emw-warp-tsalign': 'aligned_tilt_series.star',
        'emw-warp-ctfrec': 'tomograms.star',
        'emw-pytom': 'optimisation_set.star',
        'emw-warp-export_particles': 'optimisation_set.star',
        'relion.initialmodel.tomo': 'initial_model.mrc',
        'relion.refine3d.tomo': ['run_class001.mrc', 'run_data.star'],
        'emw-relion-mask_create': 'mask.mrc',
        'emw-warp-mtools_create': 'm/ApoF.population',
        'emw-warp-mcore': 'm/ApoF.population',
        'emw-warp-estimate_weights': 'm/ApoF.population',
        'emw-warp-mtools_resample': 'm/ApoF.population',
        'emw-warp-otf': ['aligned_tilt_series.star', 'tomograms.star'],
    }

    # Last job of part1, it creates the M population that part2 refines.
    population_job_type = 'emw-warp-mtools_create'

    population_name = 'ApoF'

    # Job types whose output population must differ from every earlier one.
    # EstimateWeights and MResample are left out on purpose: they update the
    # population without rewriting the species, so they legitimately produce
    # the same digest as their input.
    refine_job_types = ('emw-warp-mtools_create', 'emw-warp-mcore')

    @classmethod
    def set_args(cls, parser):
        super().set_args(parser)
        parser.add_argument(
            '--workflow', '-w', choices=['small', 'medium', 'large', 'full', 'otf'],
            default='small',
            help='Workflow size: small (preprocessing), medium (up to particle export), '
                 'large (medium plus the Relion refinement and mask), '
                 'full (part1 chained with the part2 M refinements), '
                 'or otf (preprocessing in OTF mode).')

    def _read_template_jobs(self, workflow_name):
        template = self.get_workflow_template(workflow_name)
        print(f"Loading workflow template: {Color.warn(template)}")
        with open(template) as f:
            return json.load(f)['jobs']

    def load_full_workflow_jobs(self):
        """Chain part1 and part2 into a single pipeline.

        Part2 refines the M population built by the last job of part1, so its
        population inputs are re-pointed at that job here rather than relying
        on both templates keeping the same job ids.
        """
        part1 = self._read_template_jobs('apof-warp-tutorial-part1')
        part2 = self._read_template_jobs('apof-warp-tutorial-part2')

        population_jobs = [j for j in part1
                           if j['jobtype'] == self.population_job_type]
        if len(population_jobs) != 1:
            raise ValueError(f"Expected one {self.population_job_type} job in part1, "
                             f"found {len(population_jobs)}")
        population_jobid = population_jobs[0]['jobid']

        part2_ids = {j['jobid'] for j in part2}
        if clashing := {j['jobid'] for j in part1} & part2_ids:
            raise ValueError(f"part1 and part2 share job ids: {sorted(clashing)}")

        jobs = list(part1)
        for job in part2:
            params = dict(job['params'])
            for key, value in params.items():
                # Population inputs that do not point inside part2 are the entry
                # points of the M refinement, connect them to part1.
                if key.endswith('.population') and value:
                    jobid = '/'.join(value.split('/')[:2])
                    if jobid not in part2_ids:
                        params[key] = value.replace(jobid, population_jobid, 1)
            jobs.append({**job, 'params': params})

        return jobs

    def load_workflow_jobs(self):
        if self.args.workflow != 'full':
            return super().load_workflow_jobs()

        jobs = self.load_full_workflow_jobs()
        # The base class pairs job_types with the loaded jobs to check their
        # outputs, and the M refinement repeats job types several times, so
        # job_types has to follow the chained jobs rather than the other way.
        self.job_types = [job['jobtype'] for job in jobs]
        return jobs

    def _check_job_outputs(self, pm, job_type, job_id):
        """Double-check that each M refinement updated its input population.

        A crashed refinement leaves behind the copy of the input population
        that the job makes before running, which looks like a valid output.
        """
        if job_type not in self.refine_job_types:
            return

        population = pm.join(job_id, 'm', f'{self.population_name}.population')
        self.assertTrue(os.path.isfile(population),
                        f"Job {job_id} is missing the output population")

        digest = WarpPopulation(population).digest()
        if previous := self._population_digests.get(digest):
            self.fail(f"Job {job_id} ({job_type}) produced a population "
                      f"identical to the one from {previous}, "
                      f"so it did not update it")
        self._population_digests[digest] = job_id

    def _run_workflow(self):
        """Select the jobs to run based on the workflow size."""

        self._population_digests = {}

        if self.args.workflow == 'medium':
            self.job_types = self.medium_job_types
        elif self.args.workflow == 'large':
            self.job_types = self.large_job_types
        elif self.args.workflow == 'otf':
            self.workflow_template = self.get_workflow_template('apof-warp-tutorial-otf')
            self.job_types = self.otf_job_types

        super()._run_workflow()


if __name__ == '__main__':
    TestApoFWarp.run_tests()
