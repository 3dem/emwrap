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
        'emw-warp-mtools_create',
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
            '--workflow', '-w',
            choices=['small', 'medium', 'large', 'full', 'full-continue', 'otf'],
            default='small',
            help='Workflow size: small (preprocessing), medium (up to particle export), '
                 'large (all of part1: medium plus the Relion refinement, mask '
                 'and M population), '
                 'full (part1 chained with the part2 M refinements), '
                 'full-continue (part2 M refinements only, continuing from the '
                 'Warp Create Population job of an existing project, by default '
                 'the current folder), or otf (preprocessing in OTF mode).')

    @classmethod
    def configure(cls, args):
        if args.workflow == 'full-continue' and args.project is None:
            args.project = os.getcwd()
        super().configure(args)

    def _read_template_jobs(self, workflow_name):
        template = self.get_workflow_template(workflow_name)
        print(f"Loading workflow template: {Color.warn(template)}")
        with open(template) as f:
            return json.load(f)['jobs']

    def _load_part2_jobs(self, population_jobid, other_ids=()):
        """Load part2 with its M refinement starting from population_jobid.

        Population inputs that do not point inside part2 are its entry points,
        so they are re-pointed at population_jobid rather than relying on the
        templates (or an existing project) keeping the same job ids.
        """
        part2 = self._read_template_jobs('apof-warp-tutorial-part2')
        part2_ids = {j['jobid'] for j in part2}
        if clashing := set(other_ids) & part2_ids:
            raise ValueError(f"part1 and part2 share job ids: {sorted(clashing)}")

        jobs = []
        for job in part2:
            params = dict(job['params'])
            for key, value in params.items():
                if key.endswith('.population') and value:
                    jobid = '/'.join(value.split('/')[:2])
                    if jobid not in part2_ids:
                        params[key] = value.replace(jobid, population_jobid, 1)
            jobs.append({**job, 'params': params})
        return jobs

    def load_full_workflow_jobs(self):
        """Chain part1 and part2 into a single pipeline."""
        part1 = self._read_template_jobs('apof-warp-tutorial-part1')
        population_jobs = [j for j in part1
                           if j['jobtype'] == self.population_job_type]
        if len(population_jobs) != 1:
            raise ValueError(f"Expected one {self.population_job_type} job in part1, "
                             f"found {len(population_jobs)}")

        return part1 + self._load_part2_jobs(population_jobs[0]['jobid'],
                                             {j['jobid'] for j in part1})

    def load_continue_workflow_jobs(self):
        """Load part2 connected to the last succeeded population job of the
        existing project, which is what part1 leaves behind."""
        pm = ProjectManager(self.project_path)
        pm.update()
        population_ids = [job.id for job in pm.get_workflow().jobs()
                          if job['jobtype'] == self.population_job_type
                          and job['status'] == 'Succeeded']
        if not population_ids:
            raise ValueError(f"No succeeded {self.population_job_type} job found "
                             f"in project: {self.project_path}")

        population_jobid = population_ids[-1]
        print(f"Continuing from population job: {Color.bold(population_jobid)}")
        # Seed the digests, so the first MCore is checked against its input
        self._check_job_outputs(pm, self.population_job_type, population_jobid)
        return self._load_part2_jobs(population_jobid)

    def load_workflow_jobs(self):
        if self.args.workflow == 'full':
            jobs = self.load_full_workflow_jobs()
        elif self.args.workflow == 'full-continue':
            jobs = self.load_continue_workflow_jobs()
        else:
            return super().load_workflow_jobs()

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

    def _get_run_params(self, pm, job_type, job_id):
        """Point species inputs at the species of the job's input population.

        Warp names each species folder with a generated suffix (e.g.
        ApoF_f2ca8cca), so the path saved in the template never matches the
        one created in a new run.
        """
        params = pm._readJobParams(pm._getJob(job_id))
        fixed = {}
        for key, value in params.items():
            population_key = key[:-len('species')] + 'population'
            if not (key.endswith('.species') and value and params.get(population_key)):
                continue
            population = params[population_key]
            species = WarpPopulation(pm.join(population)).Species[0]['path']
            fixed[key] = os.path.join(os.path.dirname(population), species)
            print(f"Setting {key} = {Color.bold(fixed[key])}")
        return fixed or None

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
