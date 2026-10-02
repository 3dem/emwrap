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
On-The-Fly (OTF) processing of EMhub sessions.

An OTF project is an emwrap project created from a workflow template
(config/workflows/*.json), whose jobs are filled with the session values
and then launched, each one when its input has some data.

The OTF class is generic and subclasses define the processing type:
    - SpaOTF: single-particle (import movies, preprocessing and 2D)
    - TomoOTF: tomography (import tilt series, Warp and AreTomo3)

Instance (facility) specific behaviour can be added in two ways:
    1. EMhub 'sessions' config, with job params that are applied in order
       (the last one wins):
         sconfig['otf']['job_params']
         sconfig['otf'][workflow]['job_params']
         sconfig['otf'][workflow]['microscopes'][microscope]['job_params']
    2. Subclassing a processing type and overriding customizeJobs (e.g. to
       write extra files that depend on the data). The class used by the
       command line can be set in sconfig['otf'][workflow]['emwrap_otf'],
       either a type ('spa' or 'tomo') or a 'module.Class' string.
"""

import os
import time
import json
import argparse
import importlib
from glob import glob

from emtools.utils import Color, FolderManager, Pretty
from emtools.metadata import StarFile
from emtools.image import Image

from emwrap.base import ProjectManager, ProcessingConfig


class OTF(FolderManager):
    """ Base class for the On-The-Fly processing of an EMhub session.

    Subclasses should define:
        WORKFLOW: default workflow template
        STEPS: list of (jobtype, input) to run the jobs in order, where
            input is None or a tuple (jobtype, star_file, table): the job
            will be launched once the table of the input job has some rows.
        OUTPUTS: dict jobtype -> [(star_file, table)] to report the status.
    and implement setupJobs to fill the jobs params with session values.
    """
    TYPE = None
    WORKFLOW = None
    STEPS = []
    OUTPUTS = {}

    def __init__(self, path, **kwargs):
        FolderManager.__init__(self, path)

    # ------------------- Creation -----------------------------------
    def create(self, session, sconfig, resources):
        cwd = os.getcwd()
        os.chdir(self.path)
        try:
            self._create(session, sconfig, resources)
        finally:
            os.chdir(cwd)

    def _create(self, session, sconfig, resources):
        """ Create the OTF project for this session.

        Args:
            session: EMhub session (dict)
            sconfig: EMhub 'sessions' config. The workflow template can be
                selected in sconfig['otf'][workflow]['emwrap_workflow'], as a
                workflow id or a JSON file (default: cls.WORKFLOW).
                Instance specific params for each job type (e.g. launchers
                or GPUs) can be defined in the config (see module doc), e.g.:
                {"emw-preprocessing": {"launcher_batch": "/path/to/script.sh"}}
            resources: EMhub resources, to find the microscope of the session
        """
        project = ProjectManager(self.path, create=True)
        project.clean()
        FolderManager(self.join('.slurm')).create()  # used by cluster launchers

        self._linkData(session['extra']['raw']['path'])
        ctx = self.createContext(session, sconfig, resources)

        workflow = self.loadWorkflowTemplate(ctx['wf_conf'].get('emwrap_workflow'))
        jobs = {j['jobtype']: j['params'] for j in workflow['jobs']}

        self.setupJobs(jobs, ctx)
        self.customizeJobs(jobs, ctx)

        # Config params are shared by workflows, only apply the ones of our jobs
        for params in self.configJobParams(ctx):
            for jobType, jobParams in params.items():
                if jobType in jobs:
                    jobs[jobType].update(jobParams)

        idMap = project.loadWorkflow(workflow=workflow)
        jobIds = {j['jobtype']: idMap[j['jobid']] for j in workflow['jobs']}

        self._dumpJson('acquisition.json', ctx['acq'])

        def _star(jobType, starFn):
            return os.path.join(jobIds[jobType], starFn)

        # Paths used by EMhub (and session workers) to show the session data
        sessionJson = {
            'otf_type': self.TYPE,
            'jobs': jobIds,
            'steps': self._stepsFromJobs(jobIds),
            'outputs': [_star(jobType, starFn)
                        for jobType, outputs in self.OUTPUTS.items()
                        for starFn, _ in outputs]
        }
        sessionJson.update(self.sessionInfo(jobIds))
        self._dumpJson('session.json', sessionJson)

        with open(self.join('README.txt'), 'a') as f:
            f.write(f'\n\n# OTF CREATED: {Pretty.now()}\n')
            f.write(f'# SESSION_ID = {session["id"]}\n\n')
            f.write('# Run the OTF (jobs are launched when their inputs are ready):\n')
            f.write('emh-tomo --launch emwrap.mix.otf --run\n\n')
            f.write('# or run the jobs one by one:\n')
            for jobId in jobIds.values():
                f.write(f'emw --run {jobId}\n')

    @staticmethod
    def getWorkflowConf(session, sconfig):
        """ Return the EMhub config of the OTF workflow selected for the session. """
        otf = session['extra'].get('otf', {})
        # Same (lowercase) workflow key used to launch the OTF command
        wf_conf = sconfig.get('otf', {}).get(otf.get('workflow', 'none').lower(), {})
        return wf_conf if isinstance(wf_conf, dict) else {}

    def createContext(self, session, sconfig, resources):
        """ Collect the session and config values used to setup the jobs. """
        micMap = {r['id']: r['name'] for r in resources}
        microscope = micMap[session['resource_id']]
        conf_acq = sconfig['acquisition'][microscope]
        wf_conf = self.getWorkflowConf(session, sconfig)

        acq = {k: float(v) for k, v in session['acquisition'].items()}
        acq['gain'] = None
        if gain_pattern := conf_acq.get('gain_pattern'):
            gains = glob(self.join('data', gain_pattern.format(microscope=microscope)))
            acq['gain'] = gains[-1] if gains else None

        return {
            'session': session,
            'microscope': microscope,
            'raw': session['extra']['raw'],
            'otf': session['extra'].get('otf', {}),
            'otf_conf': sconfig.get('otf', {}),
            'wf_conf': wf_conf,
            'conf_acq': conf_acq,
            'acq': acq
        }

    @staticmethod
    def getSetting(ctx, key, default=None):
        """ Get a setting from the workflow config or, if not there,
        from the microscope acquisition config. """
        for conf in [ctx['wf_conf'], ctx['conf_acq']]:
            if key in conf:
                return conf[key]
        return default

    def setupJobs(self, jobs, ctx):
        """ Fill jobs params (jobtype -> params) with the session values. """
        raise NotImplementedError

    def customizeJobs(self, jobs, ctx):
        """ Hook for instance specific changes of the jobs params.
        It is called after setupJobs and before applying the config params. """
        pass

    def configJobParams(self, ctx):
        """ Return the jobs params from the config, from general to specific. """
        micConf = ctx['wf_conf'].get('microscopes', {}).get(ctx['microscope'], {})
        return [conf.get('job_params', {})
                for conf in [ctx['otf_conf'], ctx['wf_conf'], micConf]]

    def sessionInfo(self, jobIds):
        """ Extra keys for session.json (e.g. paths used by EMhub). """
        return {}

    def _linkData(self, rawPath):
        self._remove('data')
        os.symlink(rawPath, self.join('data'))
        self.log(f"Raw data: {rawPath}")

    def _remove(self, fn):
        if os.path.lexists(self.join(fn)):
            self.log(f"Removing: {Color.warn(fn)}")
            os.remove(self.join(fn))

    def _dumpJson(self, fn, obj):
        fullFn = self.join(fn)
        self.log(f"Creating file: {Color.warn(fullFn)}")
        with open(fullFn, 'w') as f:
            json.dump(obj, f, indent=4)
            f.write('\n')

    def _loadSession(self):
        with open(self.join('session.json')) as f:
            return json.load(f)

    @classmethod
    def loadWorkflowTemplate(cls, workflow=None):
        """ Load the workflow template, either from a workflow id (from the
        emwrap config/workflows folder) or from a JSON file path.
        By default, cls.WORKFLOW is used. """
        workflow = workflow or cls.WORKFLOW
        if workflow.endswith('.json'):
            with open(os.path.expanduser(workflow)) as f:
                return json.load(f)
        return ProcessingConfig.get_workflow(workflow)

    # ------------------- Run and status ---------------------------------
    def _waitTable(self, starFn, tableName, sleep=30):
        """ Wait until the STAR file exists and has some rows in the table. """
        self.log(f"Waiting for {tableName} in {starFn}")
        while True:
            try:
                with StarFile(self.join(starFn)) as sf:
                    if sf.getTableSize(tableName):
                        return
            except Exception:
                pass
            time.sleep(sleep)

    def run(self):
        """ Run the OTF jobs (session.json steps), each one launched
        when its input has some data. """
        session = self._loadSession()
        project = ProjectManager(self.path)

        for step in self._getSteps(session):
            if wait := step['wait']:
                self._waitTable(*wait)
            project.runJob(step['job'])

    def _getSteps(self, session):
        """ Steps from session.json, or from STEPS for old projects. """
        return session.get('steps') or self._stepsFromJobs(session['jobs'])

    def _stepsFromJobs(self, jobIds):
        """ Steps to run (job and input to wait for) from STEPS. """
        return [{'job': jobIds[jobType],
                 'wait': [os.path.join(jobIds[inp[0]], inp[1]), inp[2]] if inp else None}
                for jobType, inp in self.STEPS]

    def status(self):
        def _log(logFn):
            msg = 'missing'
            if os.path.exists(logFn):
                s = os.stat(logFn)
                msg = f"{Pretty.size(s.st_size)}, {Pretty.elapsed(s.st_mtime)}"
            return f"{logFn} ({msg})"

        def _check_starfile(starFn, tableName):
            if not os.path.exists(starFn):
                color = Color.red
                msg = 'missing'
            else:
                with StarFile(starFn) as sf:
                    if tableName in sf.getTableNames():
                        n = sf.getTableSize(tableName)
                    else:
                        n = 0
                color = Color.green if n else Color.cyan
                msg = f"{n} items"
            return starFn, color(msg)

        session = self._loadSession()
        for jobType, outputs in self.OUTPUTS.items():
            runDir = self.join(session['jobs'][jobType])
            starMsgs = [_check_starfile(os.path.join(runDir, starFn), table)
                        for starFn, table in outputs]
            runOut = _log(os.path.join(runDir, 'run.out'))
            runErr = _log(os.path.join(runDir, 'run.err'))
            print(f"- {Color.bold(jobType): <20}")
            for starFn, msg in starMsgs:
                print(f"   {starFn:<35} {msg:<20}")
            print(f"   {runOut:<40}\n"
                  f"   {runErr:<40}")

    # ------------------- Class selection -----------------------------
    @staticmethod
    def getClass(name):
        """ Return the OTF class from a type ('spa', 'tomo')
        or from a 'module.Class' string. """
        if name in OTF_TYPES:
            return OTF_TYPES[name]
        moduleName, className = name.rsplit('.', 1)
        return getattr(importlib.import_module(moduleName), className)

    @classmethod
    def fromSession(cls, path, sessionJson=None, session=None, sconfig=None):
        """ Create the OTF instance from the session.json in the path
        or from the EMhub session config. """
        if session is not None:
            name = cls.getWorkflowConf(session, sconfig).get('emwrap_otf', 'spa')
        else:
            with open(sessionJson or os.path.join(path, 'session.json')) as f:
                name = json.load(f).get('otf_type') or 'spa'
        return cls.getClass(name)(path)


class SpaOTF(OTF):
    """ Single-particle On-The-Fly processing of an EMhub session.

    The project is created from the 'spa-otf' workflow template
    (config/workflows/spa-otf.json) with the following jobs:
        - emw-import-movies: import movies while they are acquired
        - emw-preprocessing: MotionCor, ctffind, crYOLO and extraction in batches
        - emw-rln2d: Relion 2D classification in batches
    Jobs are run through the ProjectManager, so they are launched with the
    central emwrap launcher (emh-tomo --launch) and use the programs
    defined in EMWRAP_CONFIG['programs'].
    """
    TYPE = 'spa'
    WORKFLOW = 'spa-otf'
    IMPORT = 'emw-import-movies'
    PREPROCESSING = 'emw-preprocessing'
    CLASS2D = 'emw-rln2d'

    STEPS = [
        (IMPORT, None),
        (PREPROCESSING, (IMPORT, 'movies.star', 'movies')),
        (CLASS2D, (PREPROCESSING, 'particles.star', 'particles'))
    ]
    OUTPUTS = {
        IMPORT: [('movies.star', 'movies')],
        PREPROCESSING: [('micrographs.star', 'micrographs'),
                        ('coordinates.star', 'coordinates'),
                        ('particles.star', 'particles')],
    }

    def setupJobs(self, jobs, ctx):
        acq = ctx['acq']
        input_movies = os.path.join('data', ctx['conf_acq']['images_pattern'])

        if movies := glob(self.join(input_movies)):
            first = movies[0]
            ctx['dims'] = Image.get_dimensions(first)
            self.log(f"Dimensions from: {first}: {ctx['dims']}")
        else:
            raise Exception(f"There are not movies in: {input_movies}")

        jobs[self.IMPORT].update({
            'in_movies': input_movies,
            'acq.gain': acq['gain'] or '',
            'acq.pixel_size': acq['pixel_size'],
            'acq.voltage': acq.get('voltage', 300),
            'acq.cs': acq.get('cs', 2.7),
            'acq.dose': acq['dose']
        })

        # crYOLO model selected when creating the session
        if cryolo_model := ctx['otf'].get('cryolo_model'):
            jobs[self.PREPROCESSING]['picking.model'] = cryolo_model

    def sessionInfo(self, jobIds):
        ppId = jobIds[self.PREPROCESSING]
        return {
            "movies": f"{jobIds[self.IMPORT]}/movies.star",
            "micrographs": f"{ppId}/micrographs.star",
            "coordinates": f"{ppId}/coordinates.star",
            "classes2d": jobIds[self.CLASS2D],
        }


class TomoOTF(OTF):
    """ Tomography On-The-Fly processing of an EMhub session.

    The project is created from the 'tomo-otf' workflow template
    (config/workflows/tomo-otf.json) with the following jobs:
        - emw-import-ts: import tilt series (from mdoc files) while acquired
        - emw-warp-otf: Warp motion/CTF, alignment and reconstruction
        - emw-aretomo3: AreTomo3 motion/CTF, alignment and reconstruction
    Both processing jobs use the imported tilt series as input and monitor
    it for new tilt series (with their 'input_timeout' param).

    Settings (from the workflow or microscope config):
        mdoc_pattern: pattern of mdoc files, relative to the data folder
        tilt_images: frames folder, relative to the data folder
        tilt_axis_angle: nominal tilt axis angle
    """
    TYPE = 'tomo'
    WORKFLOW = 'tomo-otf'
    IMPORT = 'emw-import-ts'
    WARP = 'emw-warp-otf'
    ARETOMO3 = 'emw-aretomo3'

    STEPS = [
        (IMPORT, None),
        (WARP, (IMPORT, 'tilt_series.star', 'global')),
        (ARETOMO3, (IMPORT, 'tilt_series.star', 'global'))
    ]
    OUTPUTS = {
        IMPORT: [('tilt_series.star', 'global')],
        WARP: [('aligned_tilt_series.star', 'global'),
               ('tomograms.star', 'global')],
        ARETOMO3: [('aligned_tilt_series.star', 'global'),
                   ('tomograms.star', 'global')],
    }

    def setupJobs(self, jobs, ctx):
        acq = ctx['acq']
        mdoc_files = os.path.join('data', self.getSetting(ctx, 'mdoc_pattern', '*.mdoc'))
        tilt_images = os.path.join('data', self.getSetting(ctx, 'tilt_images', ''))

        if not glob(self.join(mdoc_files)):
            raise Exception(f"There are not mdoc files in: {mdoc_files}")

        importParams = {
            'mdoc_files': mdoc_files,
            'tilt_images': tilt_images,
            'acq.gain': acq['gain'] or '',
            'acq.pixel_size': acq['pixel_size'],
            'acq.voltage': acq.get('voltage', 300),
            'acq.cs': acq.get('cs', 2.7),
            'acq.total_dose': acq['dose']
        }
        jobs[self.IMPORT].update(importParams)

        if tilt_axis := self.getSetting(ctx, 'tilt_axis_angle'):
            jobs[self.IMPORT]['tilt_axis_angle'] = tilt_axis
            # AreTomo3 TiltAxis is '<angle> <refine_flag>'
            at3 = jobs[self.ARETOMO3]
            refine = (at3.get('aretomo3.TiltAxis') or '').split()[1:] or ['0']
            at3['aretomo3.TiltAxis'] = f"{tilt_axis} {refine[0]}"

    def sessionInfo(self, jobIds):
        return {
            "tilt_series": f"{jobIds[self.IMPORT]}/tilt_series.star",
            "tomograms_warp": f"{jobIds[self.WARP]}/tomograms.star",
            "tomograms_aretomo3": f"{jobIds[self.ARETOMO3]}/tomograms.star",
        }


OTF_TYPES = {c.TYPE: c for c in [SpaOTF, TomoOTF]}


def main():
    p = argparse.ArgumentParser()
    g = p.add_mutually_exclusive_group()
    g.add_argument('--create', '-c', metavar='SESSION_ID',
                   help="Create new OTF project, previous files will be cleaned.")
    g.add_argument('--run', '-r', action='store_true')
    g.add_argument('--status', '-s', action='store_true')
    p.add_argument('--otf', metavar='TYPE_OR_CLASS',
                   help="OTF type ('spa', 'tomo') or 'module.Class' to "
                        "create the project. By default, it is taken from "
                        "the workflow config ('emwrap_otf') or 'spa'.")

    args = p.parse_args()
    path = os.getcwd()

    if args.create:
        # This option required that emhub client is properly configured
        from emhub.client import open_client
        with open_client() as dc:
            session = dc.get_session(int(args.create))
            sconfig = dc.get_config('sessions')
            resources = dc.get('resources').json()
        if args.otf:
            otf = OTF.getClass(args.otf)(path)
        else:
            otf = OTF.fromSession(path, session=session, sconfig=sconfig)
        otf.create(session, sconfig, resources)

    elif args.run:
        OTF.fromSession(path).run()

    elif args.status:
        OTF.fromSession(path).status()


if __name__ == '__main__':
    main()
