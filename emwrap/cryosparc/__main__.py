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
import json
import os.path
import argparse
import ast
import time
from collections import OrderedDict
import datetime as dt
import re
from pprint import pprint

from emtools.utils import Process, Color, System, Path, FolderManager
from emtools.metadata import EPU, SqliteFile, StarFile, Table
from emtools.jobs import Pipeline


class CryoSparc:
    STATUS_FAILED = "failed"
    STATUS_ABORTED = "aborted"
    STATUS_COMPLETED = "completed"
    STATUS_KILLED = "killed"
    STATUS_RUNNING = "running"
    STATUS_QUEUED = "queued"
    STATUS_LAUNCHED = "launched"
    STATUS_STARTED = "started"
    STATUS_BUILDING = "building"

    STOP_STATUSES = [STATUS_ABORTED, STATUS_COMPLETED, STATUS_FAILED, STATUS_KILLED]
    ACTIVE_STATUSES = [STATUS_QUEUED, STATUS_RUNNING, STATUS_STARTED,
                       STATUS_LAUNCHED, STATUS_BUILDING]

    def __init__(self, projId):
        self.projId = projId
        from cryosparc.tools import CryoSPARC, CommandClient
        cs_config = os.environ.get('CRYOSPARC_CONFIG', None)
        if cs_config is None:
            raise Exception('Please define CRYOSPARC_CONFIG="LICENSE|URL|PORT"')

        license, url, port = cs_config.split('|')
        print("\n>>> Using license: ", Color.green(license))
        print(">>> URL/port: ", Color.bold(f"{url}:{port}"))
        self._cli = CommandClient(host=url, port=port, headers={"License-ID": license})
        projInfo = self.cli('get_project', projId)
        print("\n", "=" * 20, Color.green(f"PROJECT: {projId}"), "=" * 20)
        pprint(projInfo)
        print("=" * 50, "\n")
        self.userId = projInfo['owner_user_id']
        lanes = self.cli('get_scheduler_lanes')
        pprint(lanes)

    def __call__(self, cmd, **kwargs):
        p = Process(self.csm, 'cli', cmd)
        lines = list(p.lines())

        try:
            for i, line in enumerate(lines):
                print(Color.cyan(i), Color.bold(line))
            return lines[0]
        except Exception as e:
            print(Color.red(f"Error: running command {cmd}"))
            print(e)

    def _argstr(self, args):
        return json.dumps(args).replace('true', 'True')

    def cli(self, function, *args, **kwargs):
        def _val(v):
            return Color.bold(json.dumps(v))

        argsStr = ','.join(_val(a) for a in args)
        sepStr = ', ' if argsStr else ''
        kwargsStr = ','.join("%s=%s" % (Color.cyan(k), _val(v)) for k, v in kwargs.items())
        print(f"\n{Color.green(function)}({argsStr}{sepStr}{kwargsStr})")
        func = getattr(self._cli, function)
        return func(*args, **kwargs)

    def job_status(self, jobId):
        """ Return the job status. """
        status = self.cli('get_job_status', project_uid=self.projId, job_uid=jobId)
        print(status)
        return status

    def job_wait(self, jobId):
        """ Wait for a job to complete (in any stop status). """
        while self.job_status(jobId) not in self.STOP_STATUSES:
            time.sleep(10)

    def job_run(self, wsId, jobType, args, inputs={}, wait=True):
        #cmd = (f'make_job("{jobType}", "{self.projId}", "{wsId}", "{self.userId}", None, None, None, '
        #       f'{self._argstr(args)}, {self._argstr(inputs)})')
        #jobId = self(cmd)
        # jobId = self._cli.make_job(job_type=jobType, project_uid=self.projId, workspace_uid=wsId,
        #                           user_id=self.userId, params=args, input_group_connects=inputs)
        jobId = self.cli('make_job',
                         job_type=jobType, project_uid=self.projId, workspace_uid=wsId,
                         user_id=self.userId, params=args, input_group_connects=inputs)
        #cmd = f'enqueue_job("{self.projId}", "{jobId}", "default", "{self.userId}")'
        #self(cmd)
        #self._cli.enqueue_job(project_uid=self.projId, user_id=self.userId, job_uid=jobId, lane='default')
        self.cli('enqueue_job',
                 project_uid=self.projId, user_id=self.userId, job_uid=jobId)
        if wait:
            self.job_wait(jobId)

        return jobId


def cryosparc_prepare(sessionFile, particlesStar):
    root = FolderManager('.')

    if root.exists('CS'):
        raise Exception("CS folder already exists. Remove it before running this command.")

    logger = Process.Logger(format="%(message)s", only_log=False)#True)

    csFolder = FolderManager(root.join('CS'))

    for folder in ['Micrographs', 'Movies']:
        csFolder.mkdir(folder)

    with open(sessionFile) as f:
        session = json.load(f)

    movStar = session['movies']
    micStar = session['micrographs']

    epuFolder = root.join(os.path.dirname(movStar), 'EPU', 'XML')


    logger.system(f"ln -s {csFolder.relpath(epuFolder)} {csFolder.join('XML')}")

    movTable = StarFile.getTableFromFile(movStar, 'movies')
    micTable = StarFile.getTableFromFile(micStar, 'micrographs')

    r = re.compile('(movie|micrograph)-(\d{6})')

    def _micKey(fn):
        """ Return movie/micrograph id from the name. """
        if m := r.search(fn):
            return m.groups()[1]

    micDict = {_micKey(row.rlnMicrographName): row for row in micTable}

    with StarFile('CS/particles.star', 'w') as sfOut:
        # We will ignore CTF values because is giving errors in that case
        ctfCols = ['rlnDefocusU', 'rlnDefocusV', 'rlnDefocusAngle', 'rlnCtfFigureOfMerit', 'rlnCtfMaxResolution']
        t = Table(['rlnMicrographName', 'rlnCoordinateX', 'rlnCoordinateY'])
        sfOut.writeHeader('particles', t)

        newDict = {}
        for rowMov in sorted(movTable, key=lambda r: r.TimeStamp):
            movFn = rowMov.rlnMicrographMovieName
            movKey = _micKey(movFn)
            print(f"{movKey} -> {movFn}")
            micRow = micDict.get(movKey, None)
            if micRow is None:
                print(Color.red(f'No micrograph found for movie: {movFn}'))
                continue

            ext = Path.getExt(movFn)
            if ext == '.tiff':
                ext = '.tif'  # Make it always 4 chars

            micFn = micDict[movKey].rlnMicrographName
            micName = f"mic_{movKey}.mrc"
            movRoot = f"mov_{movKey}"
            movName = f"{movRoot}{ext}"

            newDict[movKey] = (micName, movName)
            logger.system(f'ln -s ../../{micFn} CS/Micrographs/{micName}')
            logger.system(f'ln -s ../../{movFn} CS/Movies/{movName}')

        with StarFile(particlesStar) as sf:
            for row in sf.iterTable('particles'):
                micKey = _micKey(row.rlnMicrographName)
                if micKey in newDict:
                    micName, movName = newDict[micKey]
                    sfOut.writeRow(t.Row(micName, row.rlnCoordinateX, row.rlnCoordinateY))


def cryosparc_import(projId, dataRoot):
    if ':' in projId:
        projId, wksId = projId.split(':')
    else:
        wksId = 'W1'

    # FIXME!!!
    acq = {
        "psize_A": 1.297,  # 0.724,
        "accel_kv": 300,
        "cs_mm": 0.1,
        "total_dose_e_per_A2": 60,
        "amp_contrast": 2.7
    }

    cs = CryoSparc(projId)
    csRoot = os.path.join(dataRoot, 'CS')

    print(f">>> Importing data from: {Color.green(dataRoot)}")

    args = {
        "blob_paths": f"{csRoot}/Micrographs/mic_*.mrc",
        "parse_xml_files": True,
        "xml_paths": f"{csRoot}/XML/movie-*.xml",
        "mov_cut_prefix_xml": 4,
        "mov_cut_suffix_xml": 4,
        "xml_cut_prefix_xml": 6,
        "xml_cut_suffix_xml": 4
    }
    args.update(acq)
    del args['amp_contrast']  # This param is only valid for importing particles

    micsImport = cs.job_run(wksId, "import_micrographs", args)

    time.sleep(5)  # FIXME: wait for job completion
    args = {
        "ignore_blob": True,
        "particle_meta_path": f"{csRoot}/particles.star",
        "query_cut_suff": 4,
        "remove_leading_uid": True,
        "source_cut_suff": 4,
        "enable_validation": True,
        "location_exists": True,
    }
    args.update(acq)
    ptsImport = cs.job_run(wksId, "import_particles", args,
                          {'micrographs': f'{micsImport}.imported_micrographs'})


def main():
    p = argparse.ArgumentParser(prog='emw-cs')

    p.add_argument('--test', '-t', metavar='CRYOSPARC_PROJECT_ID',
                   help="Test connection to CryoSparc server. ")
    p.add_argument('--prepare', '-p', metavar=('SESSION_JSON', 'PARTICLES_STAR'), nargs=2,
                   help="Prepare a folder CS to be used to import movies, micrographs "
                        "and particles into CryoSparc. ")
    p.add_argument('--import_data', '-i', metavar=('CRYOSPARC_PROJECT_ID', 'DATA_ROOT'), nargs=2,
                   help="Import data from the CS folder into a "
                        "cryosparc project. ")

    args = p.parse_args()

    if projId := args.test:
        CryoSparc(projId)
    elif args.prepare:
        sessionJson, particlesStar = args.prepare
        cryosparc_prepare(sessionJson, particlesStar)
    elif args.import_data:
        projId, dataRoot = args.import_data
        cryosparc_import(projId, dataRoot)


if __name__ == '__main__':
    main()
