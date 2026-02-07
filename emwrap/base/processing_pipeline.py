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
import tempfile
import shutil
import sys
import json
import signal
import traceback
import threading
import argparse
import re
from collections import defaultdict

from emtools.utils import Process, Color, Pretty, FolderManager, Timer
from emtools.jobs import BatchManager, Args, Pipeline
from emtools.metadata import (Table, Column, StarFile, StarMonitor, TextFile,
                              Acquisition, RelionStar)

from .config import ProcessingConfig

class ProcessingPipeline(Pipeline, FolderManager):
    """ Subclass of Pipeline that is commonly used to run programs.

    This class will define a workingDir (usually os.getcwd)
    and an output dir where all output should be generated.
    It will also add some helper functions to manipulate file
    paths relative to the working dir.
    """
    MIC_ID = re.compile('(?P<prefix>\w+)-(?P<id>\d{6})')

    def __init__(self, args, output):
        self._args = Args(args)
        workingDir = args.get('working_dir', os.getcwd())
        scratchDir = args.get('scratch', None)
        Pipeline.__init__(self, debug=args.get('debug', False))
        self.workingDir = self.__validate(workingDir, 'working')
        self.outputDir = self.__validate(output, 'output')
        self.scratchDir = self.__validate(scratchDir, 'scratch') if scratchDir else None

        # Relative prefix from the working dir to output dir
        self.outputPrefix = os.path.relpath(self.outputDir, self.workingDir)

        FolderManager.__init__(self, output)
        self.project = FolderManager(self.workingDir)

        self.tmpDir = self.join('tmp')
        self.info = {
            'inputs': {},
            'outputs': {},
            'runs': [],
            'summary': {},
            'batches': {}
        }
        self.infoFile = self.join('info.json')
        # Lock used when requiring single thread running output generation code
        self.outputLock = threading.Lock()

    @property
    def inputs(self):
        return self.info['inputs']

    @inputs.setter
    def inputs(self, value):
        """Setter method for the radius with validation."""
        if not isinstance(value, dict):
            raise ValueError("self.inputs should be a dict")
        self.info['inputs'] = value

    @property
    def outputs(self):
        return self.info['outputs']

    @outputs.setter
    def outputs(self, value):
        """Setter method for the radius with validation."""
        if not isinstance(value, dict):
            raise ValueError("self.outputs should be a dict")
        self.info['outputs'] = value

    def __validate(self, path, key):
        if not path:
            raise Exception(f'Invalid {key} directory: {path}')
        if not os.path.exists(path):
            raise Exception(f'Non-existing {key} directory: {path}')

        return path

    def __clean_tmp(self):
        tmp = tmpDir = self.tmpDir
        if os.path.exists(tmp):
            if os.path.islink(tmp):
                tmpDir = os.readlink(tmp)
                os.unlink(tmp)
            self.log(f"Cleaning {Color.bold(tmpDir)}")
            shutil.rmtree(tmpDir)

    def __create_tmp(self):
        self.__clean_tmp()

        if self.scratchDir:
            scratchTmp = tempfile.mkdtemp(prefix=self.scratchDir)
            Process.system(f"ln -s {scratchTmp} {self.tmpDir}")
        else:
            Process.system(f"mkdir {self.tmpDir}")

    def get_arg(self, argDict, key, envKey, default=None):
        """ Get an argument from the argDict or from the environment.

        Args:
            argDict: arguments dict from where to get the 'key' value
            key: string key of the argument name in argDict
            envKey: string key of the environment variable
            default: default value if not found in argDict or environ
        """
        return argDict.get(key, os.environ.get(envKey, default))

    def get_subargs(self, prefix, new_prefix=''):
        full_prefix = f'{prefix}.'
        def _new_key(k):
            return k.replace(full_prefix, new_prefix)

        return {_new_key(k): v
                for k, v in self._args.items() if k.startswith(full_prefix)}

    def log_cmd(self, args):
        """ Log a command to the logfile. """
        with open(self.join('commands.txt'), 'a') as f:
            e = ''
            logStr = ' \\\n'.join("%s %s" % (k, v) for k, v in args.items())
            f.write(f"{logStr}\n\n")
            f.flush()

    def batch_execute(self, label, batch, args, 
                      logfile=None, logcmd=True, launcher=None):
        """ Shortcut to execute a batch using the internal launcher. """
        logfile = logfile or self.join('run.out')
        launcher = launcher or self._get_launcher()
        print(f">>>> Using launcher: {launcher}", flush=True)
        with batch.execute(label):
            if logcmd:
                self.log_cmd(args)
            batch.call(launcher, args, logfile=logfile)

    def prerun(self):
        """ This method will be called before the run. """
        pass

    def postrun(self):
        """ This method will be called after the run. """
        pass

    @classmethod
    def output_file(cls, suffix, outputFolder):
        """ Generate status files with Relion convention. """
        # First remove all previous status files
        for fn in os.listdir(outputFolder):
            if fn.startswith('RELION_JOB_'):
                os.remove(os.path.join(outputFolder, fn))

        with open(os.path.join(outputFolder, f'RELION_JOB_{suffix}'), 'w'):
            pass

    def __file(self, suffix):
        self.output_file(suffix, self.path)

    def __abort(self, signum, frame):
        self.__file('EXIT_ABORTED')
        sys.exit(0)

    @staticmethod
    def do_clean():
        return int(os.environ.get('EMWRAP_CLEAN', 1)) > 0

    @staticmethod
    def get_gpu_list(gpus, as_string=False):
        """ Get the list of GPUs base on the following options:
        1. If a single number N
            List will be [0, 1..., N-1]
        2. If there is a space-separated list
            List will be gpus.split()
        3. Single specific gpu should be specified with "GPU_NUMBER"
        """
        if not gpus:
            return '' if as_string else []

        parts = gpus.split()
        if len(parts) > 1:
            gpu_list = [int(g) for g in parts]
        else:
            gpu_list = list(range(int(gpus)))

        if as_string:
            return ' '.join(str(g) for g in gpu_list)
        else:
            return gpu_list

    @classmethod
    def get_launcher(cls, packageName=None):
        """ Get a launcher script to 'launch' programs from
        certain packages (e.g. Relion, Warp). A launcher variable
        will be used to read the value from os.environ. """
        if packageName := packageName or getattr(cls, 'PROGRAM', None):
            if launcher := ProcessingConfig.get_programs().get(packageName, {}).get('launcher', ''):
                if not os.path.exists(launcher):
                    raise Exception(f"{packageName} launcher '{launcher}' does not exists.")
            else:
                raise Exception(f"{packageName} not found in EMWRAP_CONFIG['programs']")

            return launcher
        else:
            raise Exception(f"Expecting packageName or cls.PROGRAM defined")

    def _get_launcher(self):
        ProcessingPipeline.get_launcher(self.PROGRAM)

    def run(self):
        try:
            signal.signal(signal.SIGINT, self.__abort)
            signal.signal(signal.SIGTERM, self.__abort)
            t = Timer()
            start = Pretty.now()
            self.readInfo()
            runInfo = {
                'start': start,
                'end': None,
                'elapsed': None
            }
            self.info['runs'].append(runInfo)
            self.writeInfo()
            self.__file('RUNNING')
            self.__create_tmp()
            self.prerun()
            Pipeline.run(self)
            self.postrun()
            if ProcessingPipeline.do_clean():
                self.__clean_tmp()
            else:
                print(f"Temporary directory was not deleted, "
                      f"remove it with the following command: \n"
                      f"{Color.bold('rm -rf %s' % self.tmpDir)}")

            # Update info.json file with end time
            self.readInfo()
            self.info['runs'][-1] = runInfo
            runInfo['end'] = Pretty.now()
            runInfo['elapsed'] = str(t.getElapsedTime())
            self.writeInfo()

            self.__file('EXIT_SUCCESS')

        except Exception as e:
            self.__file('EXIT_FAILURE')
            traceback.print_exc()

    @staticmethod
    def micId(filePath):
        if m := ProcessingPipeline.MIC_ID.search(filePath):
            return m.groupdict()['id']
        return None

    def addMoviesGenerator(self, inputStar, outputStar, batchSize,
                           inputTimeOut=3600, queueMaxSize=None,
                           createBatch=True):
        """
        Add a generator that monitor input movies from a
        given STAR file and group in batches.

        Args:
            inputStar: input STAR file that will be monitored for incoming data
            outputStar: output STAR that will be used in the resume mode to avoid
                reprocessing already processed items
            batchSize: number of items that will trigger a new batch
            inputTimeOut: if there are no changes in the input STAR file after
                this number of seconds, the monitor will trigger the last batch.
            queueMaxSize: maximum number of batch that can be waiting
            createBatch: if True, the BatchManager will create the batch folder
        """
        def _movie_fn(row):
            for label in ['rlnMicrographMovieName', 'rlnMicrographName']:
                if value := getattr(row, label, None):
                    return value
            return None

        def _movie_micrograph_key(row):
            """ Return the id from movie or micrograph row. """
            for label in ['rlnMicrographName', 'rlnMicrographMovieName']:
                if value := getattr(row, label, None):
                    return ProcessingPipeline.micId(value)

        # Get the micrographs IDs to avoid processing again that movies
        # and use it for the StarMonitor blacklist
        if os.path.exists(outputStar):
            with StarFile(outputStar) as sf:
                blacklist = sf.getTable('micrographs')
        else:
            blacklist = []

        monitor = StarMonitor(inputStar, 'movies', _movie_micrograph_key,
                              timeout=inputTimeOut,
                              blacklist=blacklist)

        batchMgr = BatchManager(batchSize, monitor.newItems(), self.tmpDir,
                                itemFileNameFunc=_movie_fn,
                                createBatch=createBatch)

        return self.addGenerator(batchMgr.generate,
                                 queueMaxSize=queueMaxSize)

    def updateBatchInfo(self, batch):
        """ Update general info with this batch and write json file. """
        self.info['batches'][batch.id] = batch.info
        self.writeInfo()

    def readInfo(self):
        if os.path.exists(self.infoFile):
            with open(self.infoFile) as f:
                self.info = json.load(f)

    def writeInfo(self):
        """ Write file with internal information to info.json. """
        with open(self.infoFile, 'w') as f:
            json.dump(self.info, f, indent=4)

    def fixOutputPath(self, path):
        """ Add the output prefix to a path that is relative to
        the output folder, to make it relative to the working dir
        (compatible with Relion/Scipion project's path rules)
        """
        return os.path.join(self.outputPrefix, path)

    def fixOutputRow(self, row, *pathKeys):
        """ Fix all output paths in the row defined by pathKeys. """
        newValues = {k: self.fixOutputPath(getattr(row, k)) for k in pathKeys}
        return row._replace(**newValues)

    def log(self, msg, flush=False):
        print(f"{Pretty.now()}: >>> {msg}", flush=flush)

    def loadAcquisition(self):
        # FIXME: allow to read a default one
        with open('acquisition.json') as f:
            return Acquisition(json.load(f))

    @classmethod
    def loadParams(cls, inputArgs):
        """ Load params from JSON string, JSON file or Relion job.star file. """
        if any(c in inputArgs for c in ['[', ']', '{', '}', '"']):
            args = json.loads(inputArgs)
        elif inputArgs.endswith('.star'):
            args = RelionStar.read_jobstar(inputArgs)
        elif inputArgs.endswith('.json'):
            with open(inputArgs) as f:
                args = json.load(f)
        else:
            raise Exception(f"Unknown input args file type: {inputArgs}")

        return args

    @classmethod
    def main(cls):
        p = argparse.ArgumentParser(prog=cls.name)
        p.add_argument(f'--input_movies', '-i', metavar="ARGS",
                       help="Input all arguments through a JSON string, file or JOB.STAR file.")
        p.add_argument('--output', '-o', metavar='OUTPUT_DIR')
        p.add_argument('--j', '-j',
                       help="Just to ignore the threads option from Relion")

        args = p.parse_args()

        if len(sys.argv) == 1:
            p.print_help(sys.stderr)
            sys.exit(1)

        params, output = cls.loadParams(args.input_movies), getattr(args, 'output')
        # Let's use the --j for special parameters
        params['__j'] = args.j

        try:
            # Create the ProcessingPipeline instance and run it
            pp = cls(params, output)
            pp.run()
        except Exception as e:
            ProcessingPipeline.output_file('EXIT_FAILURE', output)
            traceback.print_exc()


