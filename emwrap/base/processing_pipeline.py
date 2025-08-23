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
                              Acquisition)


class ProcessingPipeline(Pipeline, FolderManager):
    """ Subclass of Pipeline that is commonly used to run programs.

    This class will define a workingDir (usually os.getcwd)
    and an output dir where all output should be generated.
    It will also add some helper functions to manipulate file
    paths relative to the working dir.
    """
    MIC_ID = re.compile('(?P<prefix>\w+)-(?P<id>\d{6})')

    def __init__(self, input_args):
        self._input_args = input_args
        args = input_args[self.name]
        self._args = args
        workingDir = args.get('working_dir', os.getcwd())
        outputDir = args.get('output', None)
        scratchDir = args.get('scratch', None)
        Pipeline.__init__(self, debug=args.get('debug', False))
        self.workingDir = self.__validate(workingDir, 'working')
        self.outputDir = self.__validate(outputDir, 'output')
        self.scratchDir = self.__validate(scratchDir, 'scratch') if scratchDir else None

        # Relative prefix from the working dir to output dir
        self.outputPrefix = os.path.relpath(self.outputDir, self.workingDir)

        FolderManager.__init__(self, outputDir)

        self.tmpDir = self.join('tmp')
        self.info = {
            'inputs': [],
            'outputs': {},
            'runs': [],
            'summary': {},
            'batches': {}
        }
        self.infoFile = self.join('info.json')
        # Lock used when requiring single thread running output generation code
        self.outputLock = threading.Lock()

    @property
    def intpus(self):
        return self.info['intputs']

    @property
    def outputs(self):
        return self.info['outputs']

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

    def __dumpArgs(self):
        argStr = json.dumps(self._input_args, indent=4) + '\n'
        with open(self.join('input_args.json'), 'w') as f:
            f.write(argStr)

    def get_arg(self, argDict, key, envKey, default=None):
        """ Get an argument from the argDict or from the environment.

        Args:
            argDict: arguments dict from where to get the 'key' value
            key: string key of the argument name in argDict
            envKey: string key of the environment variable
            default: default value if not found in argDict or environ
        """
        return argDict.get(key, os.environ.get(envKey, default))

    def prerun(self):
        """ This method will be called before the run. """
        pass

    def postrun(self):
        """ This method will be called after the run. """
        pass

    def __file(self, suffix):
        """ Generate status files with Relion convention. """
        # First remove all previous status files
        for fn in os.listdir(self.path):
            if fn.startswith('RELION_JOB_'):
                os.remove(self.join(fn))

        with open(self.join(f'RELION_JOB_{suffix}'), 'w'):
            pass

    def __abort(self, signum, frame):
        self.__file('EXIT_ABORTED')
        sys.exit(0)

    @staticmethod
    def do_clean():
        return int(os.environ.get('EMWRAP_CLEAN', 1)) > 0

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
            self.__dumpArgs()
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
            return row.rlnMicrographMovieName

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
        return Acquisition(self._input_args['acquisition'])

    @staticmethod
    def getInputArgs(progName, inputName):
        p = argparse.ArgumentParser(prog=progName)
        p.add_argument('--json',
                       help="Input all arguments through this JSON file.")
        p.add_argument(f'--{inputName}', '-i')
        p.add_argument('--output', '-o')
        p.add_argument('--j', help="Just to ignore the threads option from Relion")

        args = p.parse_args()

        with open(args.json) as f:
            input_args = json.load(f)

            prog_args = input_args[progName]

            if inputValue := getattr(args, inputName):
                prog_args[inputName] = inputValue

            if outputValue := getattr(args, 'output'):
                prog_args['output'] = outputValue

            return input_args

    @classmethod
    def runFromArgs(cls):
        input_args = ProcessingPipeline.getInputArgs(cls.name, cls.input_name)
        p = cls(input_args)
        p.run()
