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
from uuid import uuid4

from emtools.utils import Process, Color, Path, FolderManager
from emtools.jobs import BatchManager, Args, Pipeline
from emtools.metadata import Table, Column, StarFile, StarMonitor, TextFile


class ProcessingPipeline(Pipeline, FolderManager):
    """ Subclass of Pipeline that is commonly used to run programs.

    This class will define a workingDir (usually os.getcwd)
    and an output dir where all output should be generated.
    It will also add some helper functions to manipulate file
    paths relative to the working dir.
    """
    def __init__(self, **kwargs):
        workingDir = kwargs.pop('working_dir', os.getcwd())
        outputDir = kwargs.pop('output', None)
        scratchDir = kwargs.pop('scratch', None)
        Pipeline.__init__(self, debug=kwargs.get('debug', False))
        self.workingDir = self.__validate(workingDir, 'working')
        self.outputDir = self.__validate(outputDir, 'output')
        self.scratchDir = self.__validate(scratchDir, 'scratch') if scratchDir else None

        # Relative prefix from the working dir to output dir
        self.outputPrefix = os.path.relpath(self.outputDir, self.workingDir)

        FolderManager.__init__(self, outputDir)

        self.tmpDir = self.join('tmp')
        self.batchesInfo = {}  # Keep track of batches info
        self._batchesInfoFile = self.join('batches_info.json')
        # Lock used when requiring single thread running output generation code
        self.outputLock = threading.Lock()

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
            print(f"Cleaning {Color.bold(tmpDir)}")
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

    def prerun(self):
        """ This method will be called before the run. """
        pass

    def postrun(self):
        """ This method will be called after the run. """
        pass

    def __file(self, suffix):
        with open(self.join(f'RELION_JOB_EXIT_{suffix}'), 'w'):
            pass

    def __abort(self, signum, frame):
        self.__file('ABORTED')
        sys.exit(0)

    def run(self):
        try:
            signal.signal(signal.SIGINT, self.__abort)
            signal.signal(signal.SIGTERM, self.__abort)
            self.__create_tmp()
            self.prerun()
            if os.path.exists(self._batchesInfoFile):
                with open(self._batchesInfoFile) as f:
                    self.batchesInfo = json.load(f)
            Pipeline.run(self)
            self.postrun()
            if int(os.environ.get('EMWRAP_CLEAN', 1)):
                self.__clean_tmp()
            else:
                print(f"Temporary directory was not deleted, "
                      f"remove it with the following command: \n"
                      f"{Color.bold('rm -rf %s' % self.tmpDir)}")
            self.__file('SUCCESS')
        except Exception as e:
            self.__file('FAILURE')
            traceback.print_exc()

    def addMoviesGenerator(self, inputStar, batchSize, inputTimeOut=3600):
        """ Add a generator that monitor input movies from a
        given STAR file and group in batches. """
        def _movie_filename(row):
            return row.rlnMicrographMovieName

        monitor = StarMonitor(inputStar, 'movies',
                              _movie_filename, timeout=inputTimeOut)

        batchMgr = BatchManager(batchSize, monitor.newItems(), self.tmpDir,
                                itemFileNameFunc=_movie_filename)

        return self.addGenerator(batchMgr.generate)

    def updateBatchInfo(self, batch):
        """ Update general info with this batch and write json file. """
        self.batchesInfo[batch.id] = batch.info
        with open(self._batchesInfoFile, 'w') as f:
            json.dump(self.batchesInfo, f, indent=4)

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


