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

from emtools.utils import (Process, Color, Pretty, FolderManager, Timer,
                           ResourceMonitor)
from emtools.jobs import BatchManager, Args, Pipeline
from emtools.hpc import ClusterJob, ComputingEnv
from emtools.metadata import (Table, Column, StarFile, StarMonitor, TextFile,
                              Acquisition, RelionStar)

from .config import ProcessingConfig
from .job_resources import get_job_gpu_list, count_job_cpus


def banner(lines):
    """ Box of asterisks around the given lines. """
    width = max(80, max(len(line) for line in lines) + 4)
    border = '*' * width
    rows = '\n'.join(f"* {line.ljust(width - 4)} *" for line in lines)
    return f"{border}\n{rows}\n{border}"


def launcher_banner(variable, launcher):
    """ Box of asterisks with the launcher variable and its path,
    marked as NOT FOUND if it is not a file nor a command in the PATH. """
    lines = [f"{variable} launcher: {launcher}"]
    if not launcher or not (os.path.exists(launcher) or shutil.which(launcher)):
        lines.append("NOT FOUND")
    return banner(lines)


def print_launcher(variable, launcher):
    print(f"\n{launcher_banner(variable, launcher)}\n", flush=True)


class ProcessingPipeline(Pipeline, FolderManager):
    """ Subclass of Pipeline that is commonly used to run programs.

    This class will define a workingDir (usually os.getcwd)
    and an output dir where all output should be generated.
    It will also add some helper functions to manipulate file
    paths relative to the working dir.
    """
    MIC_ID = re.compile(r'(?P<prefix>\w+)-(?P<id>\d{6})')

    def __init__(self, args, output):
        self._args = Args(args)
        workingDir = args.get('working_dir', os.getcwd())
        scratchDir = (args.get('scratch_dir') or ProcessingConfig.get_scratch_dir())
        Pipeline.__init__(self, debug=args.get('debug', False))
        self.workingDir = self.__validate(workingDir, 'working')
        self.outputDir = self.__validate(output, 'output')
        # The scratch might only exist in the compute nodes running the
        # batches (EMWRAP_CONFIG['validate_paths'] = false)
        if scratchDir and ProcessingConfig.validate_paths():
            self.__validate(scratchDir, 'scratch')
        self.scratchDir = scratchDir or None

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
        # (runInfo, timer) of the run in progress, see __endRun
        self._currentRun = None
        # Resource monitor process of the run in progress (see profile_interval)
        self._monitor = None
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

        if self.scratchDir and os.path.exists(self.scratchDir):
            scratchTmp = tempfile.mkdtemp(dir=self.scratchDir)
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
        with open(self.join('note.txt'), 'a') as f:
            e = ''
            logStr = ' \\\n'.join("%s %s" % (k, v) for k, v in args.items())
            f.write(f"{logStr}\n\n")
            f.flush()

    def batch_execute(self, label, batch, args,
                      logfile=None, logcmd=True, launcher=None, call=True,
                      work_dir=None, check=True, outputs=None):
        """ Shortcut to execute a batch using the internal launcher.
        Args:
            label: label to identify the batch
            batch: batch object
            args: arguments to pass to the program
            logfile: logfile to write the logs, by default it will be the batch folder + 'run.out'
            logcmd: if True, the command will be logged
            launcher: launcher to use, by default it will use the internal launcher for this Pipeline instance
            call: if True, the program will be executed, otherwise only logs will be written
            work_dir: optional project-root-relative folder to run the command from
            check: if True (default), raise an exception if the program could
                not be run (batch.error) or exited with a non-zero code, so
                the job is marked as failed instead of succeeded.
            outputs: optional list of files that the program should have
                generated (absolute or relative to the batch folder), an
                exception is raised if any of them is missing (only if check).

        Returns:
            The exit code of the program, or None if it was not executed.
            With check=False, batch.execute stores exceptions in batch.error
            instead of raising them, so callers that need to detect a failed
            run should check both this value and batch.error.
        """
        logfile = logfile or self.join('run.out')
        launcher = launcher or self._get_launcher()
        print_launcher(getattr(self, 'PROGRAM', None) or label, launcher)
        returncode = None
        # Clear any error from a previous command of the same batch,
        # so it is not reported again for this one
        batch.info.pop('error', None)
        with batch.execute(label):
            if logcmd:
                self.log_cmd(args)
            if call:
                cwd = True
                if work_dir is not None:
                    cwd = os.path.join(self.workingDir,
                                       self.toProjectPath(work_dir))
                returncode = batch.call(launcher, args, logfile=logfile, cwd=cwd)

        if check and call:
            self.check_execution(label, batch, returncode, outputs=outputs,
                                 logfile=logfile)
        return returncode

    @staticmethod
    def check_execution(label, batch, returncode, outputs=None, logfile=None):
        """ Raise an exception if a command executed in a batch failed:
        an error stored in the batch, a non-zero exit code or missing
        output files (absolute or relative to the batch folder). """
        logStr = f", check {logfile}" if logfile else ''
        if batch.error:
            raise Exception(f"{label}: failed with error:\n{batch.error}")
        if returncode:
            raise Exception(f"{label}: failed with exit code {returncode}{logStr}")
        if missing := [fn for fn in (outputs or [])
                       if not os.path.exists(batch.join(fn))]:
            raise Exception(f"{label}: missing output files: "
                            f"{', '.join(missing)}{logStr}")

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
        self.__endRun('aborted')
        self.__file('EXIT_ABORTED')
        sys.exit(0)

    def profile_interval(self):
        """ Seconds between resource usage samples of the job: its 'profile'
        param (Compute > Launchers > Profile in the form) or, if it is
        empty, the EMWRAP_PROFILE variable. A positive number turns the
        profiling on with that interval, 0 (or none set) turns it off. An
        invalid value is reported and taken as 0. """
        value = str(self._args.get('profile', None) or '').strip()
        source = "'profile' param"
        if not value:
            value = os.environ.get('EMWRAP_PROFILE', '').strip()
            source = 'EMWRAP_PROFILE'
        try:
            interval = float(value or 0)
        except ValueError:
            interval = float('nan')
        if not interval >= 0:  # Invalid, negative or nan
            print(f"WARNING: Invalid {source}='{value}', expected the "
                  f"seconds between samples (0 for no profiling)", flush=True)
            return 0.0
        return interval

    def __startProfile(self, runInfo):
        """ Start the resource monitor of this run, if profile_interval > 0.
        It writes profile/runNN.csv in the job folder, see ResourceMonitor.
        A failure here is only logged, it must never stop the job. """
        if not (interval := self.profile_interval()):
            return
        try:
            n = len(self.info['runs']) + 1
            csvFile = os.path.join('profile', f'run{n:02d}.csv')
            os.makedirs(self.join('profile'), exist_ok=True)
            gpus = ComputingEnv.monitor_gpus(runInfo.get('computing') or {})
            gpusStr = gpus if isinstance(gpus, str) else ', '.join(map(str, gpus))
            self.log(f"Resource usage every {interval} s (job GPUs: "
                     f"{gpusStr or 'none'}) -> {self.join(csvFile)}", flush=True)
            self._monitor = ResourceMonitor.launch(
                os.getpid(), self.join(csvFile), interval, gpus)
            runInfo['profile'] = {'file': csvFile, 'interval': interval}
        except Exception as e:
            self._monitor = None
            self.log(f"WARNING: Could not start the resource monitor: {e}")

    def __endProfile(self, runInfo):
        """ Stop the resource monitor and store its summary in runInfo. """
        if self._monitor is None:
            return
        monitor, self._monitor = self._monitor, None
        try:
            ResourceMonitor.terminate(monitor)
            profile = runInfo['profile']
            csvFile = self.join(profile['file'])
            # The node GPUs (nvidia-smi index) monitored for the job ones
            profile['gpus'] = ResourceMonitor.csv_gpus(csvFile)
            profile['summary'] = summary = ResourceMonitor.summarize(csvFile)
            self.log('\n'.join(ResourceMonitor.summary_lines(summary)))
        except Exception as e:
            self.log(f"WARNING: Could not summarize the resource usage: {e}")

    def __endRun(self, status):
        """ Store end time, elapsed and status of the current run in
        info.json, also when the run fails or is aborted. """
        if self._currentRun is None:
            return
        runInfo, t = self._currentRun
        self._currentRun = None
        self.__endProfile(runInfo)
        runInfo['end'] = Pretty.now()
        runInfo['elapsed'] = str(t.getElapsedTime())
        runInfo['status'] = status
        computing = runInfo.get('computing') or {}
        if cluster := computing.get('cluster'):
            # Memory used by the cluster job, while it is still running
            if memory := ClusterJob.memory(cluster):
                computing['memory'] = memory
                self.log(f"Cluster job {cluster['job_id']} memory: "
                         f"max {memory.get('max')}, avg {memory.get('avg')}")
        try:
            self.readInfo()
            runs = self.info['runs']
            if runs and runs[-1].get('start') == runInfo['start']:
                runs[-1] = runInfo
            else:
                runs.append(runInfo)
            self.writeInfo()
        except Exception as e:
            self.log(f"WARNING: Could not store the run time in {self.infoFile}: {e}")

    @staticmethod
    def do_clean():
        return int(os.environ.get('EMWRAP_CLEAN', 1)) > 0

    @staticmethod
    def get_gpu_groups(gpus):
        """ Parse the emwrap 'gpus' param into GPU groups (lists of GPU ids),
        one group for each processing thread:
        1. A single number N: N GPUs, e.g. "2" -> [[0], [1]]
        2. A quoted number: a specific GPU id, e.g. '"2"' -> [[2]]
        3. Several values separated by spaces: specific GPU ids, one thread
           for each value (even if repeated, e.g. one cluster job each),
           e.g. "0 0 1" -> [[0], [0], [1]]
        4. Values separated by commas: GPU ids used by the same thread,
           e.g. "0,1 2,3" -> [[0, 1], [2, 3]]
        """
        gpus = str(gpus or '').strip()
        quoted = len(gpus) > 1 and gpus[0] == gpus[-1] and gpus[0] in '"\''
        if quoted:
            gpus = gpus[1:-1].strip()

        parts = gpus.split()
        if len(parts) == 1 and not quoted and ',' not in parts[0]:
            return [[i] for i in range(int(parts[0]))]

        return [[int(g) for g in p.split(',') if g.strip()] for p in parts]

    @staticmethod
    def count_gpus(gpus):
        """ Number of different GPU ids in the 'gpus' param (see get_gpu_groups),
        e.g. "0 0 0 0" -> 1, "0,1 0,1" -> 2, "4" -> 4. """
        groups = ProcessingPipeline.get_gpu_groups(gpus)
        return len({g for group in groups for g in group})

    @staticmethod
    def get_gpu_list(gpus, as_string=False):
        """ Get the list of GPUs from the 'gpus' param (all GPU ids of
        the groups from get_gpu_groups), e.g. "2" -> [0, 1], '"2"' -> [2]
        or "0 1" -> [0, 1].
        """
        gpu_list = [g for group in ProcessingPipeline.get_gpu_groups(gpus)
                    for g in group]

        if as_string:
            return ' '.join(str(g) for g in gpu_list)
        else:
            return gpu_list

    def computingInfo(self):
        """ Computing environment of this run (see ComputingEnv.collect),
        with the CPUs and GPUs set in the job params. It never fails. """
        def _safe(func, default=None):
            try:
                return func()
            except Exception:
                return default

        return ComputingEnv.collect(
            gpu_ids=_safe(lambda: get_job_gpu_list(self._args), []),
            cpus=_safe(lambda: count_job_cpus(self._args)))

    @classmethod
    def get_launcher(cls, packageName=None):
        """ Get a launcher script to 'launch' programs from
        certain packages (e.g. Relion, Warp). A launcher variable
        will be used to read the value from os.environ. """
        if packageName := packageName or getattr(cls, 'PROGRAM', None):
            if launcher := ProcessingConfig.get_programs().get(packageName, {}).get('launcher', ''):
                if not os.path.exists(launcher):
                    print_launcher(packageName, launcher)
                    raise Exception(f"{packageName} launcher '{launcher}' does not exists.")
            else:
                raise Exception(f"{packageName} not found in EMWRAP_CONFIG['programs']")

            return launcher
        else:
            raise Exception(f"Expecting packageName or cls.PROGRAM defined")

    def _get_launcher(self):
        return ProcessingPipeline.get_launcher(self.PROGRAM)

    def _register_output_only(self):
        """ This will be used mainly for development, when we don't want to 
        re-run the job, but we want to register the outputs. It might be also
        useful in case that the users could make modifications the output files
        and we want to update the results. 
        """
        return self._args.get('register_output_only', False)

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
            # The pending launch time (written by the ProjectManager when
            # launching) belongs to this run, to report its time in the queue
            if launched := self.info.pop('launched', None):
                runInfo['launched'] = launched
            runInfo['computing'] = computing = self.computingInfo()
            print(f"\n{banner(ComputingEnv.lines(computing))}\n", flush=True)
            self.__startProfile(runInfo)
            self.info['runs'].append(runInfo)
            self.writeInfo()
            self._currentRun = (runInfo, t)
            self.__file('RUNNING')
            self.__create_tmp()
            self.prerun()
            Pipeline.run(self)
            self.postrun()
            if ProcessingPipeline.do_clean():
                try:
                    self.__clean_tmp()
                except OSError as e:
                    # e.g. a process still writing in tmp, the outputs are fine
                    self.log(f"WARNING: Could not clean {self.tmpDir}: {e}")
            else:
                print(f"Temporary directory was not deleted, "
                      f"remove it with the following command: \n"
                      f"{Color.bold('rm -rf %s' % self.tmpDir)}")

            # Update info.json file with end time
            self.__endRun('finished')
            self.__file('EXIT_SUCCESS')

        except Exception as e:
            self.__endRun('failed')
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

    def inputTsStreaming(self):
        """ Return the 'input_timeout' param (in seconds). If greater than 0,
        the input tilt series should be monitored with inputTsMonitor. """
        return int(self._args.get('input_timeout', 0) or 0)

    def inputTsMonitor(self, inputStar, **kwargs):
        """ Return a StarMonitor of the tilt series ('global' table) of the
        input STAR file (e.g. from an import job running on-the-fly), that
        times out after 'input_timeout' seconds without new tilt series.
        kwargs are passed to the StarMonitor (e.g. tableKwargs). """
        return StarMonitor(inputStar, 'global', lambda row: row.rlnTomoName,
                           timeout=self.inputTsStreaming(), **kwargs)

    def addGpuProcessors(self, generator, getProcessingFunc, outputFunc):
        """ Add processors that will process the batches in parallel using the GPUs. """
        outputQueue = None

        self.log(f"Creating {len(self.gpuList)} processing threads.", flush=True)

        for gpu in self.gpuList:
            p = self.addProcessor(generator.outputQueue,
                                  getProcessingFunc(gpu),
                                  outputQueue=outputQueue)
            outputQueue = p.outputQueue

        self.addProcessor(outputQueue, outputFunc)

    def updateBatchInfo(self, batch):
        """ Update general info with this batch and write json file. """
        self.info['batches'][batch.id] = batch.info
        self.writeInfo()

    @staticmethod
    def readJobInfo(info_file, default=None):
        """Load a job info.json file safely.

        Returns ``default`` when the file is missing, empty, or invalid JSON.
        """
        if not info_file or not os.path.isfile(info_file):
            return default

        try:
            with open(info_file) as f:
                content = f.read().strip()
            if not content:
                return default
            return json.loads(content)
        except (OSError, json.JSONDecodeError):
            return default

    @staticmethod
    def writeJobInfo(info_file, info):
        """ Write a job info.json file atomically. """
        tmpFile = f'{info_file}.tmp'
        with open(tmpFile, 'w') as f:
            json.dump(info, f, indent=4)
        os.replace(tmpFile, info_file)

    @staticmethod
    def updateJobInfo(info_file, **kwargs):
        """ Update some keys of a job info.json file, e.g. the 'launched'
        time written when the job is launched, moved to the run when it starts. """
        info = ProcessingPipeline.readJobInfo(info_file, default=None) or {}
        info.update(kwargs)
        ProcessingPipeline.writeJobInfo(info_file, info)

    def readInfo(self):
        info = self.readJobInfo(self.infoFile, default=None)
        if info is not None:
            # Keep the default keys, info.json might only have the
            # 'launched' time written when the job was launched
            self.info.update(info)

    def writeInfo(self):
        """ Write file with internal information to info.json. """
        self.writeJobInfo(self.infoFile, self.info)

    def writeRelionOutputNodes(self, outputs):
        """ Write expected file by relion in case the jobs are launched
        from the Relion pipeliner. This function might be automated later
        based on the self.outputs. """
        t = Table(columns=['rlnPipeLineNodeName', 'rlnPipeLineNodeTypeLabel'])
        for oName, oType in outputs:
            t.addRowValues(oName, oType)
        with StarFile(self.join('RELION_OUTPUT_NODES.star'), 'w') as sf:
            sf.writeTable('pipeline_nodes', t, timeStamp=True,
                          version=RelionStar.PIPELINE_VERSION)

    def fixOutputPath(self, path):
        """ Add the output prefix to a path that is relative to
        the output folder, to make it relative to the working dir
        (compatible with Relion/Scipion project's path rules)
        """
        return os.path.join(self.outputPrefix, path)

    def toProjectPath(self, path):
        """Return a path relative to the project root (workingDir)."""
        if not path:
            return path
        path = os.path.normpath(str(path))
        if os.path.isabs(path):
            return os.path.relpath(path, self.workingDir)
        return path

    def _projectPathFunc(self, mapping=None):
        """ Function fixing one absolute path with the projectPath rules,
        with the mapping keys and the project roots resolved only once
        (relativizePaths applies it to many paths). """
        mapping = {os.path.realpath(k): v for k, v in (mapping or {}).items()}
        roots = [os.path.realpath(self.workingDir),
                 os.path.abspath(self.workingDir)]

        def _fixAbs(p):
            real = os.path.realpath(p)
            if real in mapping:
                return mapping[real]
            for path, root in ((real, roots[0]), (os.path.normpath(p), roots[1])):
                if path == root or path.startswith(root + os.sep):
                    return os.path.relpath(path, root)
            return p

        return _fixAbs

    def projectPath(self, path, mapping=None):
        """ Path to persist in the job outputs (STAR files, info.json, other
        JSON files): relative to the project root (workingDir) when it is
        inside the project, absolute when it is outside (e.g. programs or
        external inputs).

        - An empty value (None, '') is returned as is.
        - A relative path is taken as relative to the project root (the jobs
          run with the project root as current folder) and normalized. If it
          goes out of the project ('../x') it is made absolute instead.
        - An absolute path is first looked up in 'mapping' (absolute paths,
          e.g. of tmp files, to their final project path). Otherwise it is
          made relative to the project root if it is inside it, comparing
          after resolving symlinks (the same folder can be reached through
          different mounts) and also with the non-resolved root. Absolute
          paths outside the project are returned unchanged.

        Absolute paths are fine during the processing (e.g. for programs
        running in a tmp/scratch folder), see relativizePaths for whole
        dicts/lists (e.g. a loaded JSON) and relativizeStar for STAR files.
        Unlike toProjectPath, paths outside the project are kept absolute
        instead of becoming '../..' paths.
        """
        if not path:
            return path
        path = os.path.normpath(str(path))
        if not os.path.isabs(path):
            if path != os.pardir and not path.startswith(os.pardir + os.sep):
                return path
            path = os.path.normpath(
                os.path.join(os.path.abspath(self.workingDir), path))
        return self._projectPathFunc(mapping)(path)

    def relativizePaths(self, obj, mapping=None):
        """ Return a copy of obj (dict/list/tuple/str, e.g. a loaded JSON)
        where every absolute path string is fixed with projectPath (same
        'mapping'): made relative to the project root if inside it, left
        absolute otherwise. Relative strings are left untouched, since
        not every string value is a path (e.g. URLs, labels, numbers).
        """
        fixAbs = self._projectPathFunc(mapping)

        def _fix(v):
            if isinstance(v, dict):
                return {k: _fix(x) for k, x in v.items()}
            if isinstance(v, (list, tuple)):
                return type(v)(_fix(x) for x in v)
            if isinstance(v, str) and os.path.isabs(v):
                return fixAbs(v)
            return v

        return _fix(obj)

    def relativizeStar(self, starFile, mapping=None):
        """ Rewrite starFile (written by an external program) making its
        absolute paths relative to the project root (see relativizePaths).
        Returns True if the file was modified. """
        tables = StarFile.getTablesDict(starFile, guessType=False)
        changed = False
        newTables = {}
        for name, table in tables.items():
            newTable = Table(table.getColumnNames())
            for row in table:
                values = row._asdict()
                newValues = self.relativizePaths(values, mapping)
                changed = changed or newValues != values
                newTable.addRowValues(**newValues)
            newTables[name] = newTable

        if changed:
            with StarFile(starFile, 'w') as sf:
                for name, table in newTables.items():
                    sf.writeTable(name, table, computeFormat='left')
        return changed

    def projectFile(self, path):
        """Full path of a project-root-relative (or absolute) path, to open
        it whatever the current folder is."""
        return os.path.join(self.workingDir, self.toProjectPath(path))

    def projectExists(self, path):
        """Return True when a project-root-relative path exists on disk."""
        return os.path.exists(self.projectFile(path))

    def linkProjectPath(self, dest_folder, src_path, name=None):
        """Symlink src_path into dest_folder using project-root-relative paths."""
        src = self.toProjectPath(src_path)
        dest_folder = self.toProjectPath(dest_folder)
        base = name or os.path.basename(src)
        link_rel = os.path.join(dest_folder, base)
        target = os.path.relpath(src, dest_folder)
        link_full = os.path.join(self.workingDir, link_rel)
        os.makedirs(os.path.dirname(link_full), exist_ok=True)
        if os.path.lexists(link_full):
            os.remove(link_full)
        os.symlink(target, link_full)
        return base

    def fixOutputRow(self, row, *pathKeys):
        """ Fix all output paths in the row defined by pathKeys. """
        newValues = {k: self.fixOutputPath(getattr(row, k)) for k in pathKeys}
        return row._replace(**newValues)

    def log(self, msg, flush=False):
        print(f"{Pretty.now()}: >>> {msg}", flush=flush)

    def loadAcquisition(self, inputStar=None):
        if inputStar:
            acq = RelionStar.getAcquisition(inputStar)
        elif acq := self._args.get('acquisition'):
            acq = Acquisition(acq)
        else:
            acqArgs = {k.replace('acq.', ''): v for k, v in self._args.items()
                       if k.startswith('acq.')}
            if acqArgs:
                acq = Acquisition(acqArgs)
            elif os.path.exists(acqJson := os.path.join(self.workingDir,
                                                        'acquisition.json')):
                with open(acqJson) as f:
                    acq = Acquisition(json.load(f))
            else:
                raise Exception(
                    "Could not load acquisition parameters: no input STAR file, "
                    "acquisition args or acquisition.json found."
                )

        if gain := self._args.get('acq.gain'):
            if gain not in (None, ''):
                acq['gain'] = gain

        return acq

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


