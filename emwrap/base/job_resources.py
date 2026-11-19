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

""" CPUs and GPUs used by a job, from its params (Relion's native ones or
the emwrap 'cpus' and 'gpus' params). """


def get_relion_gpu_list(gpu_ids):
    """ Different GPU ids (sorted) in Relion's --gpu syntax, where ':'
    separates MPI processes and ',' threads (e.g. "0,1:0,1" -> [0, 1]). """
    return sorted({int(t) for mpi_part in str(gpu_ids).split(':')
                   for t in mpi_part.split(',') if t.strip()})


def get_job_gpu_list(params):
    """ Different GPU ids (sorted) used by a job, from Relion's native
    gpu_ids param (none if use_gpu is No) or from the emwrap gpus param. """
    from .processing_pipeline import ProcessingPipeline

    # Relion jobs only use their gpu_ids if use_gpu is set
    if str(params.get('use_gpu', '')).strip().lower() in ('no', 'false', '0'):
        return []
    gpu_ids = params.get('gpu_ids')
    if gpu_ids is not None and str(gpu_ids).strip():
        return get_relion_gpu_list(gpu_ids)
    return sorted(set(ProcessingPipeline.get_gpu_list(params.get('gpus', ''))))


def count_job_cpus(params):
    """ Number of CPUs of a job (MPIs x threads), from the Relion
    nr_mpi/nr_threads params or the emwrap cpus param ("N" or "MxT"). """
    mpi, threads = 1, 1
    if 'nr_mpi' in params or 'nr_threads' in params:
        mpi = int(params.get('nr_mpi', 1))
        threads = int(params.get('nr_threads', 1))
    elif cpus_str := params.get('cpus', ''):
        if 'x' in str(cpus_str):
            mpi, threads = map(int, cpus_str.split('x'))
        else:
            mpi, threads = int(cpus_str), 1
    return mpi * threads
