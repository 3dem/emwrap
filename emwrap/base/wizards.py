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

""" Wizards: interactive helpers that set some parameters of a job form.

A wizard has three parts (see docs/wizards.md):

  - Wizard: the tool-agnostic logic. prepare() validates the inputs and
    resolves the data shown in the UI when the wizard is opened, finalize()
    validates the values selected by the user when OK is pressed. Its 'ui'
    names the emflow component that renders it.
  - WizardAdaptor: maps the params of a given job type to the wizard
    inputs (toWizard) and the wizard outputs back to params (fromWizard).
  - The 'wizards' entry of the job form JSON, that tells which adaptor to
    use and which params show the wizard button.

Wizards never save or run the job: OK only returns new values for some
form params, the user still saves or launches the job.
"""

import importlib
import os

from emtools.metadata import StarFile, RelionStar

from .config import ProcessingConfig
from .job_form import JobValidationError


class WizardError(JobValidationError):
    """ Raised by wizards and adaptors when they can not run, e.g. a missing
    input. The errors are shown to the user and the wizard is not opened. """


def to_float(value):
    """ Return value as float, or None if empty or not a number. """
    try:
        return None if value is None or str(value).strip() == '' else float(value)
    except (TypeError, ValueError):
        return None


def to_int(value):
    """ Return value as int (rounded), or None if empty or not a number. """
    v = to_float(value)
    return None if v is None else int(round(v))


def to_bool(value):
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ('yes', 'true', '1')


class Wizard:
    """ Base class of the wizards. """
    ui = None  # Name of the emflow component that renders the wizard
    label = ''

    def __init__(self, project):
        """ project: the ProjectManager (or any FolderManager) of the job,
        relative paths of the params are resolved from its path. """
        self.project = project

    def join(self, path):
        return path if os.path.isabs(path) else self.project.join(path)

    def prepare(self, inputs):
        """ Validate the inputs and resolve the data needed by the UI.
        Raise WizardError if the wizard can not run.

        Return a dict with:
            inputs: initial values of the editable fields
            data: read-only values (e.g. the input pixel size)
            warnings: list of messages shown in the dialog
        """
        return {'inputs': inputs, 'data': {}, 'warnings': []}

    def finalize(self, outputs, prepared):
        """ Validate the values selected in the UI when OK is pressed.
        prepared is the result of prepare() for the current form params,
        computed again on the server so read-only values are not taken
        from the client. Raise WizardError if the values are not valid.

        Return a dict with:
            outputs: the validated values, passed to the adaptor
            warnings: list of messages
        """
        return {'outputs': outputs, 'warnings': []}


class WizardAdaptor:
    """ Maps the params of a job type from/to the inputs/outputs of a wizard.
    The same wizard can be used by different job types (e.g. Relion and
    Warp), each one with its own adaptor. """
    wizard = None  # Wizard subclass

    def __init__(self, project):
        self.project = project
        self.warnings = []

    def join(self, path):
        return path if os.path.isabs(path) else self.project.join(path)

    def toWizard(self, params):
        """ Return the wizard inputs from the form params. """
        raise NotImplementedError

    def fromWizard(self, outputs, params):
        """ Return {param_name: value} with the form params to update from
        the wizard outputs. Messages for the user can be added to
        self.warnings. """
        raise NotImplementedError


class Wizards:
    """ Find the wizards of the job forms and run them. """

    @classmethod
    def get_form_wizards(cls, jobForm):
        return (jobForm or {}).get('wizards', [])

    @classmethod
    def get_spec(cls, jobType, wizardId):
        for spec in cls.get_form_wizards(ProcessingConfig.get_job_form(jobType)):
            if spec['id'] == wizardId:
                return spec
        raise WizardError(f"Wizard '{wizardId}' is not defined "
                          f"for job type '{jobType}'.")

    @classmethod
    def get_adaptor(cls, spec, project):
        """ Instantiate the adaptor class from its full name in the spec,
        e.g. emwrap.warp.wizards.WarpExportParticlesAdaptor """
        moduleName, className = spec['adaptor'].rsplit('.', 1)
        adaptorClass = getattr(importlib.import_module(moduleName), className)
        return adaptorClass(project)

    @classmethod
    def tag_form(cls, formDef, jobForm):
        """ Mark the params of formDef that open a wizard with
        'wizard': <wizard id> and copy the wizards (without the adaptor)
        to formDef['wizards']. """
        wizards = cls.get_form_wizards(jobForm)
        if not wizards:
            return

        paramWizards = {}
        for spec in wizards:
            for name in spec.get('params', []):
                paramWizards[name] = spec['id']

        def _tag(params):
            for p in params:
                if p.get('name') in paramWizards:
                    p['wizard'] = paramWizards[p['name']]
                _tag(p.get('params', []))

        for section in formDef['sections']:
            _tag(section['params'])

        formDef['wizards'] = [{'id': s['id'],
                               'label': s.get('label', s['id']),
                               'params': s.get('params', [])}
                              for s in wizards]

    @classmethod
    def _prepare(cls, project, jobType, wizardId, params):
        spec = cls.get_spec(jobType, wizardId)
        adaptor = cls.get_adaptor(spec, project)
        wizard = adaptor.wizard(project)
        prepared = wizard.prepare(adaptor.toWizard(params or {}))
        return spec, adaptor, wizard, prepared

    @classmethod
    def open(cls, project, jobType, wizardId, params):
        """ Called when the user opens the wizard, with the current values
        of the form (not saved yet). """
        spec, adaptor, wizard, prepared = cls._prepare(
            project, jobType, wizardId, params)
        return {
            'id': wizardId,
            'label': spec.get('label', wizard.label),
            'ui': wizard.ui,
            'inputs': prepared.get('inputs', {}),
            'data': prepared.get('data', {}),
            'warnings': prepared.get('warnings', []) + adaptor.warnings
        }

    @classmethod
    def apply(cls, project, jobType, wizardId, params, outputs):
        """ Called when the user presses OK. Return the form params to
        update; nothing is saved. """
        spec, adaptor, wizard, prepared = cls._prepare(
            project, jobType, wizardId, params)
        adaptor.warnings = []
        result = wizard.finalize(outputs or {}, prepared)
        newParams = adaptor.fromWizard(result['outputs'], params or {})
        return {
            'params': newParams,
            'warnings': result.get('warnings', []) + adaptor.warnings
        }


# ---------------------- Tomography helpers --------------------------------

def tomograms_from_optimisation_set(project, optimisationSet):
    """ Return the tomograms STAR file referenced by an optimisation set,
    or None if it is not an optimisation set or has no tomograms. """
    if not optimisationSet:
        return None
    path = optimisationSet if os.path.isabs(optimisationSet) else project.join(optimisationSet)
    if not RelionStar.isTomoOptimisationSet(path):
        return None
    row = RelionStar.readTomoOptimisationSet(path)[0]
    return getattr(row, 'rlnTomoTomogramsFile', None) or None


def read_tiltseries_pixel_size(path):
    """ Read the tilt-series pixel size from the global table of a
    tomograms STAR file. Return (pixel_size, count, warnings). """
    try:
        table = StarFile.getTableFromFile('global', path)
    except Exception as e:
        raise WizardError(f"Could not read the tomograms from {path}: {e}")

    if not table or not len(table):
        raise WizardError(f"There are no tomograms in {path}.")

    if 'rlnTomoTiltSeriesPixelSize' not in table.getColumnNames():
        raise WizardError(f"{path} has no column rlnTomoTiltSeriesPixelSize.")

    sizes = sorted({round(float(row.rlnTomoTiltSeriesPixelSize), 4)
                    for row in table})
    warnings = []
    if len(sizes) > 1:
        warnings.append(f"The tilt-series have different pixel sizes "
                        f"({', '.join(map(str, sizes))}), using {sizes[0]}.")
    return sizes[0], len(table), warnings


class ExportTomoParticlesWizard(Wizard):
    """ Box and pixel size of the tomography particles exported (extracted)
    from the tilt-series.

    The input pixel size is the tilt-series pixel size, read from the
    tomograms STAR file. The binning factor relates the input and output
    pixel sizes; the input and output box sizes are independent.

    Inputs (from the adaptor):
        tomograms: tomograms STAR file (required)
        in_box: input box size (px), or in_box_angst (A)
        out_angpix: output pixel size (A/px)
        out_box: output box size (px)
        diameter: particle diameter (A), optional; by default half of the
            input box. It is the reference circle of the UI.

    Outputs (from the UI):
        in_box, out_angpix, out_box, diameter (in_angpix comes from prepare())
    """
    ui = 'ExportTomoParticlesWizard'
    label = 'Particles box and pixel size'

    def prepare(self, inputs):
        tomograms = inputs.get('tomograms')
        if not tomograms:
            raise WizardError("Select the input tomograms (or an optimisation "
                              "set with tomograms) before opening the wizard.")
        path = self.join(tomograms)
        if not os.path.exists(path):
            raise WizardError(f"Input tomograms file not found: {tomograms}")

        inAngpix, count, warnings = read_tiltseries_pixel_size(path)

        outAngpix = to_float(inputs.get('out_angpix')) or inAngpix
        outBox = to_int(inputs.get('out_box'))
        inBox = to_int(inputs.get('in_box'))
        if not inBox and (inBoxAngst := to_float(inputs.get('in_box_angst'))):
            inBox = int(round(inBoxAngst / inAngpix))
        # Without one of the box sizes, take the same size in A
        if not inBox and outBox:
            inBox = int(round(outBox * outAngpix / inAngpix))
        if not outBox and inBox:
            outBox = int(round(inBox * inAngpix / outAngpix))

        diameter = to_float(inputs.get('diameter'))
        if not diameter or diameter <= 0:
            diameter = round(inBox * inAngpix / 2, 2) if inBox else None

        return {
            'inputs': {
                'in_box': inBox or 0,
                'out_angpix': outAngpix,
                'out_box': outBox or 0,
                'binning': round(outAngpix / inAngpix, 4),
                'diameter': diameter
            },
            'data': {
                'tomograms': tomograms,
                'tomograms_count': count,
                'in_angpix': inAngpix
            },
            'warnings': warnings
        }

    def finalize(self, outputs, prepared):
        inAngpix = prepared['data']['in_angpix']
        outAngpix = to_float(outputs.get('out_angpix'))
        inBox = to_int(outputs.get('in_box'))
        outBox = to_int(outputs.get('out_box'))
        diameter = to_float(outputs.get('diameter'))

        errors = []
        if not outAngpix or outAngpix <= 0:
            errors.append("The output pixel size should be a positive number.")
        if not inBox or inBox <= 0:
            errors.append("The input box size should be a positive integer.")
        if not outBox or outBox <= 0:
            errors.append("The output box size should be a positive integer.")
        if diameter is not None and diameter <= 0:
            errors.append("The particle diameter should be a positive number.")
        if errors:
            raise WizardError(errors)

        warnings = []
        if outAngpix < inAngpix:
            warnings.append("The output pixel size is smaller than the input "
                            "one, the particles will be upsampled.")
        for label, box in [('input', inBox), ('output', outBox)]:
            if box % 2:
                warnings.append(f"The {label} box size ({box} px) is odd.")

        return {
            'outputs': {
                'in_angpix': inAngpix,
                'in_box': inBox,
                'in_box_angst': round(inBox * inAngpix, 2),
                'out_angpix': outAngpix,
                'out_box': outBox,
                'out_box_angst': round(outBox * outAngpix, 2),
                'binning': round(outAngpix / inAngpix, 4),
                'diameter': diameter
            },
            'warnings': warnings
        }


# ---------------------- Volume helpers -------------------------------------

def read_volume_header(path):
    """ Dimensions [X, Y, Z] and voxel size (A, or None if unknown) of an
    MRC volume, from its header. """
    import mrcfile
    try:
        with mrcfile.mmap(path, mode='r', permissive=True) as mrc:
            shape = mrc.data.shape
            angpix = float(mrc.voxel_size.x)
    except Exception as e:
        raise WizardError(f"Could not read the volume {path}: {e}")
    if len(shape) < 3:
        raise WizardError(f"{path} is not a volume (dimensions: {shape}).")
    zdim, ydim, xdim = shape[-3:]
    return [int(xdim), int(ydim), int(zdim)], (angpix if angpix > 0 else None)


class VolumeMaskWizard(Wizard):
    """ Check the mask of a volume: a map (a mask file) or a sphere (its
    diameter). With a mask map, it takes precedence over the sphere.

    The volume and the mask are shown in emflow's VolumeViewer (Slices and
    3D volume). The data items with 'kind': 'volume' are the volumes to show;
    the host adds the request of their density ('density') to them.

    Inputs (from the adaptor):
        volume: the volume (required)
        mask: mask map, optional; on the same box as the volume
        diameter: diameter of the spherical mask (A), optional; by default
            80% of the box

    Outputs (from the UI):
        diameter (A), only changed with the spherical mask
    """
    ui = 'VolumeMaskWizard'
    label = 'Mask'

    def _volume(self, path, label):
        full = self.join(path)
        if not os.path.exists(full):
            raise WizardError(f"The {label} was not found: {path}")
        dims, angpix = read_volume_header(full)
        return {'kind': 'volume', 'path': path, 'dimensions': dims,
                'voxelSize': angpix}

    def prepare(self, inputs):
        if not inputs.get('volume'):
            raise WizardError("Select the input map before opening the wizard.")
        volume = self._volume(inputs['volume'], 'map')
        mask = self._volume(inputs['mask'], 'mask') if inputs.get('mask') else None
        if mask and mask['dimensions'] != volume['dimensions']:
            raise WizardError(
                f"The mask ({' x '.join(map(str, mask['dimensions']))}) and the "
                f"map ({' x '.join(map(str, volume['dimensions']))}) have "
                f"different dimensions.")

        angpix = volume['voxelSize']
        warnings = []
        if not angpix:
            warnings.append("The map has no pixel size in its header, "
                            "the diameter is shown only in pixels.")
        boxAngst = min(volume['dimensions']) * (angpix or 1)
        diameter = to_float(inputs.get('diameter'))
        if not diameter or diameter <= 0:
            diameter = round(0.8 * boxAngst, 2)
        if mask:
            warnings.append("The mask map is used, the spherical mask "
                            "(diameter) is not shown.")

        return {
            'inputs': {
                'mask_type': 'map' if mask else 'sphere',
                'diameter': diameter
            },
            'data': {
                'volume': volume,
                'mask': mask,
                'angpix': angpix,
                'box_angst': boxAngst
            },
            'warnings': warnings
        }

    def finalize(self, outputs, prepared):
        maskType = 'map' if prepared['data']['mask'] else 'sphere'
        diameter = to_float(outputs.get('diameter'))
        warnings = []
        if maskType == 'sphere':
            if not diameter or diameter <= 0:
                raise WizardError("The mask diameter should be a positive number.")
            boxAngst = prepared['data']['box_angst']
            if diameter > boxAngst:
                warnings.append(f"The mask diameter ({diameter:g} A) is larger "
                                f"than the box ({boxAngst:g} A).")
        else:
            diameter = prepared['inputs']['diameter']
        return {
            'outputs': {
                'mask_type': maskType,
                'mask': prepared['data']['mask'],
                'diameter': round(diameter, 2)
            },
            'warnings': warnings
        }
