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

""" Wizards of the PyTom job forms (see emwrap.base.wizards). """

import os

from emwrap.base.wizards import (Wizard, WizardAdaptor, WizardError,
                                 read_volume_header, to_bool, to_float)


class PyTomTemplateWizard(Wizard):
    """ Template and mask of pytom_create_template.py and
    pytom_create_mask.py, with a tentative template and mask computed with
    numpy (emwrap.base.volume_mask) on the output box downscaled to about
    100 px: the input map centred, resampled to the output pixel size,
    cropped or padded to the box, inverted and mirrored, and the spherical
    mask with its Gaussian edge. It is only a guide, the job runs PyTom.

    Inputs (from the adaptor):
        volume: the input map (required)
        in_angpix, out_angpix: input and output pixel sizes (A), by default
            the one of the map header
        box: output box (px), by default the map box at the output pixel size
        radius, sigma: mask radius and Gaussian edge (px of the output box)
        center, invert, mirror: template operations

    Outputs (from the UI): the same values but the volume.
    Preview data: template, mask (VolumeDensity), binning, angpix
    """
    ui = 'PyTomTemplateWizard'
    label = 'Template and mask'
    preview_on_open = True

    def prepare(self, inputs):
        from emwrap.base.volume_mask import load_binned_map

        if not inputs.get('volume'):
            raise WizardError("Select the input map before opening the wizard.")
        path = self.join(inputs['volume'])
        if not os.path.exists(path):
            raise WizardError(f"The input map was not found: {inputs['volume']}")
        dims, headerAngpix = read_volume_header(path)
        load_binned_map(path)  # Cached for the previews

        inAngpix = to_float(inputs.get('in_angpix')) or headerAngpix
        if not inAngpix:
            raise WizardError("The map has no pixel size in its header: "
                              "set the input voxel size first.")
        warnings = []
        if headerAngpix and abs(inAngpix - headerAngpix) > 0.01 * headerAngpix:
            warnings.append(f"The input voxel size ({inAngpix:g} Å) is not the "
                            f"one of the map header ({headerAngpix:g} Å).")
        outAngpix = to_float(inputs.get('out_angpix')) or inAngpix
        box = to_float(inputs.get('box')) or round(max(dims) * inAngpix / outAngpix)
        radius = to_float(inputs.get('radius'))
        sigma = to_float(inputs.get('sigma'))
        return {
            'inputs': {
                'in_angpix': inAngpix,
                'out_angpix': outAngpix,
                'box': int(box),
                'radius': radius if radius else round(0.4 * box, 1),
                'sigma': sigma if sigma is not None else 1.0,
                'center': to_bool(inputs.get('center', False)),
                'invert': to_bool(inputs.get('invert', False)),
                'mirror': to_bool(inputs.get('mirror', False))
            },
            'data': {
                'input': {'path': inputs['volume'], 'dimensions': dims,
                          'voxelSize': headerAngpix}
            },
            'warnings': warnings
        }

    def _values(self, outputs):
        """ The validated values of the outputs, and their warnings. """
        v = {k: to_float(outputs.get(k))
             for k in ['in_angpix', 'out_angpix', 'box', 'radius', 'sigma']}
        errors = [f"The {label} should be a positive number."
                  for k, label in [('in_angpix', 'input voxel size'),
                                   ('out_angpix', 'output voxel size'),
                                   ('box', 'box size'),
                                   ('radius', 'mask radius')]
                  if not v[k] or v[k] <= 0]
        if v['sigma'] is None or v['sigma'] < 0:
            errors.append("The mask sigma should not be negative.")
        if errors:
            raise WizardError(errors)
        v['box'] = int(round(v['box']))
        for k in ['center', 'invert', 'mirror']:
            v[k] = to_bool(outputs.get(k, False))
        warnings = []
        if v['out_angpix'] < v['in_angpix']:
            warnings.append("The output voxel size is smaller than the input "
                            "one: the map is upsampled.")
        if v['radius'] > v['box'] / 2:
            warnings.append(f"The mask radius ({v['radius']:g} px) is larger "
                            f"than half of the box ({v['box'] / 2:g} px).")
        return v, warnings

    def preview(self, outputs, prepared):
        from emwrap.base.volume_mask import (load_binned_map, compute_template,
                                             spherical_mask, density_from_array)

        v, warnings = self._values(outputs)
        binned = load_binned_map(self.join(prepared['data']['input']['path']))
        template, f = compute_template(binned, v['in_angpix'], v['out_angpix'],
                                       v['box'], v['center'], v['invert'],
                                       v['mirror'])
        mask = spherical_mask(v['box'], v['radius'], v['sigma'], binning=f)
        valueRange = (float(template.min()), float(template.max()))
        return {
            'data': {
                'template': density_from_array(template, f, valueRange),
                'mask': density_from_array(mask, f),
                'binning': f,
                'box': v['box'],
                'angpix': v['out_angpix']
            },
            'warnings': warnings
        }

    def finalize(self, outputs, prepared):
        v, warnings = self._values(outputs)
        return {'outputs': v, 'warnings': warnings}


class PyTomCreateTemplateAdaptor(WizardAdaptor):
    """ emw-pytom-create_template <-> PyTomTemplateWizard """
    wizard = PyTomTemplateWizard
    T = 'pytom_create_template.'
    M = 'pytom_create_mask.'

    def toWizard(self, params):
        t = lambda name: params.get(self.T + name)
        return {
            'volume': t('input-map'),
            'in_angpix': t('input-voxel-size-angstrom'),
            'out_angpix': t('output-voxel-size-angstrom'),
            'box': t('box-size'),
            'center': t('center'),
            'invert': t('invert'),
            'mirror': t('mirror'),
            'radius': params.get(self.M + 'radius'),
            'sigma': params.get(self.M + 'sigma')
        }

    def fromWizard(self, outputs, params):
        return {
            self.T + 'input-voxel-size-angstrom': outputs['in_angpix'],
            self.T + 'output-voxel-size-angstrom': outputs['out_angpix'],
            self.T + 'box-size': outputs['box'],
            self.T + 'center': outputs['center'],
            self.T + 'invert': outputs['invert'],
            self.T + 'mirror': outputs['mirror'],
            self.M + 'radius': outputs['radius'],
            self.M + 'sigma': outputs['sigma']
        }
