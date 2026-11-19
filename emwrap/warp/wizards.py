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

""" Wizard adaptors of the Warp job forms (see emwrap.base.wizards). """

import os
import re

from emwrap.base.wizards import (WizardAdaptor, WizardError,
                                 ExportTomoParticlesWizard, VolumeMaskWizard,
                                 tomograms_from_optimisation_set)


class WarpExportParticlesAdaptor(WizardAdaptor):
    """ emw-warp-export_particles <-> ExportTomoParticlesWizard

    Warp exports the particles with a box (px) at a given pixel size, so
    the input box size of the wizard is not used. The particle diameter of
    the form is shown (and can be changed) in the wizard.
    """
    wizard = ExportTomoParticlesWizard

    def toWizard(self, params):
        tomograms = (params.get('input_tomograms')
                     or tomograms_from_optimisation_set(
                         self.project, params.get('input_coordinates')))
        return {
            'tomograms': tomograms,
            'out_angpix': params.get('ts_export_particles.output_angpix'),
            'out_box': params.get('ts_export_particles.box'),
            'diameter': params.get('ts_export_particles.diameter')
        }

    def fromWizard(self, outputs, params):
        return {
            'ts_export_particles.output_angpix': outputs['out_angpix'],
            'ts_export_particles.box': outputs['out_box'],
            'ts_export_particles.diameter': outputs['diameter']
        }


# Volume of the first class of a Relion iteration (e.g. run_it025_class001.mrc,
# run_ct5_it025_class001.mrc)
RELION_ITER_CLASS001 = re.compile(r'^(.*)_it(\d+)_class001\.mrc$')


def relion_last_volume(project, particles):
    """ Return the project path of the last Relion volume of the particles
    STAR file of a Relion refinement (<prefix>_data.star): <prefix>_class001.mrc
    (e.g. run_class001.mrc for run_data.star, or run_it025_class001.mrc for
    run_it025_data.star) or, if not written yet, the class001 volume of the
    last iteration with the same prefix. Raise WizardError if none is found. """
    if not particles.endswith('_data.star'):
        raise WizardError(f"The particles should be a Relion _data.star file: "
                          f"{particles}")
    prefix = particles[:-len('_data.star')]
    volume = f'{prefix}_class001.mrc'
    join = lambda p: p if os.path.isabs(p) else project.join(p)
    if os.path.exists(join(volume)):
        return volume

    folder, base = os.path.split(prefix)
    iterations = []
    if os.path.isdir(join(folder or '.')):
        for fn in os.listdir(join(folder or '.')):
            if (m := RELION_ITER_CLASS001.match(fn)) and m.group(1) == base:
                iterations.append((int(m.group(2)), fn))
    if not iterations:
        raise WizardError(f"No Relion volume was found for the particles "
                          f"{particles} (expected {volume}).")
    return os.path.join(folder, max(iterations)[1])


class WarpCreateSpeciesMaskAdaptor(WizardAdaptor):
    """ emw-warp-mtools_create <-> VolumeMaskWizard

    Shows the species mask (create_species.mask) over the last Relion volume
    of the input particles (create_species.particles_relion, a Relion
    _data.star). Both the mask and the particles are required; OK changes
    nothing (the mask is a map).
    """
    wizard = VolumeMaskWizard

    def toWizard(self, params):
        mask = str(params.get('create_species.mask') or '').strip()
        particles = str(params.get('create_species.particles_relion') or '').strip()
        errors = []
        if not mask:
            errors.append("Select the mask before opening the wizard.")
        elif not os.path.exists(self.join(mask)):
            errors.append(f"The mask was not found: {mask}")
        if not particles:
            errors.append("Select the input particles (Relion _data.star) "
                          "before opening the wizard.")
        elif not os.path.exists(self.join(particles)):
            errors.append(f"The input particles were not found: {particles}")
        if errors:
            raise WizardError(errors)

        return {
            'volume': relion_last_volume(self.project, particles),
            'mask': mask,
            'diameter': params.get('create_species.diameter')
        }

    def fromWizard(self, outputs, params):
        return {}
