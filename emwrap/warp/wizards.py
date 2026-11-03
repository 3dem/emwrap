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

from emwrap.base.wizards import (WizardAdaptor, ExportTomoParticlesWizard,
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
