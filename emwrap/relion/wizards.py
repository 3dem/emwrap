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

""" Wizard adaptors of the Relion job forms (see emwrap.base.wizards). """

from emtools.metadata import StarFile

from emwrap.base.wizards import (WizardAdaptor, WizardError,
                                 ExportTomoParticlesWizard, VolumeMaskWizard,
                                 MaskCreateWizard,
                                 tomograms_from_optimisation_set,
                                 to_bool, to_float)


class RelionPseudoSubtomoAdaptor(WizardAdaptor):
    """ relion.pseudosubtomo <-> ExportTomoParticlesWizard

    Relion takes the box size in A (box_size_angst, at the tilt-series
    pixel size) and the binned box size (binned_size_pix); the output pixel
    size is box_size_angst / binned_size_pix. The form has no particle
    diameter: the wizard uses half of the box as reference (not saved).
    """
    wizard = ExportTomoParticlesWizard

    def toWizard(self, params):
        if to_bool(params.get('use_direct_entries', False)):
            tomograms = params.get('in_tomograms')
        else:
            tomograms = tomograms_from_optimisation_set(
                self.project, params.get('in_optimisation'))

        boxAngst = to_float(params.get('box_size_angst'))
        binnedBox = to_float(params.get('binned_size_pix'))
        outAngpix = boxAngst / binnedBox if boxAngst and binnedBox else None
        return {
            'tomograms': tomograms,
            'in_box_angst': boxAngst,
            'out_box': binnedBox,
            'out_angpix': outAngpix
        }

    def fromWizard(self, outputs, params):
        boxAngst = outputs['in_box_angst']
        outBox = outputs['out_box']
        relionAngpix = boxAngst / outBox
        if abs(relionAngpix - outputs['out_angpix']) > 0.01 * outputs['out_angpix']:
            self.warnings.append(
                f"Relion derives the output pixel size from the box sizes: "
                f"{boxAngst} A / {outBox} px = {relionAngpix:.3f} A/px "
                f"(selected in the wizard: {outputs['out_angpix']:.3f} A/px).")
        return {
            'box_size_angst': boxAngst,
            'binned_size_pix': outBox
        }


class RelionReferenceMaskAdaptor(WizardAdaptor):
    """ relion.class3d.tomo, relion.refine3d.tomo <-> VolumeMaskWizard

    The volume is the reference map (fn_ref) and the mask is the reference
    mask (fn_mask). Without a mask, the wizard shows the spherical mask of
    the mask diameter (particle_diameter), which OK sets back.
    """
    wizard = VolumeMaskWizard

    def toWizard(self, params):
        return {
            'volume': self._referenceMap(params.get('fn_ref')),
            'mask': params.get('fn_mask'),
            'diameter': params.get('particle_diameter')
        }

    def fromWizard(self, outputs, params):
        if outputs['mask_type'] == 'map':
            return {}
        return {'particle_diameter': outputs['diameter']}

    def _referenceMap(self, fnRef):
        """ The reference map, or the first one of a list of references
        (Volumes: a STAR file with rlnReferenceImage, one per class). """
        if not fnRef or not str(fnRef).endswith('.star'):
            return fnRef
        try:
            refs = StarFile.getTableFromFile('references', self.join(fnRef))
        except Exception as e:
            raise WizardError(f"Could not read the references {fnRef}: {e}")
        if not len(refs):
            raise WizardError(f"There are no references in {fnRef}.")
        if len(refs) > 1:
            self.warnings.append(f"The mask is shown over the first of the "
                                 f"{len(refs)} references.")
        return refs[0].rlnReferenceImage


class RelionMaskCreateAdaptor(WizardAdaptor):
    """ emw-relion-mask_create (from volume) <-> MaskCreateWizard

    Only for the masks created from a volume: the de novo masks have no
    map to threshold. A lowpass of -1 (or empty) is none.
    """
    wizard = MaskCreateWizard
    PREFIX = 'relion_mask_create.volume.'

    def toWizard(self, params):
        if to_bool(params.get('relion_mask_create.denovo', False)):
            raise WizardError("The wizard creates masks from a volume: "
                              "turn off 'De novo creation'.")
        p = lambda name: params.get(self.PREFIX + name)
        return {
            'volume': p('i'),
            'threshold': p('ini_threshold'),
            'lowpass': p('lowpass'),
            'extend': p('extend_inimask'),
            'soft_edge': p('width_soft_edge'),
            'invert': p('invert'),
            'angpix': p('angpix')
        }

    def fromWizard(self, outputs, params):
        return {
            self.PREFIX + 'ini_threshold': round(outputs['threshold'], 6),
            self.PREFIX + 'lowpass': outputs['lowpass'] or -1,
            self.PREFIX + 'extend_inimask': outputs['extend'],
            self.PREFIX + 'width_soft_edge': outputs['soft_edge'],
            self.PREFIX + 'invert': outputs['invert']
        }
