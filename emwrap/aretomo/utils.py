# * Authors:     Daniel Marchan Torres (danielmarchan3@gmail.com)
# * Code based in GitHub Repository: https://github.com/Phaips/aretomo3torelion5/tree/main

import os
import math

from emtools.metadata import Imod, RelionStar


def compute_relion_alignments_from_imod(imod_folder, ts_name, pixel_size):
    """ Read tilt angles and XF transforms from AreTomo3 IMOD output and compute Relion alignments.
    Returns:
        list[dict]: list of Relion alignments
    """
    tlt_file = os.path.join(imod_folder, f'{ts_name}_st.tlt')
    tilt_angles = Imod.get_angles_from_tlt(tlt_file)
    xf_file = os.path.join(imod_folder, f'{ts_name}_st.xf')
    alignments = Imod.get_alignment_from_xf(xf_file)

    return RelionStar.alignments_from_imod(tilt_angles, alignments, pixel_size)


def stack_entry(stack_path, index, zero_pad=False):
    """Build Relion stack reference like 1@stack.mrc or 000001@stack.mrcs."""
    if not stack_path:
        return ''

    if zero_pad:
        return f'{index:06d}@{stack_path}'

    return f'{index}@{stack_path}'


def create_dummy_edf_file(output_dir, tomo_prefix):
    """Create a dummy ETOMO directive (.edf) file for RELION5.

    RELION5 tomograms.star expects rlnEtomoDirectiveFile to point to an
    ETOMO directive file. AreTomo3 does not naturally produce one, so this
    placeholder preserves compatibility.
    """
    os.makedirs(output_dir, exist_ok=True)

    edf_file_path = os.path.join(output_dir, f'{tomo_prefix}.edf')

    with open(edf_file_path, 'w') as f:
        f.write("# Dummy ETOMO directive file for RELION5\n")
        f.write(f"# Generated for tomogram: {tomo_prefix}\n")
        f.write("# This is a placeholder file\n")

    return edf_file_path