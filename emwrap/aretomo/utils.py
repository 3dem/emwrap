# * Authors:     Daniel Marchan Torres (danielmarchan3@gmail.com)
# * Code based in GitHub Repository: https://github.com/Phaips/aretomo3torelion5/tree/main

import os
import math


def read_imod_tlt_file(imod_folder, ts_name):
    """Read AreTomo3/IMOD corrected tilt angles.
    Expected file:
        TS_NAME_Imod/TS_NAME_st.tlt
    Returns:
        dict[int, float]: {micrograph_index_1_based: corrected_tilt_angle}
    """
    if not imod_folder:
        return {}

    tlt_file = os.path.join(imod_folder, f'{ts_name}_st.tlt')
    if not os.path.exists(tlt_file):
        return {}

    tilt_by_index = {}

    with open(tlt_file) as f:
        for index, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            tilt_by_index[index] = float(line)

    return tilt_by_index


def read_imod_xf_file(imod_folder, ts_name):
    """Read IMOD XF transformation matrices.
    Expected file:
        TS_NAME_Imod/TS_NAME_st.xf
    Each row contains:
        A11 A12 A21 A22 DX DY
    Returns:
        dict[int, list[float]]
    """
    if not imod_folder:
        return {}

    xf_file = os.path.join(imod_folder, f'{ts_name}_st.xf')
    if not os.path.exists(xf_file):
        return {}

    xf_by_index = {}

    with open(xf_file) as f:
        for index, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue

            values = [float(x) for x in line.split()]
            if len(values) >= 6:
                xf_by_index[index] = values[:6]

    return xf_by_index


def compute_relion_alignment_from_xf(xf_row, pixel_size):
    """Convert one IMOD XF row into Relion alignment labels.
    IMOD XF row:
        A11 A12 A21 A22 DX DY
    The translation should be taken from the inverse transform, then
    converted from pixels to Angstroms.
    """
    a11, a12, a21, a22, dx, dy = xf_row

    det = a11 * a22 - a12 * a21
    if abs(det) < 1e-12:
        return {
            'rlnTomoZRot': '',
            'rlnTomoXShiftAngst': '',
            'rlnTomoYShiftAngst': '',
        }

    z_rot = math.degrees(math.atan2(a12, a11))

    # Inverse affine translation:
    # inv(M) * -t
    inv_dx = -((a22 * dx - a12 * dy) / det)
    inv_dy = -((-a21 * dx + a11 * dy) / det)

    return {
        'rlnTomoZRot': z_rot,
        'rlnTomoXShiftAngst': inv_dx * pixel_size,
        'rlnTomoYShiftAngst': inv_dy * pixel_size,
    }


def read_imod_alignment(imod_folder, ts_name, pixel_size):
    """Read corrected tilt angles and XF transforms from AreTomo3 IMOD output.
    Returns:
        {
            micrograph_index: {
                'tilt': float,
                'rlnTomoXTilt': 0.0,
                'rlnTomoYTilt': float,
                'rlnTomoZRot': float,
                'rlnTomoXShiftAngst': float,
                'rlnTomoYShiftAngst': float,
                'rlnCtfScalefactor': float,
            }
        }
    """
    tilt_by_index = read_imod_tlt_file(imod_folder, ts_name)
    xf_by_index = read_imod_xf_file(imod_folder, ts_name)

    alignment_by_index = {}

    all_indices = sorted(set(tilt_by_index) | set(xf_by_index))

    for index in all_indices:
        tilt = tilt_by_index.get(index, '')
        xf_row = xf_by_index.get(index)

        xf_values = {}
        if xf_row is not None:
            xf_values = compute_relion_alignment_from_xf(xf_row, pixel_size)

        if tilt != '':
            ctf_scale = math.cos(math.radians(tilt))
        else:
            ctf_scale = ''

        alignment_by_index[index] = {
            'tilt': tilt,
            'rlnTomoXTilt': 0.0 if tilt != '' else '',
            'rlnTomoYTilt': tilt,
            'rlnTomoZRot': xf_values.get('rlnTomoZRot', ''),
            'rlnTomoXShiftAngst': xf_values.get('rlnTomoXShiftAngst', ''),
            'rlnTomoYShiftAngst': xf_values.get('rlnTomoYShiftAngst', ''),
            'rlnCtfScalefactor': ctf_scale,
        }

    return alignment_by_index


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