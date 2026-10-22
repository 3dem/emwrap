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
import argparse
import glob
import numpy as np
import time
import json

from emtools.utils import Color, Process, System, Path, FolderManager
from emtools.jobs import BatchManager, Args
from emtools.metadata import Table, StarFile, WarpXml


def load_tomograms_table(tomo_session):
    session_path = tomo_session['path']
    s = FolderManager(session_path)

    if not s.exists():
        raise Exception("Session path does not exist")
    else:
        for k in ['tomograms', 'reconstruction', 'picking']:
            if k in tomo_session and not s.exists(tomo_session[k]):
                raise Exception(f"*{k}* folder does not exist.")

    picking = tomo_session.get('picking', '')
    t = FolderManager(s.join(tomo_session['tomograms']))
    c = FolderManager(s.join(picking, 'Coordinates')) if picking else None
    r = FolderManager(s.join(tomo_session['reconstruction']))
    ts = FolderManager(r.path.replace('reconstruction', 'tiltstack'))

    def _glob_file(fm, pattern):
        if files := fm.glob(pattern):
            return files[0]
        else:
            return ''

    coordsDict = {}
    if c:
        if coords := c.glob('*_default_particles.star'):
            # Get the splitting token (e.g 9.52Apx)
            token = coords[0].split('_')[-3]
            coordsDict = {os.path.basename(c.split(token)[0]): c for c in coords}

    tomoDict = {}
    if tomos := r.glob('*.mrc'):
        # Get the splitting token (e.g 9.52Apx)
        suffix = tomos[0].split('_')[-1]
        tomoDict = {os.path.basename(t).replace(suffix, ''): t for t in tomos}

    thickDict = {}
    if tomoStar := tomo_session.get('thickness', None):
        with StarFile(s.join(tomoStar)) as sf:
            for row in sf.iterTable('tomograms'):
                if 'Apx' in row.rlnTomoName:
                    suffix = "_" + row.rlnTomoName.split('_')[-1]
                else:
                    suffix = ""
                thickDict[row.rlnTomoName.replace(suffix, '')] = row.slabThickness

    table = Table([
        'rlnTomoName',
        'rlnTomoMetadata',
        'rlnCoordinatesMetadata',
        'rlnParticleNumber',
        'rlnTomoReconstructedTomogram',
        'rlnAlignedTiltSeries',
        'wrpTomoMetadataXml',
        'rlnDefocus',
        'rlnThickness'
    ])

    for tstar in t.glob("*.tomostar"):
        tsName = Path.removeBaseExt(tstar)

        # Load coordinates file
        # coordMd = _glob_file(c, tsName + '*default_particles.star')
        tsKey = f'{tsName}_'
        coordMd = coordsDict.get(tsKey, '')

        if coordMd:
            with StarFile(coordMd) as sf:
                coordN = sf.getTableSize('particles')
        else:
            coordN = ''

        # Load xml, tomogram, and aligned TS files
        # tomoFn = _glob_file(r, tsName + '*.mrc')
        tomoFn = tomoDict.get(tsKey, '')
        alignedTs = _glob_file(ts, f"{tsName}/{tsName}_aligned.mrc")
        tomoXml = _glob_file(r, f"../{tsName}.xml")

        # Load defocus
        if tomoXml:
            ctf = WarpXml(tomoXml).getDict('TiltSeries', 'CTF', 'Param')
            defocus = round(float(ctf['Defocus']), 2)
        else:
            defocus = ''

        # Relative path if non-empty value
        def _rel(p):
            return s.relpath(p) if p else p

        table.addRow(table.Row(
            rlnTomoName=tsName,
            rlnTomoMetadata=_rel(tstar),
            rlnCoordinatesMetadata=_rel(coordMd),
            rlnParticleNumber=coordN,
            rlnTomoReconstructedTomogram=_rel(tomoFn),
            rlnAlignedTiltSeries=_rel(alignedTs),
            wrpTomoMetadataXml=_rel(tomoXml),
            rlnDefocus=defocus,
            rlnThickness=thickDict.get(tsName, 0)
        ))

    return table


def _zscore_detrended(k, v, degree=3):
    """ v minus a low order polynomial in k, scaled to unit spread: removes
    the smooth background under the Thon rings. """
    v = v - np.polyval(np.polyfit(k, v, degree), k)
    return v / (v.std() or 1.0)


def warpCtfView(ps, ctf, opts):
    """ Diagnostic view of a Warp CTF fit: an image and a radial profile.

    ps is Warp's half-plane power spectrum as stored (kx across, ky up to
    the bottom row), ctf and opts the Movie CTF and OptionsCTF params of the
    movie xml.  Only the fitted frequency range is shown.

    Image (uint8): the measured half plane on top and the CTF^2 simulated
    from the fit mirrored below, so rings that run straight across the
    divider mean the fit matches.  The measured half is Warp's values as
    stored, with only a linear grey level window: no filtering, averaging
    or normalisation, which on a low dose tilt would turn noise into ring
    like structure.  Faint rings are what the data has.

    Profile (dict): the rotational average of the measured spectrum and of
    the simulated CTF^2, background removed, against spatial frequency, and
    their correlation.  At this dose this is the more reliable check.
    """
    ps = ps.astype(np.float64)
    h, w = ps.shape
    oy, ox = h - 1, w // 2
    # The spectrum is of the image binned 2^BinTimes times; PixelSize is
    # before binning (half the data's with EER upsampling)
    window = int(opts['Window'])
    pix = float(opts['PixelSize']) * 2 ** float(opts.get('BinTimes', 0) or 0)
    # Warp's range is a fraction of Nyquist
    rmin = float(opts['RangeMin']) * window / 2
    rmax = float(opts['RangeMax']) * window / 2
    R = int(min(np.ceil(rmax * 1.1), oy, ox - 1))

    top = ps[oy - R:oy + 1, ox - R:ox + R + 1]          # ky = R..0 as stored
    yy, xx = np.mgrid[R:-1:-1, -R:R + 1]
    rr = np.hypot(xx, yy)
    band = (rr >= rmin) & (rr <= rmax)
    # Detector artifacts put a bright line along kx = 0.  It is shown as it
    # is, but left out of the grey level window and of the profile.
    offLine = np.abs(xx) > 1

    # CTF^2 from the fit: Warp's defocus is D + dD cos(2 (theta - angle))
    volt = float(ctf['Voltage']) * 1e3
    lam = 12.2643247 / np.sqrt(volt * (1 + volt * 0.978466e-6))    # Å
    cs = float(ctf['Cs']) * 1e7                                     # mm -> Å
    D, dD = float(ctf['Defocus']) * 1e4, float(ctf['DefocusDelta']) * 1e4
    ang = np.radians(float(ctf['DefocusAngle']))

    def ctf2(rad, theta):
        k = rad / (window * pix)
        dz = D + dD * np.cos(2 * (theta - ang))
        chi = np.pi * lam * dz * k ** 2 - 0.5 * np.pi * cs * lam ** 3 * k ** 4
        return np.sin(chi + np.arcsin(float(ctf['Amplitude']))) ** 2

    sim = ctf2(rr, np.arctan2(yy, xx))

    def stretch(a, mask):
        lo, hi = np.percentile(a[mask], [2, 98])
        v = np.clip((a - lo) / ((hi - lo) or 1), 0, 1)
        v[~band] = 0.5
        return v

    img = np.vstack([stretch(top, band & offLine),
                     (0.15 + 0.7 * stretch(sim, band))[::-1][1:]])
    out = (img * 255).astype(np.uint8)
    out[R, :] = 255   # divider: measured above, simulated below

    # Radial profiles in half pixel bins over the fitted range
    cols = np.arange(w)[None, :] - ox
    full_rr = np.hypot(cols, oy - np.arange(h)[:, None])
    valid = np.broadcast_to(np.abs(cols) > 1, ps.shape)
    bins = (full_rr[valid] * 2).astype(int)
    b = np.arange(int(np.ceil(rmin * 2)), int(rmax * 2))
    radius = b / 2.0 + 0.25
    counts = np.bincount(bins, minlength=b[-1] + 1)
    sums = np.bincount(bins, weights=ps[valid], minlength=b[-1] + 1)
    measured = sums[b] / np.maximum(counts[b], 1)
    thetas = np.linspace(0, np.pi, 90)
    simulated = ctf2(radius[:, None], thetas[None, :]).mean(axis=1)
    k = radius / (window * pix)
    measured, simulated = _zscore_detrended(k, measured), _zscore_detrended(k, simulated)
    profile = {
        'k': np.round(k, 5).tolist(),                 # 1/Å
        'measured': np.round(measured, 3).tolist(),
        'simulated': np.round(simulated, 3).tolist(),
        'correlation': round(float(np.corrcoef(measured, simulated)[0, 1]), 3),
        'pixelSize': pix,     # of the spectrum, Å
    }
    return out, profile
