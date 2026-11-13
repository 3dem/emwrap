# **************************************************************************
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
""" Image content measures of the tilts of a tilt series, from their jpegs
(TiltThumbnailWriter.tilt_image() at JPEG_SIZE: the recipe and size the
measures were calibrated on), and the curation rule.

What curators reject a tilt for is mostly in the image: blank, partly
blacked out (a lamella edge or a grid bar), or showing another area.  For
each tilt this measures:

    texture     median standard deviation of 16 px blocks (grey levels)
    darkArea    fraction of 16 px blocks with a mean below 40
    matchPeak   phase correlation peak with the last good tilt before it
                on its side of the first acquired tilt
    matchNcc    normalised cross correlation with that tilt
    fieldShift  shift from the first acquired tilt, in fractions of the
                field of view, accumulated along the same path

They are raw values: a reader makes texture and darkArea relative to the
series (median), so a series dark or low in contrast throughout is not
penalised.  The measures and the walk are the ones calibrated with the
tomo-data-curation skill on a hand curation (ChlamyBrnA, EMPIAR-11830).

One json per tilt series, <tomoName>_imageqc.json beside the jpegs, written
by the tilt analysis job (emw-tilt-analyze) that made them (write_table_json).

The curation rule (curate_series) turns the measures and the motion and CTF
values of the star file into the tilts to leave out: the OTF dashboard
proposes them, the emw-tilt-curation job applies them.

The thresholds and the rule's limits are kept in a tilt_qc_config.json per
job: see TiltQualityCuration.Config.
"""

import os
import json
import math
import time
import copy

import numpy as np
from PIL import Image as PILImage

from emwrap.base.tilt_thumbnails import TiltThumbnailWriter


class TiltQualityCuration:
    """ The image measures of the tilts of a tilt series and the curation
    rule: the only implementation.  The OTF dashboard flags and proposes
    with it, the emw-tilt-curation job applies it and the
    tomo-data-curation skill uses it. """
    VERSION = 1
    SUFFIX = '_imageqc.json'
    MEASURES = ('texture', 'darkArea', 'matchPeak', 'matchNcc', 'fieldShift')

    # The tilt jpegs (TiltThumbnailWriter).  The measures were calibrated on
    # this size and on TiltThumbnailWriter.tilt_image(): changing either
    # needs a recalibration
    JPEG_SIZE = 512
    # A closer look.  Larger is not better: each jpeg's contrast is set from
    # its own pixels, and beyond this size those of a low dose tilt are
    # mostly shot noise
    MEDIUM_JPEG_SIZE = 1024
    # The CTF fit view, measured over simulated
    PS_JPEG_SIZE = 384

    N = 128            # side of the images compared, px
    BLOCK = 16         # px, for texture and darkArea
    DARK_LEVEL = 40    # grey level of a black block in the jpeg
    # A neighbour this well matched becomes the reference for the next tilt
    GOOD_PEAK = 0.08
    _BILINEAR = getattr(PILImage, 'Resampling', PILImage).BILINEAR
    # The high-pass filter and window of the compared images, made once
    _KY, _KX = np.meshgrid(np.fft.fftfreq(N), np.fft.fftfreq(N), indexing='ij')
    # high-pass: smooth shading (a dark corner) must not drive the match
    _HIGHPASS = 1 - np.exp(-(_KX ** 2 + _KY ** 2) / (2 * 0.02 ** 2))
    _WINDOW = np.outer(np.hanning(N), np.hanning(N))

    # Limits a tilt is left out beyond; None leaves the check out.  Calibrated
    # on a hand curation of lamella tilt series (ChlamyBrnA, EMPIAR-11830).
    DEFAULT_LIMITS = {
        'texture': 0.5,          # image texture under this fraction of the series median: blank
        'darkArea': 0.10,        # fraction of the field black beyond the series median: blacked out
        'matchPeak': 0.04,       # phase correlation with the tilt before under this...
        'matchNcc': 0.15,        # ...and normalised correlation under this: a different field
        'fieldShift': 0.30,      # shift from the first tilt, fractions of the field of view
        # Motion was no reason to reject below this; 50-75 Å flagged 34 kept tilts
        'motion': 75.0,          # accumulated motion, Å
        'ctfResolution': 10.0,   # CTF fit resolution, Å
        'ctfCorrelation': 0.15,  # CTF profile correlation (<movie>_ctf.json written by emwrap)
        'figureOfMerit': 0.03,   # CTF figure of merit (Warp does not report one)
        'astigmatism': 4000.0,   # Å
        'defocus': 60000.0,      # Å; overfocus (negative defocus) too
        'beyond': True,          # also the tilts acquired after one that lost the field
    }
    # Reasons meaning the tilt series lost the field: the tilts acquired after
    # it on the same side of the first tilt are not trusted either ('beyond')
    FIELD_REASONS = ('blank', 'blacked', 'field', 'shifted')
    REASON_LABELS = {
        'motion': 'motion', 'ctf': 'CTF fit resolution', 'ctfcorr': 'CTF profile correlation',
        'fom': 'CTF figure of merit', 'astigmatism': 'astigmatism', 'defocus': 'defocus',
        'blank': 'blank image', 'blacked': 'blacked out', 'field': 'different field',
        'shifted': 'field shifted', 'beyond': 'after a tilt that lost the field',
    }
    # What a tilt dict may carry (missing or None: not measured, not judged):
    #   movie, angle      movie index (acquisition order) and tilt angle
    #   texture, darkArea, matchPeak, matchNcc, fieldShift    from series_measures
    #   motion, ctfResolution, ctfCorrelation, figureOfMerit, astigmatism, defocus
    #   dropped           processing dropped it: counts for the series medians,
    #                     is not judged, and the field is not followed through it

    class Config:
        """ The tilt QC configuration: the thresholds the OTF dashboard categorizes
        tilts and tilt series with, and the limits of the curation rule
        (curate_series), in one json file per job.

            <tilt analysis job>/tilt_qc_config.json   written by emw-tilt-analyze
                                                      when it starts (from the
                                                      defaults, or from a previous
                                                      file), then saved over from
                                                      the OTF dashboard
            <tilt curation job>/tilt_qc_config.json   the limits that job applied

        It also keeps which plots the dashboard's tilt series window shows
        ('display', not used by the jobs).

        The file:

            {"version": 1, "type": "emwrap.TiltQcConfig",
             "updated": "2026-11-12T10:00:00", "updatedBy": "...", "source": "...",
             "thresholds": {"motionTotal": {"target": 25.0, "reject": 75.0}, ...}}

        Units are the stored ones: Å (defocus and astigmatism too), and fractions
        (0.75, not 75%).  A limit set to null is not used.  A threshold left out of
        the file keeps its default, so a file written before a new threshold was
        added still reads.
        """
        VERSION = 1
        TYPE = 'emwrap.TiltQcConfig'  # in the files: kept as is
        FILENAME = 'tilt_qc_config.json'

        # Default thresholds, following the facility's RELION 5 CryoET QC
        # reference, revised against a hand curation of lamella data
        # (ChlamyBrnA, 38 tilt series): motion was not a reason to reject below
        # ~75 Å, Warp's CTF fit resolution saturated around 7 Å, and the CTF
        # profile correlation told failed fits apart.  Not meant to be universal.
        #
        # Tilts are judged first: a tilt past a 'reject' limit is unusable, one
        # past a 'target' is usable but not optimal.  The reject limits are the
        # curation rule's (TiltQualityCuration.DEFAULT_LIMITS, filled in below
        # through RULE_KEYS); the targets are the dashboard's own.  A tilt series
        # is then judged on how many usable tilts it keeps.
        DEFAULT_THRESHOLDS = {
            # Per tilt
            'motionTotal': {'target': 25.0},                       # Å
            # Not judged when early motion equals total motion on every tilt (a
            # dose per tilt under RELION's 4 e/Å² split): it would only repeat
            # motion
            'motionEarly': {'flag': 10.0},                         # Å
            # The CTF fit target loosens with tilt, since the path length through
            # the sample grows as 1/cos(tilt): lowTarget up to lowTilt,
            # highTarget from highTilt, linear in between
            'ctfMaxResolution': {'lowTilt': 20.0, 'lowTarget': 7.5,
                                 'highTilt': 45.0, 'highTarget': 9.0},  # degrees, Å
            # Correlation of the measured and simulated CTF radial profiles: below
            # 0.15 the fit failed (hand-rejected tilts had a median of 0.25, the
            # kept ones 0.55)
            'ctfCorrelation': {'target': 0.3},
            # Per tilt series
            'usableFraction': {'bad': 0.75},       # usable tilts / collected tilts: Poor below
            'droppedFraction': {'suspect': 0.15},  # excluded + rejected / collected: Needs review above
            'earlyMotionFraction': {'suspect': 0.30},  # tilts over motionEarly flag
            # Tilt series that would keep fewer than this fraction of their tilts
            # once the curation leaves the failing ones out: listed by the
            # dashboard for review, left out by the tilt curation job.  None: off
            'tiltsKept': {'min': None},
            # Per session: spread of the tilt axis between tilt series
            'tiltAxisSessionStd': 1.5,             # degrees
            # Reported but not used to flag a series: on a real session it came
            # out at 67-113 Å on every tilt series.  Set limits once calibrated.
            'shiftRoughness': {'suspect': None, 'bad': None},
        }

        # Where each limit of the curation rule (TiltQualityCuration.DEFAULT_LIMITS)
        # sits in the thresholds.  Image content: blank, blacked out, a different
        # field (both correlations low), shifted.
        RULE_KEYS = {
            'motion': ('motionTotal', 'reject'),               # Å
            'ctfResolution': ('ctfMaxResolution', 'reject'),   # Å
            'ctfCorrelation': ('ctfCorrelation', 'reject'),
            'figureOfMerit': ('ctfFigureOfMerit', 'reject'),
            'astigmatism': ('astigmatism', 'reject'),          # Å
            'defocus': ('defocus', 'reject'),                  # Å; overfocus (negative) too
            'texture': ('imageTexture', 'reject'),             # of the series median
            'darkArea': ('darkArea', 'reject'),                # of the field, beyond the series median
            'matchPeak': ('neighbourMatch', 'peak'),
            'matchNcc': ('neighbourMatch', 'ncc'),
            'fieldShift': ('fieldShift', 'reject'),            # of the field of view
            'beyond': ('fieldLoss', 'beyond'),                 # also the tilts after a lost field
        }

        # Not a threshold: which per tilt plots the OTF dashboard's tilt series
        # window shows ('display': {metric: true/false}).  By default the ones
        # the curation is decided on: motion, the CTF fit and its correlation,
        # and the image content (blank, blacked out, a different field, shifted)
        DISPLAY_DEFAULTS = {
            'motionTotal': True, 'motionEarly': False, 'defocus': False, 'astigmatism': False,
            'ctfResolution': True, 'ctfCorrelation': True, 'ctfFigureOfMerit': False,
            'imageTexture': True, 'darkArea': True, 'matchPeak': True, 'matchNcc': False,
            'fieldShift': True, 'tiltAxis': False,
        }
        DEFAULT_THRESHOLDS['display'] = dict(DISPLAY_DEFAULTS)

        # On/off values (and every 'display' one); every other is a number or None
        BOOLEAN_FIELDS = {('fieldLoss', 'beyond')}

        # ------------------------------------------------------------------
        @classmethod
        def defaults(cls):
            return copy.deepcopy(cls.DEFAULT_THRESHOLDS)

        @classmethod
        def validate(cls, thresholds, where='thresholds'):
            """ Check that thresholds only has known thresholds and values of the
            right type; raises ValueError naming the first wrong one. """
            if not isinstance(thresholds, dict):
                raise ValueError(f"{where}: expected an object of thresholds")
            for key, value in thresholds.items():
                if key not in cls.DEFAULT_THRESHOLDS:
                    raise ValueError(f"{where}: unknown threshold '{key}'")
                default = cls.DEFAULT_THRESHOLDS[key]
                if isinstance(default, dict):
                    if not isinstance(value, dict):
                        raise ValueError(f"{where}: '{key}' must be an object, got {value!r}")
                    for field, v in value.items():
                        if field not in default:
                            raise ValueError(f"{where}: unknown field '{field}' of '{key}'")
                        cls._check_value(v, (key, field), f"{where}: '{key}.{field}'")
                else:
                    cls._check_value(value, (key, None), f"{where}: '{key}'")

        @classmethod
        def _check_value(cls, v, path, name):
            if path in cls.BOOLEAN_FIELDS or path[0] == 'display':
                if not isinstance(v, bool):
                    raise ValueError(f"{name} must be true or false, got {v!r}")
            elif v is not None and (isinstance(v, bool) or not isinstance(v, (int, float))
                                    or not math.isfinite(v)):
                raise ValueError(f"{name} must be a number or null, got {v!r}")

        @classmethod
        def merge(cls, base, overrides):
            """ base with overrides on top: an override of one limit keeps the
            other limits of that threshold.  Returns a new dict. """
            out = copy.deepcopy(base)
            for k, v in (overrides or {}).items():
                if isinstance(out.get(k), dict) and isinstance(v, dict):
                    out[k].update(v)
                else:
                    out[k] = v
            return out

        @classmethod
        def rule_limits(cls, thresholds):
            """ The curation rule's limits (TiltQualityCuration) from thresholds;
            None turns a check off. """
            return {limit: (thresholds.get(key) or {}).get(field)
                    for limit, (key, field) in cls.RULE_KEYS.items()}

        @classmethod
        def from_rule_limits(cls, limits, base=None):
            """ Thresholds (over base, by default the defaults) with the curation
            rule's limits set from limits. """
            th = copy.deepcopy(base) if base is not None else cls.defaults()
            for limit, (key, field) in cls.RULE_KEYS.items():
                if limit in limits:
                    th.setdefault(key, {})[field] = limits[limit]
            return th

        @staticmethod
        def tilts_kept_min(thresholds):
            return (thresholds.get('tiltsKept') or {}).get('min')

        # ------------------------------------------------------------------
        @classmethod
        def path(cls, folder):
            return os.path.join(folder, cls.FILENAME)

        @classmethod
        def read_file(cls, path):
            """ The file as stored ({'thresholds', 'updated', ...}), with its
            thresholds validated (not merged).  Raises ValueError if it is not a
            tilt QC configuration, OSError if it cannot be read. """
            with open(path) as f:
                try:
                    data = json.load(f)
                except ValueError as e:
                    raise ValueError(f"{path} is not valid json: {e}")
            if not isinstance(data, dict) or not isinstance(data.get('thresholds'), dict):
                raise ValueError(f"{path} is not a tilt QC configuration file "
                                 "(no 'thresholds')")
            if data.get('version', cls.VERSION) > cls.VERSION:
                raise ValueError(f"{path} is of a newer version ({data['version']}) than this "
                                 f"one reads ({cls.VERSION})")
            cls.validate(data['thresholds'], where=path)
            return data

        @classmethod
        def read(cls, path):
            """ The thresholds of a file over the defaults. """
            return cls.merge(cls.DEFAULT_THRESHOLDS, cls.read_file(path)['thresholds'])

        @classmethod
        def write(cls, path, thresholds, user=None, source=None, loaded_from=None):
            """ Write thresholds (validated, over the defaults, so the file lists
            every threshold) to path.  source: who wrote it (a job, the
            dashboard); loaded_from: the file it was first loaded from, if any
            (kept by later saves, so a job continued knows it loaded it).  Written to a temporary file made with
            open(), so the folder's default ACL applies, then renamed: a reader
            never gets a partial file.  Returns what was written. """
            cls.validate(thresholds)
            data = {
                'version': cls.VERSION,
                'type': cls.TYPE,
                'updated': time.strftime('%Y-%m-%dT%H:%M:%S'),
                'updatedBy': user,
                'source': source,
                'loadedFrom': loaded_from,
                'units': 'Å for motion, resolution, defocus and astigmatism; fractions '
                         'for the image measures and tilt series fractions; null: not used',
                'thresholds': cls.merge(cls.DEFAULT_THRESHOLDS, thresholds),
            }
            os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
            tmp = f'{path}.{os.getpid()}.{time.time_ns()}.tmp'
            with open(tmp, 'x') as f:
                json.dump(data, f, indent=2)
            os.replace(tmp, path)
            return data

    # The reject limits of the thresholds are the rule's: filled in here,
    # since the body of Config cannot see DEFAULT_LIMITS
    for _limit, (_key, _field) in Config.RULE_KEYS.items():
        Config.DEFAULT_THRESHOLDS.setdefault(_key, {})[_field] = DEFAULT_LIMITS[_limit]
    del _limit, _key, _field

    # ------------------------------------------------------------------
    # Image measures
    # ------------------------------------------------------------------
    @staticmethod
    def load_jpeg(path):
        return np.asarray(PILImage.open(path).convert('L'), dtype=np.float32)

    @classmethod
    def block_measures(cls, a):
        """ (texture, darkArea) of a grey level image. """
        b = cls.BLOCK
        h, w = (a.shape[0] // b) * b, (a.shape[1] // b) * b
        blocks = a[:h, :w].reshape(h // b, b, w // b, b)
        return (float(np.median(blocks.std(axis=(1, 3)))),
                float((blocks.mean(axis=(1, 3)) < cls.DARK_LEVEL).mean()))

    @classmethod
    def _prep(cls, a):
        small = np.asarray(PILImage.fromarray(a).resize((cls.N, cls.N), cls._BILINEAR),
                           dtype=np.float32)
        hp = np.real(np.fft.ifft2(np.fft.fft2(small) * cls._HIGHPASS))
        hp = (hp - hp.mean()) / (hp.std() + 1e-6)
        return hp * cls._WINDOW

    @classmethod
    def _match(cls, a, b):
        """ Shift (fractions of the field), phase correlation peak and
        normalised cross correlation between two prepared images. """
        n = cls.N
        fa, fb = np.fft.fft2(a), np.fft.fft2(b)
        cross = fa * np.conj(fb)
        r = np.real(np.fft.ifft2(cross / (np.abs(cross) + 1e-6)))
        y, x = np.unravel_index(np.argmax(r), r.shape)
        peak = float(r[y, x])
        y, x = (y - n if y > n // 2 else y), (x - n if x > n // 2 else x)
        ncc = float(np.real(np.fft.ifft2(cross)).max() / (np.sqrt((a ** 2).sum() * (b ** 2).sum()) + 1e-6))
        return x / n, y / n, peak, ncc

    @classmethod
    def series_measures(cls, tilts):
        """ Measures of each tilt of one tilt series.

        tilts: dicts with 'movie' (index, acquisition order), 'angle' and
        'image' (grey level array, or None if there is no jpeg).  Returns one
        dict per tilt, in tilt angle order, with 'movie', 'angle' and the
        measures it has (the first acquired tilt has no match values).
        """
        tilts = sorted(tilts, key=lambda t: t['angle'])
        rows, imgs = [], {}
        for i, t in enumerate(tilts):
            r = {k: t[k] for k in ('movie', 'angle', 'name') if k in t}
            if t.get('image') is not None:
                r['texture'], r['darkArea'] = cls.block_measures(t['image'])
                imgs[i] = cls._prep(t['image'])
            rows.append(r)
        # Follow the field outward from the first acquired tilt, each tilt
        # compared with the last good one before it on its side (by position,
        # so a repeated movie index does not merge two tilts)
        if rows:
            i0 = min(range(len(tilts)), key=lambda i: tilts[i]['movie'])
            for side in (range(i0 + 1, len(tilts)), range(i0 - 1, -1, -1)):
                ref, pos = i0, np.zeros(2)
                for i in side:
                    if i not in imgs or ref not in imgs:
                        continue
                    dx, dy, peak, ncc = cls._match(imgs[ref], imgs[i])
                    rows[i].update(matchPeak=peak, matchNcc=ncc,
                                   fieldShift=float(np.hypot(*(pos + [dx, dy]))))
                    if peak > cls.GOOD_PEAK:
                        pos, ref = pos + [dx, dy], i
        return rows

    # ------------------------------------------------------------------
    # The curation rule
    # ------------------------------------------------------------------
    @staticmethod
    def _median(values):
        v = [x for x in values if x is not None]
        return float(np.median(v)) if v else None

    @classmethod
    def relative_measures(cls, tilts):
        """ (texture / series median, dark area - series median) of each tilt,
        in order, None where not measured.  Medians over every tilt given, so
        a tilt series dark or low in contrast throughout is not penalised. """
        texture = cls._median(t.get('texture') for t in tilts)
        dark = cls._median(t.get('darkArea') for t in tilts)
        return [(t['texture'] / texture if t.get('texture') is not None and texture else None,
                 t['darkArea'] - dark if t.get('darkArea') is not None and dark is not None else None)
                for t in tilts]

    @classmethod
    def tilt_flags(cls, tilts, limits=None):
        """ The reasons to leave out each tilt of one tilt series, in order
        (an empty list for a tilt kept or dropped).  Limits default to
        DEFAULT_LIMITS; 'beyond' is not applied here, see beyond_field. """
        lim = dict(cls.DEFAULT_LIMITS, **(limits or {}))

        def over(v, key):
            return v is not None and lim.get(key) is not None and v > lim[key]

        def under(v, key):
            return v is not None and lim.get(key) is not None and v < lim[key]

        flags = []
        for t, (texture, dark) in zip(tilts, cls.relative_measures(tilts)):
            why = []
            if not t.get('dropped'):
                if over(t.get('motion'), 'motion'):
                    why.append('motion')
                if over(t.get('ctfResolution'), 'ctfResolution'):
                    why.append('ctf')
                if under(t.get('ctfCorrelation'), 'ctfCorrelation'):
                    why.append('ctfcorr')
                if under(t.get('figureOfMerit'), 'figureOfMerit'):
                    why.append('fom')
                if t.get('astigmatism') is not None and over(abs(t['astigmatism']), 'astigmatism'):
                    why.append('astigmatism')
                d = t.get('defocus')
                if d is not None and lim.get('defocus') is not None and (d < 0 or d > lim['defocus']):
                    why.append('defocus')
                if under(texture, 'texture'):
                    why.append('blank')
                if over(dark, 'darkArea'):
                    why.append('blacked')
                if under(t.get('matchPeak'), 'matchPeak') and under(t.get('matchNcc'), 'matchNcc'):
                    why.append('field')
                if over(t.get('fieldShift'), 'fieldShift'):
                    why.append('shifted')
            flags.append(why)
        return flags

    @classmethod
    def beyond_field(cls, tilts, flags):
        """ Add 'beyond' to the flags (aligned with tilts) of the tilts
        acquired after one that lost the field (FIELD_REASONS), on the same
        side of the first acquired tilt.  Dropped tilts, and those without a
        movie index or angle, are left out of the walk. """
        walk = [(t, f) for t, f in zip(tilts, flags) if not t.get('dropped')
                and t.get('movie') is not None and t.get('angle') is not None]
        if not walk:
            return flags
        first = min(walk, key=lambda x: x[0]['movie'])[0]['angle']
        for sign in (1, -1):
            lost = False
            for t, f in sorted((x for x in walk if (x[0]['angle'] - first) * sign > 0),
                               key=lambda x: abs(x[0]['angle'] - first)):
                if set(f) & set(cls.FIELD_REASONS):
                    lost = True
                elif lost and not f:
                    f.append('beyond')
        return flags

    @classmethod
    def curate_series(cls, tilts, limits=None):
        """ {movie index: [reasons]} for the tilts of one tilt series to leave
        out: tilt_flags, and beyond_field unless limits['beyond'] is off. """
        limits = dict(cls.DEFAULT_LIMITS, **(limits or {}))
        flags = cls.tilt_flags(tilts, limits)
        if limits.get('beyond'):
            cls.beyond_field(tilts, flags)
        return {t['movie']: f for t, f in zip(tilts, flags) if f and t.get('movie') is not None}

    # ------------------------------------------------------------------
    # The json of a tilt series
    # ------------------------------------------------------------------
    @classmethod
    def json_path(cls, thumbnails_dir, tomo_name):
        return os.path.join(thumbnails_dir, tomo_name + cls.SUFFIX)

    @classmethod
    def read_series_json(cls, path, tilts=None):
        """ The measures (rows) of a series json, or None if it is missing,
        unreadable, of another version, or, given tilts, older than one of
        their jpegs or missing one of them. """
        try:
            with open(path) as f:
                data = json.load(f)
            rows = data['tilts'] if data.get('version') == cls.VERSION else None
            if rows is not None and tilts is not None:
                jpegs = [t['jpeg'] for t in tilts if t.get('jpeg') and os.path.exists(t['jpeg'])]
                names = {r.get('name') for r in rows}
                if (any(t['name'] not in names for t in tilts if t.get('jpeg'))
                        or (jpegs and os.path.getmtime(path) < max(map(os.path.getmtime, jpegs)))):
                    return None
            return rows
        except (OSError, ValueError, KeyError, TypeError, AttributeError):
            return None

    @classmethod
    def write_series_json(cls, path, tomo_name, tilts, force=False):
        """ Measure the tilts of a series and write the json, unless it is
        up to date: newer than every jpeg and listing the same tilts.

        tilts: dicts with 'movie', 'angle', 'name' (the micrograph base name,
        which is how readers match a tilt) and 'jpeg' (path, None for a tilt
        without an average: left out).  Returns (rows, written): the measures,
        None if no jpeg exists.  Written to a temporary file then renamed, so a
        reader never gets a partial file.
        """
        tilts = [t for t in tilts if t.get('jpeg')]
        if not any(os.path.exists(t['jpeg']) for t in tilts):
            return None, False
        if not force:
            rows = cls.read_series_json(path, tilts)
            if rows is not None and len(rows) == len(tilts):
                return rows, False
        rows = cls.series_measures([
            dict(movie=t['movie'], angle=t['angle'], name=t['name'],
                 image=cls.load_jpeg(t['jpeg']) if os.path.exists(t['jpeg']) else None)
            for t in tilts])
        data = {'version': cls.VERSION, 'tomoName': tomo_name,
                'source': 'tilt jpegs', 'jpegSize': cls.JPEG_SIZE,
                'blockSize': cls.BLOCK, 'darkLevel': cls.DARK_LEVEL, 'tilts': rows}
        TiltThumbnailWriter.save_json(data, path)
        return rows, True

    @classmethod
    def write_table_json(cls, thumbnails_dir, tomo_name, ts_table):
        """ <tomoName>_imageqc.json of a tilt series in thumbnails_dir, the
        folder its jpegs were written to, from its tilt-series STAR table,
        once the jpegs are all written: see write_series_json. """
        tilts = cls.tilts_from_star_rows([row._asdict() for row in ts_table], thumbnails_dir,
                                         ctf_correlation=False)
        return cls.write_series_json(cls.json_path(thumbnails_dir, tomo_name), tomo_name, tilts)

    # ------------------------------------------------------------------
    # The tilts of a tilt series star file
    # ------------------------------------------------------------------
    @staticmethod
    def _star_float(d, key, placeholder_zero=False):
        """ A star value as a float, None when missing or, for the values the
        pipeline leaves at 0 when it did not measure them, a zero. """
        v = d.get(key)
        if v in (None, '', 'None'):
            return None
        v = float(v)
        return None if placeholder_zero and v == 0 else v

    @classmethod
    def tilts_from_star_rows(cls, rows, thumbnails, root='', ctf_correlation=True):
        """ The tilts of a tilt series star file as the measures and the rule
        take them: 'movie', 'angle', 'name' (micrograph base name), 'mic' and
        'jpeg' (the tilt average and its jpeg, TiltThumbnailWriter; None
        without an average), the star values of the rule (placeholder zeros
        as None) and the CTF profile correlation.

        rows: dicts of the star rows (values as strings or numbers);
        thumbnails: the folder of the jpegs and jsons (the tilt analysis
        job's); root: what the paths in the rows are relative to;
        ctf_correlation: whether to read the _ctf.json of each tilt.  Raises
        ValueError without the movie index or the tilt angle.
        """
        tilts = []
        for d in rows:
            for col in ('rlnTomoTiltMovieIndex', 'rlnTomoNominalStageTiltAngle'):
                if d.get(col) in (None, '', 'None'):
                    raise ValueError(f"no '{col}': it is needed to follow the tilts in "
                                     "acquisition order")
            mic = d.get('rlnMicrographName')
            mic = None if mic in (None, '', 'None') else mic
            ps = d.get('rlnCtfPowerSpectrum')
            ps = None if ps in (None, '', 'None') else ps
            name = os.path.splitext(os.path.basename(mic or d.get('rlnMicrographMovieName') or ''))[0]
            jpeg = TiltThumbnailWriter.tilt_jpeg(mic, thumbnails) if mic else None
            tilts.append({
                'movie': int(float(d['rlnTomoTiltMovieIndex'])),
                'angle': float(d['rlnTomoNominalStageTiltAngle']),
                'name': name,
                'mic': os.path.join(root, mic) if mic else None,
                'jpeg': jpeg,
                'motion': cls._star_float(d, 'rlnAccumMotionTotal', True),
                'ctfResolution': cls._star_float(d, 'rlnCtfMaxResolution', True),
                'figureOfMerit': cls._star_float(d, 'rlnCtfFigureOfMerit', True),
                'astigmatism': cls._star_float(d, 'rlnCtfAstigmatism'),
                'defocus': cls._star_float(d, 'rlnDefocusU'),
                'ctfCorrelation': (TiltThumbnailWriter.read_ctf_correlation(ps, thumbnails)
                                   if ctf_correlation and ps else None),
            })
        return tilts
