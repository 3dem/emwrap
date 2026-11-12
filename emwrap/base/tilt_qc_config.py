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
""" The tilt QC configuration: the thresholds the OTF dashboard categorizes
tilts and tilt series with, and the limits of the curation rule
(TiltQualityCuration), in one json file per job.

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

import os
import json
import math
import time
import copy

from emwrap.base.tilt_quality_curation import TiltQualityCuration


class TiltQcConfig:
    VERSION = 1
    TYPE = 'emwrap.TiltQcConfig'
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
    for _limit, (_key, _field) in RULE_KEYS.items():
        DEFAULT_THRESHOLDS.setdefault(_key, {})[_field] = TiltQualityCuration.DEFAULT_LIMITS[_limit]
    del _limit, _key, _field

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
