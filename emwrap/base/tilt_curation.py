# **************************************************************************
# *
# * Authors:     Daniel Marchan Torres (danielmarchan3@gmail.com)
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
import csv
from collections import Counter

from emtools.utils import Color
from emtools.metadata import StarFile

from emwrap.base import ProcessingPipeline
from emwrap.base.subset_ts import SubsetTsPipeline, MODE_EXPLICIT_NAMES
from emwrap.base.tilt_quality_curation import TiltQualityCuration as tqc
from emwrap.warp.warp_subset import WarpSubsetTs


# Form parameter -> (curation rule limit, factor from the form's unit to the
# rule's).  Defocus and astigmatism are given in µm, stored in Å.
LIMIT_PARAMS = {
    'texture_min': ('texture', 1.0),
    'dark_area_max': ('darkArea', 1.0),
    'match_peak_min': ('matchPeak', 1.0),
    'match_ncc_min': ('matchNcc', 1.0),
    'field_shift_max': ('fieldShift', 1.0),
    'motion_max': ('motion', 1.0),
    'ctf_resolution_max': ('ctfResolution', 1.0),
    'ctf_correlation_min': ('ctfCorrelation', 1.0),
    'fom_min': ('figureOfMerit', 1.0),
    'astigmatism_max': ('astigmatism', 1e4),
    'defocus_max': ('defocus', 1e4),
}
# Columns of tilt_curation.csv after the decision: the rule's inputs, with
# texture and dark area relative to the series as the limits apply to them
CSV_VALUES = ['motion', 'ctfResolution', 'ctfCorrelation', 'figureOfMerit', 'defocus',
              'astigmatism', 'textureRel', 'darkExcess', 'matchPeak', 'matchNcc', 'fieldShift']


class TiltCurationPipeline(SubsetTsPipeline):
    """ Leave out the tilt images of each tilt series that fail the
    curation rule (TiltQualityCuration.curate_series, the one the OTF
    dashboard uses): blank, blacked out, a different field or shifted away,
    motion, CTF fit, defocus and astigmatism, and the tilts acquired after
    one that lost the field.

    Input: a tilt analysis (emw-tilt-analyze, TiltSeriesAnalysis).  Its
    image measures and CTF fit correlations, and the motion and CTF values
    of the tilt series it analyzed, are what the rule is applied to; this
    job computes nothing from the images.

    Output: the subset (emw-subset-ts, SubsetTsPipeline._writeSubset) of the
    tilt series the analysis was made on, with every analyzed tilt series
    and the failing tilts excluded: a filtered tilt-series STAR file per
    tilt series, and the same tilts deselected in the Warp frame series
    state when that input job has one.  The per tilt report
    (tilt_curation.csv) and the summary are written after it (postrun).
    """
    name = 'emw-tilt-curation'

    def __init__(self, args, output):
        ProcessingPipeline.__init__(self, args, output)
        self.analysisStar = args['input_analysis']
        analysis = self._readAnalysis(self.analysisStar)
        self.thumbnails = analysis.emwThumbnailsFolder
        # The subset is of the tilt series the analysis was made on (also
        # for WarpSubsetTs, that reads it from the args)
        self.inputSet = self._args['input_set'] = analysis.emwInputTiltSeries
        # As a subset job of every tilt series, with excluded tilts
        self.mode = MODE_EXPLICIT_NAMES
        self.subsetNames = set()
        self.excludedTiltsMap = {}
        self.randomCount = self.randomSeed = None
        self.matchSet = ''

        # A parameter not given keeps the rule's default; an empty one
        # turns that check off
        self.limits = {}
        for param, (key, factor) in LIMIT_PARAMS.items():
            if param not in args:
                self.limits[key] = tqc.DEFAULT_LIMITS[key]
            else:
                value = self._float(args[param], param)
                self.limits[key] = None if value is None else value * factor
        self.limits['beyond'] = args.get('reject_beyond', True)
        self.dropBelow = self._float(args.get('drop_series_below', ''), 'drop_series_below')
        # Filled by _prepareDeselection, reported in postrun
        self.results = {}
        self.droppedSeries = {}

    @staticmethod
    def _readAnalysis(path):
        """ The 'analysis' row of a tilt_analysis.star (emw-tilt-analyze). """
        if not os.path.exists(path):
            raise Exception(f"Input tilt analysis not found: {path}")
        table = StarFile.getTableFromFile('analysis', path, guessType=False)
        if not table:
            raise Exception(f"{path} is not a tilt analysis (emw-tilt-analyze output)")
        return table[0]

    @staticmethod
    def _float(value, name):
        """ A float, or None for an empty value: that check is not used. """
        if value is None or str(value).strip() == '':
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            raise Exception(f"Invalid '{name}' value: {value!r}. Expected a number, "
                            "or leave it empty to not use it.")

    # ------------------------------------------------------------------
    # Per tilt series
    # ------------------------------------------------------------------
    def _curate_series(self, tomoName, tsStar, qcJson):
        """ Apply the rule to the tilts of a tilt series, with the analysis'
        image measures (none if it has no json for it: judged on the other
        values only).  Returns (tilts, {movie: [reasons]}); the tilts with
        their measures and those relative to the series. """
        table = StarFile.getTableFromFile(tomoName, tsStar, guessType=False)
        if not table:
            raise Exception(f"Could not read tilt-series table '{tomoName}' from {tsStar}")
        try:
            tilts = tqc.tilts_from_star_rows([row._asdict() for row in table], self.thumbnails)
        except ValueError as e:
            raise Exception(f"Tilt-series STAR file {tsStar} for '{tomoName}': {e}")
        rows = tqc.read_series_json(qcJson) if qcJson else None
        if rows is None:
            self.log(f"WARNING: no image measures for TS {tomoName} in the analysis: "
                     "judged on its motion and CTF values only")
            rows = []
        measures = {r.get('name'): r for r in rows}
        for t in tilts:
            m = measures.get(t['name'], {})
            t.update({k: m.get(k) for k in tqc.MEASURES})
        # Measured tilts the tilt series does not have count for the medians only
        names = {t['name'] for t in tilts}
        series = tilts + [dict(r, dropped=True) for r in rows if r.get('name') not in names]
        for t, (texture, dark) in zip(series, tqc.relative_measures(series)):
            t['textureRel'], t['darkExcess'] = texture, dark
        return tilts, tqc.curate_series(series, self.limits)

    # ------------------------------------------------------------------
    # Job
    # ------------------------------------------------------------------
    def _write_report(self, results):
        """ tilt_curation.csv: one row per tilt with the rule's inputs,
        whether it was kept and why not.  Written then renamed. """
        path = self.join('tilt_curation.csv')
        tmp = path + '.tmp'
        with open(tmp, 'w', newline='') as f:
            w = csv.writer(f)
            w.writerow(['tomoName', 'movieIndex', 'tiltAngle', 'kept', 'reasons'] + CSV_VALUES)
            for tomoName, (tilts, rejected) in sorted(results.items()):
                for t in sorted(tilts, key=lambda t: t['angle']):
                    why = rejected.get(t['movie'], [])
                    w.writerow([tomoName, t['movie'], t['angle'], int(not why), ' '.join(why)]
                               + [t.get(k) for k in CSV_VALUES])
        os.replace(tmp, path)
        return path

    # ------------------------------------------------------------------
    # The deselection: by the curation rule (the output is the subset
    # job's, SubsetTsPipeline._writeSubset)
    # ------------------------------------------------------------------
    def _prepareDeselection(self):
        """ Curate every tilt series of the analysis: the failing tilts go to
        self.excludedTiltsMap, and the tilt series left with nothing or (if
        asked) too few tilts out of self.subsetNames. """
        globalTable = self._readGlobalTable(self.inputSet)
        inputType = self._detectGlobalTableType(self.inputSet, globalTable)
        if inputType != 'TiltSeries':
            raise Exception(f"The analysis must be of TiltSeries (motion corrected tilt series, "
                            f"before alignment), got {inputType}: {self.inputSet}")
        # Warp cannot deselect single tilts once its tilt series state
        # exists: refuse before doing any work
        warpKeys = WarpSubsetTs.detectState(os.path.dirname(self.inputSet))
        if warpKeys & WarpSubsetTs.TILT_STATE_KEYS:
            raise Exception("The input job has a Warp tilt-series state, where single tilts "
                            "cannot be deselected: curate an analysis of the motion "
                            "correction output instead.")

        analyzed = StarFile.getTableFromFile('global', self.analysisStar, guessType=False)
        inputNames = {r.rlnTomoName for r in globalTable}
        series = [(r.rlnTomoName, r.rlnTomoTiltSeriesStarFile, r.emwImageQcFile)
                  for r in analyzed if r.rlnTomoName in inputNames]
        self.log(f"Analysis: {Color.bold(self.analysisStar)}")
        self.log(f"Curating {Color.green(len(series))} analyzed tilt series of "
                 f"{Color.bold(len(globalTable))} in {self.inputSet}")
        if notAnalyzed := len(inputNames) - len(series):
            self.log(f"{Color.red(notAnalyzed)} tilt series not analyzed are left out")
        self.log("Limits: " + ', '.join(f'{k}={v}' for k, v in self.limits.items()))

        for name, tsStar, qcJson in series:
            self.results[name] = self._curate_series(name, tsStar, qcJson)

        for name, (tilts, rejected) in self.results.items():
            kept = len(tilts) - len(rejected)
            if kept == 0:
                self.droppedSeries[name] = 'no tilt left'
            elif self.dropBelow is not None and kept / len(tilts) < self.dropBelow:
                self.droppedSeries[name] = f'keeps {kept} of {len(tilts)} tilts'
            if rejected and name not in self.droppedSeries:
                self.excludedTiltsMap[name] = set(rejected)
            self.log(f"Tilt series {Color.bold(name)}: kept {Color.green(kept)} / {len(tilts)}"
                     + (f", left out {Color.red(sorted(rejected))}" if rejected else '')
                     + (Color.red(f" -> tilt series left out ({self.droppedSeries[name]})")
                        if name in self.droppedSeries else ''))

        self.subsetNames = {name for name, _, _ in series} - set(self.droppedSeries)
        if not self.subsetNames:
            raise Exception("Every tilt series would be left out; check the limits.")

    # ------------------------------------------------------------------
    # The curation's own outputs, once the subset is written
    # ------------------------------------------------------------------
    def postrun(self):
        report = self._write_report(self.results)
        reasons = Counter(r for _, rejected in self.results.values()
                          for why in rejected.values() for r in why)
        reasons = {tqc.REASON_LABELS.get(k, k): v for k, v in reasons.most_common()}
        nTilts = sum(len(tilts) for tilts, _ in self.results.values())
        nOut = sum(len(rejected) for _, rejected in self.results.values())

        self.log(f"Left out {Color.red(nOut)} of {Color.bold(nTilts)} tilts in "
                 f"{Color.green(len(self.excludedTiltsMap))} tilt series"
                 + (f", and {Color.red(len(self.droppedSeries))} tilt series"
                    if self.droppedSeries else '')
                 + ": " + ', '.join(f'{v} {k}' for k, v in reasons.items()))
        self.log(f"Per tilt measures and decisions: {Color.cyan(report)}")


if __name__ == '__main__':
    TiltCurationPipeline.main()
