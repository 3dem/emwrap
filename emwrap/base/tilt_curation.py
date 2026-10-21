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
from concurrent.futures import ThreadPoolExecutor

from emtools.utils import Color
from emtools.metadata import StarFile
from emtools.image import TiltThumbnails, tilt_qc

from emwrap.base import ProcessingPipeline
from emwrap.base.subset_ts import SubsetTsPipeline, MODE_EXPLICIT_NAMES
from emwrap.warp.warp_subset import WarpSubsetTs


# Form parameter -> (tilt_qc limit, factor from the form's unit to the
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
    curation rule (emtools.image.tilt_qc.curate_series, the one the OTF
    dashboard uses): blank, blacked out, a different field or shifted away
    (measured on the 512 px tilt jpegs), motion, CTF fit, defocus and
    astigmatism, and the tilts acquired after one that lost the field.

    The jpegs and image measures are the ones the job that made the tilt
    averages wrote (emwrap's wrappers do, see TiltThumbnails) when they
    exist; those missing are made here, into this job's thumbnails folder.

    Input and output are TiltSeries.  The output is written as by the
    subset job (emw-subset-ts) with every tilt series and the failing tilts
    excluded: a filtered tilt-series STAR file per tilt series, and the
    same tilts deselected in the Warp frame series state when the input
    job has one.
    """
    name = 'emw-tilt-curation'

    def __init__(self, args, output):
        ProcessingPipeline.__init__(self, args, output)
        self.inputSet = args['input_set']
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
                self.limits[key] = tilt_qc.DEFAULT_LIMITS[key]
            else:
                value = self._float(args[param], param)
                self.limits[key] = None if value is None else value * factor
        self.limits['beyond'] = args.get('reject_beyond', True)
        self.dropBelow = self._float(args.get('drop_series_below', ''), 'drop_series_below')
        self.threads = max(1, int(self._float(args.get('threads', 4), 'threads') or 1))

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
    def _read_tilts(self, tomoName, tsStar):
        """ The tilts of a tilt series (tilt_qc.tilts_from_star_rows), each
        with the jpeg to measure: the one of the job of its average when it
        exists, else one to make ('make') into this job's thumbnails folder,
        in <tomoName>/ (names may repeat across tilt series). """
        table = StarFile.getTableFromFile(tomoName, tsStar, guessType=False)
        if not table:
            raise Exception(f"Could not read tilt-series table '{tomoName}' from {tsStar}")
        try:
            tilts = tilt_qc.tilts_from_star_rows([row._asdict() for row in table])
        except ValueError as e:
            raise Exception(f"Tilt-series STAR file {tsStar} for '{tomoName}': {e}")
        for t in tilts:
            t['make'] = False
            if t['mic'] is None:
                continue        # no average: judged on its star values only
            if not (t['jpeg'] and os.path.exists(t['jpeg'])):
                t['jpeg'] = TiltThumbnails.tilt_jpeg(t['mic'], self._thumbnails(tomoName))
                t['make'] = True
        return tilts

    def _thumbnails(self, tomoName):
        return self.join(TiltThumbnails.FOLDER, tomoName)

    def _measures(self, tomoName, tilts):
        """ The image measures of the tilts.  Those of the job of the
        averages, when its json is up to date for every tilt: measured over
        all the motion corrected tilts, as the dashboard reads them, so the
        series medians and the field walk agree with it even for a subset;
        the tilts the input does not have come back as dropped (they count
        for the medians only).  Otherwise measured here, beside the jpegs
        made here. """
        jpeg = next((t['jpeg'] for t in tilts if t['jpeg']), None)
        if jpeg and not any(t['make'] for t in tilts):
            path = tilt_qc.json_path(os.path.dirname(jpeg), tomoName)
            rows = tilt_qc.read_series_json(path, tilts)
            if rows is not None:
                names = {t['name'] for t in tilts}
                return rows, [dict(r, dropped=True) for r in rows if r.get('name') not in names]
        rows, _ = tilt_qc.write_series_json(
            tilt_qc.json_path(self._thumbnails(tomoName), tomoName), tomoName, tilts)
        return rows or [], []

    def _curate_series(self, tomoName, tsStar):
        """ Make the missing jpegs, measure and apply the rule.  Returns
        (tilts, {movie: [reasons]}, number of jpegs made); the tilts with
        their measures and those relative to the series. """
        tilts = self._read_tilts(tomoName, tsStar)
        made = sum(TiltThumbnails.write_tilt(t['mic'], self._thumbnails(tomoName), medium=False)
                   for t in tilts if t['make'])
        rows, others = self._measures(tomoName, tilts)
        measures = {r['name']: r for r in rows}
        for t in tilts:
            m = measures.get(t['name'], {})
            t.update({k: m.get(k) for k in tilt_qc.MEASURES})
        series = tilts + others
        for t, (texture, dark) in zip(series, tilt_qc.relative_measures(series)):
            t['textureRel'], t['darkExcess'] = texture, dark
        return tilts, tilt_qc.curate_series(series, self.limits), made

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
            for tomoName, (tilts, rejected, _) in sorted(results.items()):
                for t in sorted(tilts, key=lambda t: t['angle']):
                    why = rejected.get(t['movie'], [])
                    w.writerow([tomoName, t['movie'], t['angle'], int(not why), ' '.join(why)]
                               + [t.get(k) for k in CSV_VALUES])
        os.replace(tmp, path)
        return path

    def _curate_all(self, series):
        """ _curate_series for every (name, star), in threads.  On a failure
        the tilt series not started yet are cancelled, so it shows at once
        rather than after the whole session. """
        results = {}
        with ThreadPoolExecutor(self.threads) as pool:
            futures = {name: pool.submit(self._curate_series, name, star) for name, star in series}
            try:
                for i, (name, fut) in enumerate(futures.items(), 1):
                    results[name] = fut.result()
                    if i % 25 == 0 or i == len(futures):
                        self.log(f"Curated {i} / {len(futures)} tilt series", flush=True)
            except Exception:
                for fut in futures.values():
                    fut.cancel()
                raise
        return results

    def prerun(self):
        if not os.path.exists(self.inputSet):
            raise Exception(f"Input STAR file not found: {self.inputSet}")
        globalTable = self._readGlobalTable(self.inputSet)
        inputType = self._detectGlobalTableType(self.inputSet, globalTable)
        if inputType != 'TiltSeries':
            raise Exception(f"The input must be TiltSeries (motion corrected tilt series, "
                            f"before alignment), got {inputType}: {self.inputSet}")
        # Warp cannot deselect single tilts once its tilt series state
        # exists: refuse before doing any work
        warpKeys = WarpSubsetTs.detectState(os.path.dirname(self.inputSet))
        if warpKeys & WarpSubsetTs.TILT_STATE_KEYS:
            raise Exception("The input job has a Warp tilt-series state, where single tilts "
                            "cannot be deselected: curate the motion correction output instead.")

        self.log(f"Input set: {Color.bold(self.inputSet)}, "
                 f"{Color.green(len(globalTable))} tilt series")
        self.log("Limits: " + ', '.join(f'{k}={v}' for k, v in self.limits.items()))

        series = [(r.rlnTomoName, r.rlnTomoTiltSeriesStarFile) for r in globalTable]
        results = self._curate_all(series)

        # What goes out: failing tilts, and the tilt series left with
        # nothing or (if asked) too few tilts
        dropped, reasons = {}, Counter()
        for name, (tilts, rejected, _) in results.items():
            kept = len(tilts) - len(rejected)
            reasons.update(r for why in rejected.values() for r in why)
            if kept == 0:
                dropped[name] = 'no tilt left'
            elif self.dropBelow is not None and kept / len(tilts) < self.dropBelow:
                dropped[name] = f'keeps {kept} of {len(tilts)} tilts'
            if rejected and name not in dropped:
                self.excludedTiltsMap[name] = set(rejected)
            self.log(f"Tilt series {Color.bold(name)}: kept {Color.green(kept)} / {len(tilts)}"
                     + (f", left out {Color.red(sorted(rejected))}" if rejected else '')
                     + (Color.red(f" -> tilt series left out ({dropped[name]})") if name in dropped else ''))

        self.subsetNames = {name for name, _ in series} - set(dropped)
        if not self.subsetNames:
            raise Exception("Every tilt series would be left out; check the limits.")
        report = self._write_report(results)

        outputType, count = self._subsetGlobalInput(self.inputSet, self.subsetNames)
        if warpKeys:
            WarpSubsetTs(self._args, self.outputDir).apply(self.subsetNames, self.excludedTiltsMap)

        nTilts = sum(len(r[0]) for r in results.values())
        nOut = sum(len(r[1]) for r in results.values())
        self.inputs = {
            'input_set': self.inputSet,
            'limits': self.limits,
            'drop_series_below': self.dropBelow,
        }
        
        reasons = {tilt_qc.REASON_LABELS.get(k, k): v for k, v in reasons.most_common()},
            
        self.writeInfo()
        self.log(f"Left out {Color.red(nOut)} of {Color.bold(nTilts)} tilts in "
                 f"{Color.green(len(self.excludedTiltsMap))} tilt series"
                 + (f", and {Color.red(len(dropped))} tilt series" if dropped else '')
                 + ": " + ', '.join(f'{v} {k}' for k, v in reasons.items()))
        self.log(f"Per tilt measures and decisions: {Color.cyan(report)}")


if __name__ == '__main__':
    TiltCurationPipeline.main()
