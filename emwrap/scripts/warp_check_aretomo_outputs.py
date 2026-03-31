#!/usr/bin/env python3
# **************************************************************************
# *
# * Check WarpAreTomo (WarpBaseTsAlign) job folder for missing aligned stacks.
# * Matches warp.py _output: each tilt series should have
# *   warp_tiltseries/tiltstack/{tsName}/{tsName}_aligned.mrc
# *
# * A failed item = one tilt series whose aligned mrc is missing.
# *
# * Usage:
# *   python check_aretomo_outputs.py /path/to/job003
# *   python check_aretomo_outputs.py /path/to/job003 --input-ts /path/to/tilt_series.star
# *   python check_aretomo_outputs.py /path/to/job003 --quiet   # one path per line
# *
# **************************************************************************

import argparse
import json
import os
import sys

# WarpBasePipeline.TS
WARP_TILTSERIES = 'warp_tiltseries'
TILTSTACK = 'tiltstack'


def _aligned_mrc_path(job_folder, ts_name):
    """Path expected by WarpBaseTsAlign._output."""
    return os.path.join(
        job_folder, WARP_TILTSERIES, TILTSTACK, ts_name, f"{ts_name}_aligned.mrc"
    )


def _input_ts_from_info(job_folder):
    """Input tilt series star from job info.json (AreTomo registers TiltSeries)."""
    info_path = os.path.join(job_folder, 'info.json')
    if not os.path.exists(info_path):
        return None
    with open(info_path) as f:
        info = json.load(f)
    inputs = info.get('inputs') or {}
    block = inputs.get('TiltSeries')
    if block:
        for entry in block.get('files') or []:
            if isinstance(entry, (list, tuple)) and len(entry) >= 1:
                return entry[0]
    return None


def _resolve_star_path(job_folder, star_path):
    if os.path.isabs(star_path) and os.path.exists(star_path):
        return os.path.abspath(star_path)
    for base in (job_folder, os.path.dirname(os.path.abspath(job_folder)), os.getcwd()):
        p = os.path.join(base, star_path) if not os.path.isabs(star_path) else star_path
        if os.path.exists(p):
            return os.path.abspath(p)
    return star_path


def _ts_names_from_star(ts_star_path):
    """Yield rlnTomoName from global table."""
    try:
        from emtools.metadata import StarFile
    except ImportError:
        raise SystemExit(
            "emtools not available; pass --input-ts after installing emtools, "
            "or use a job with warp_tiltseries/tiltstack/ populated."
        )
    ts_star_path = os.path.abspath(ts_star_path)
    if not os.path.exists(ts_star_path):
        return
    ts_all = StarFile.getTableFromFile('global', ts_star_path)
    seen = set()
    for ts_row in ts_all:
        name = ts_row.rlnTomoName
        if name not in seen:
            seen.add(name)
            yield name


def _ts_names_from_tiltstack(job_folder):
    """Yield ts_name from subdirs of tiltstack (no star needed)."""
    tiltstack_dir = os.path.join(job_folder, WARP_TILTSERIES, TILTSTACK)
    if not os.path.isdir(tiltstack_dir):
        return
    for name in sorted(os.listdir(tiltstack_dir)):
        path = os.path.join(tiltstack_dir, name)
        if os.path.isdir(path):
            yield name


def collect_expected_ts_names(job_folder, input_ts=None):
    """Return sorted list of tilt series names to check."""
    job_folder = os.path.abspath(job_folder)
    names = []

    if not input_ts:
        input_ts = _input_ts_from_info(job_folder)
    if input_ts:
        input_ts = _resolve_star_path(job_folder, input_ts)
        if os.path.exists(input_ts):
            names.extend(_ts_names_from_star(input_ts))

    if not names:
        # Fall back: every subdir under tiltstack is an expected TS
        names.extend(_ts_names_from_tiltstack(job_folder))

    if not names:
        raise SystemExit(
            "No tilt series to check. Pass --input-ts pointing at the input "
            "TomogramGroupMetadata star, or ensure warp_tiltseries/tiltstack/ exists."
        )

    # Unique preserve order
    seen = set()
    out = []
    for n in names:
        if n not in seen:
            seen.add(n)
            out.append(n)
    return out


def check_job_folder(job_folder, input_ts=None):
    """Return list of dicts: ts_name, aligned_path for each missing aligned mrc."""
    job_folder = os.path.abspath(job_folder)
    missing = []
    for ts_name in collect_expected_ts_names(job_folder, input_ts=input_ts):
        aligned_path = _aligned_mrc_path(job_folder, ts_name)
        if not os.path.exists(aligned_path):
            missing.append({
                'ts_name': ts_name,
                'aligned_path': aligned_path,
            })
    return missing


def main():
    parser = argparse.ArgumentParser(
        description='List missing aligned tilt-series stacks for WarpAreTomo job folder.'
    )
    parser.add_argument(
        'job_folder',
        help='WarpAreTomo output job directory (contains warp_tiltseries/)',
    )
    parser.add_argument(
        '--input-ts',
        dest='input_ts',
        help='Input tilt series star (motioncorr) if not in info.json',
    )
    parser.add_argument(
        '--quiet',
        action='store_true',
        help='Print only missing aligned mrc paths, one per line',
    )
    args = parser.parse_args()

    if not os.path.isdir(args.job_folder):
        print(f"Not a directory: {args.job_folder}", file=sys.stderr)
        sys.exit(2)

    try:
        missing = check_job_folder(args.job_folder, input_ts=args.input_ts)
    except SystemExit as e:
        print(e, file=sys.stderr)
        sys.exit(1)

    if not missing:
        if not args.quiet:
            print("All expected aligned tilt-series mrc files are present.")
            print("Total failed items: 0  (no tilt series missing aligned stack)")
        sys.exit(0)

    n_failed = len(missing)

    if args.quiet:
        print(f"Total failed items: {n_failed}", file=sys.stderr)        
        sys.exit(1)

    base = os.path.join(args.job_folder, WARP_TILTSERIES, TILTSTACK)
    print(f"Missing aligned stacks under {base} — {n_failed} failed item(s)\n")
    for m in missing:
        print(f"  ts_name: {m['ts_name']}")
        print(f"    MISSING: {m['aligned_path']}")
        print()
    print(f"Total failed items: {n_failed}")
    sys.exit(1)


if __name__ == '__main__':
    main()
