#!/usr/bin/env python3
# **************************************************************************
# *
# * Check WarpMotionCtf job folder for missing xml / average mrc per input movie.
# * A failed item = one movie (prefix) where the xml and/or the average mrc is missing.
# * Expectations match warp_mctf._output:
# *   warp_frameseries/{prefix}.xml
# *   warp_frameseries/average/{prefix}.mrc
# *
# * Movies are discovered from job_folder/frames/ (symlinks created by
# * WarpMotionCtf._create_settings). Optional --input-ts uses the tilt series
# * star instead when frames/ is empty or unavailable.
# *
# * Usage:
# *   python check_mctf_outputs.py /path/to/job003
# *   python check_mctf_outputs.py /path/to/job003 --input-ts /path/to/tilt_series.star
# *   python check_mctf_outputs.py /path/to/job003 --quiet   # one path per line
# *
# **************************************************************************

import argparse
import json
import os
import sys

# Must match WarpBasePipeline.FS in warp.py
WARP_FRAMESERIES = 'warp_frameseries'


def _movie_prefix(path_or_name):
    """Same as Path.removeBaseExt: basename without extension."""
    return os.path.splitext(os.path.basename(path_or_name))[0]


def _input_ts_from_info(job_folder):
    """Get input tilt series star path from job info.json."""
    info_path = os.path.join(job_folder, 'info.json')
    if not os.path.exists(info_path):
        return None
    with open(info_path) as f:
        info = json.load(f)
    inputs = info.get('inputs') or {}
    for key in ('FrameSeries', 'TiltSeries'):
        block = inputs.get(key)
        if not block:
            continue
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


def _prefixes_from_frames_dir(frames_dir):
    """Yield unique movie prefixes from filenames/symlinks in frames/."""
    if not os.path.isdir(frames_dir):
        return
    seen = set()
    for name in os.listdir(frames_dir):
        path = os.path.join(frames_dir, name)
        if os.path.isfile(path) or os.path.islink(path):
            prefix = _movie_prefix(name)
            if prefix not in seen:
                seen.add(prefix)
                yield prefix, name


def _prefixes_from_star(ts_star_path):
    """Yield (ts_name, movie_name, prefix) using emtools StarFile if available."""
    try:
        from emtools.metadata import StarFile
    except ImportError:
        raise SystemExit(
            "emtools not available; install emtools or use a job folder with frames/ populated."
        )
    ts_star_path = os.path.abspath(ts_star_path)
    if not os.path.exists(ts_star_path):
        return
    ts_all = StarFile.getTableFromFile('global', ts_star_path)
    seen = set()
    for ts_row in ts_all:
        ts_name = ts_row.rlnTomoName
        ts_star = ts_row.rlnTomoTiltSeriesStarFile
        if not os.path.isabs(ts_star):
            ts_star = os.path.join(os.path.dirname(ts_star_path), ts_star)
        if not os.path.exists(ts_star):
            continue
        ts_table = StarFile.getTableFromFile(ts_name, ts_star)
        for frame_row in ts_table:
            movie = frame_row.rlnMicrographMovieName
            prefix = _movie_prefix(movie)
            if prefix in seen:
                continue
            seen.add(prefix)
            yield ts_name, movie, prefix


def collect_expected_prefixes(job_folder, input_ts=None):
    """
    Return list of dicts: prefix, movie (optional), ts_name (optional).
    """
    job_folder = os.path.abspath(job_folder)
    frames_dir = os.path.join(job_folder, 'frames')
    out = []

    for prefix, name in _prefixes_from_frames_dir(frames_dir):
        out.append({'prefix': prefix, 'movie': name, 'ts_name': None})

    if out:
        return out

    if not input_ts:
        input_ts = _input_ts_from_info(job_folder)
    if not input_ts:
        raise SystemExit(
            "No movies found under frames/ and no input tilt series. "
            "Pass --input-ts or run from job folder with info.json."
        )
    input_ts = _resolve_star_path(job_folder, input_ts)
    if not os.path.exists(input_ts):
        raise SystemExit(f"Input tilt series star not found: {input_ts}")

    for ts_name, movie, prefix in _prefixes_from_star(input_ts):
        out.append({'prefix': prefix, 'movie': movie, 'ts_name': ts_name})
    return out


def check_job_folder(job_folder, input_ts=None):
    """Return list of missing entries with xml_path, mrc_path, flags."""
    job_folder = os.path.abspath(job_folder)
    fs_dir = os.path.join(job_folder, WARP_FRAMESERIES)
    avg_dir = os.path.join(fs_dir, 'average')

    expected = collect_expected_prefixes(job_folder, input_ts=input_ts)
    missing = []

    for item in expected:
        prefix = item['prefix']
        xml_path = os.path.join(fs_dir, prefix + '.xml')
        mrc_path = os.path.join(avg_dir, prefix + '.mrc')
        has_xml = os.path.exists(xml_path)
        has_mrc = os.path.exists(mrc_path)
        if not has_xml or not has_mrc:
            missing.append({
                'ts_name': item.get('ts_name'),
                'movie': item.get('movie'),
                'prefix': prefix,
                'missing_xml': not has_xml,
                'missing_mrc': not has_mrc,
                'xml_path': xml_path,
                'mrc_path': mrc_path,
            })
    return missing


def main():
    parser = argparse.ArgumentParser(
        description='List missing xml / average mrc for WarpMotionCtf job folder.'
    )
    parser.add_argument(
        'job_folder',
        help='WarpMotionCtf output job directory (contains warp_frameseries/)',
    )
    parser.add_argument(
        '--input-ts',
        dest='input_ts',
        help='Input tilt series star if frames/ is missing or incomplete',
    )
    parser.add_argument(
        '--quiet',
        action='store_true',
        help='Print only missing paths, one per line',
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
            print("All expected xml and average mrc files are present.")
            print("Total failed items: 0  (no movie missing xml or mrc)")
        sys.exit(0)

    # One failed item = one input movie (prefix) with missing xml and/or mrc
    n_failed = len(missing)
    n_missing_xml = sum(1 for m in missing if m['missing_xml'])
    n_missing_mrc = sum(1 for m in missing if m['missing_mrc'])

    if args.quiet:
        print(f"Total failed items: {n_failed} (missing xml: {n_missing_xml}, "
              f"missing mrc: {n_missing_mrc})", file=sys.stderr)
        for m in missing:
            if m['missing_xml']:
                print(m['xml_path'])
            if m['missing_mrc']:
                print(m['mrc_path'])
        sys.exit(1)

    print(f"Missing under {os.path.join(args.job_folder, WARP_FRAMESERIES)} "
          f"— {n_failed} failed item(s) "
          f"(missing xml: {n_missing_xml}, missing mrc: {n_missing_mrc})\n")
    for m in missing:
        print(f"  prefix: {m['prefix']}")
        if m.get('movie'):
            print(f"    movie:  {m['movie']}")
        if m.get('ts_name'):
            print(f"    ts:     {m['ts_name']}")
        if m['missing_xml']:
            print(f"    MISSING xml: {m['xml_path']}")
        if m['missing_mrc']:
            print(f"    MISSING mrc: {m['mrc_path']}")
        print()
    print(f"Total failed items: {n_failed}")
    sys.exit(1)


if __name__ == '__main__':
    main()
