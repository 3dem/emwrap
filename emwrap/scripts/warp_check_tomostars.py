#!/usr/bin/env python3
# **************************************************************************
# *
# * Check .tomostar files in warp_tomostar folder: print items whose main
# * table has fewer rows than a given threshold.
# *
# * Usage:
# *   python check_tomostars.py /path/to/warp_tomostar --min-rows 40
# *   python check_tomostars.py /path/to/job003 --min-rows 30  # job003/warp_tomostar
# *
# **************************************************************************

import argparse
import glob
import os
import sys

from emtools.metadata import StarFile
from emtools.utils import Color

# WarpBasePipeline.TM
WARP_TOMOSTAR = 'warp_tomostar'


def _get_table_row_count(star_path, table_name=None):
    """
    Return row count for the given table, or for the first table if table_name is None.
    Uses getTableSize for efficiency. Returns (count, table_name_used) or (None, None) on error.
    """
    if not os.path.isfile(star_path):
        return None, None
    with StarFile(star_path) as sf:
        names = sf.getTableNames()
        if not names:
            return None, None
        if table_name is not None:
            if table_name not in names:
                return None, table_name
            return sf.getTableSize(table_name), table_name
        # Use first table as the main one
        first = names[0]
        return sf.getTableSize(first), first


def check_folder(folder, min_rows, table_name=None):
    """
    Scan folder for *.tomostar and return list of dicts for all files.
    Each dict: path, basename, row_count, table_name, below_threshold.
    """
    folder = os.path.abspath(folder)
    if not os.path.isdir(folder):
        return None, None

    pattern = os.path.join(folder, '*.tomostar')
    files = sorted(glob.glob(pattern))
    all_results = []
    below = []
    for path in files:
        count, tname = _get_table_row_count(path, table_name=table_name)
        basename = os.path.basename(path)
        below_threshold = count is None or count < min_rows
        item = {
            'path': path,
            'basename': basename,
            'row_count': count,
            'table_name': tname,
            'below_threshold': below_threshold,
        }
        all_results.append(item)
        if below_threshold:
            below.append(item)
    return all_results, below


def main():
    parser = argparse.ArgumentParser(
        description='List .tomostar files whose table has fewer rows than a threshold.'
    )
    parser.add_argument(
        'folder',
        help='Folder containing .tomostar files (e.g. warp_tomostar or job dir)',
    )
    parser.add_argument(
        '--min-rows',
        dest='min_rows',
        type=int,
        required=True,
        metavar='N',
        help='Report files whose table has fewer than N rows',
    )
    parser.add_argument(
        '--table',
        dest='table_name',
        default=None,
        help='Table name to check (default: first table in each file)',
    )
    parser.add_argument(
        '--quiet',
        action='store_true',
        help='Print only paths, one per line',
    )
    args = parser.parse_args()

    folder = os.path.abspath(args.folder)
    if os.path.isdir(folder):
        # If folder looks like a job dir, use warp_tomostar inside it
        tm_dir = os.path.join(folder, WARP_TOMOSTAR)
        if os.path.isdir(tm_dir):
            folder = tm_dir
    else:
        print(f"Not a directory: {args.folder}", file=sys.stderr)
        sys.exit(2)

    try:
        all_results, below = check_folder(folder, args.min_rows, table_name=args.table_name)
    except SystemExit as e:
        print(e, file=sys.stderr)
        sys.exit(1)

    if all_results is None:
        print(f"Not a directory: {folder}", file=sys.stderr)
        sys.exit(2)

    # Table: basename | number of lines (red if below threshold)
    col1_w = max(len("basename"), max((len(r['basename']) for r in all_results), default=0))
    col2_w = max(len("rows"), len(str(args.min_rows)) + 2)
    fmt = f"  {{0:<{col1_w}}}  {{1:>{col2_w}}}"

    if not args.quiet:
        print(f"\n  {'basename':<{col1_w}}  {'rows':>{col2_w}}")
        print("  " + "-" * (col1_w + col2_w + 2))
        for item in all_results:
            count_str = str(item['row_count']) if item['row_count'] is not None else "?"
            line = fmt.format(item['basename'], count_str)
            if item['below_threshold']:
                print(Color.red(line))
            else:
                print(line)
        if below:
            print(f"\nTotal below threshold ({args.min_rows}): {len(below)}")

    if not below:
        sys.exit(0)

    if args.quiet:
        for item in below:
            print(item['path'])
    sys.exit(1)


if __name__ == '__main__':
    main()
