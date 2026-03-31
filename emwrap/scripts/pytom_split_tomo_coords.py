#!/usr/bin/env python3
# **************************************************************************
# *
# * Split Pytom picking result (tomo_coords.star) into singleshot and multishot.
# * Reads the global table; for each row:
# *   - If rlnTomoName starts with 'grid1_Position_' and the number after it is < 120
# *     -> add to singleshot_tomo_coords.star
# *   - All other rows -> multishot_tomo_coords.star
# *
# * Usage:
# *   python -m emwrap.pytom.split_tomo_coords tomo_coords.star
# *   python -m emwrap.pytom.split_tomo_coords tomo_coords.star --output-dir /path
# *
# **************************************************************************

import argparse
import os
import re
import sys

from emtools.metadata import StarFile, Table

PREFIX = 'grid1_Position_'
SINGLESHOT_OUT = 'singleshot_tomo_coords.star'
MULTISHOT_OUT = 'multishot_tomo_coords.star'
POSITION_THRESHOLD = 120


def _position_number(tomo_name):
    """
    If tomo_name starts with PREFIX, return the integer after it, else None.
    Handles suffix like 'grid1_Position_1' -> 1, 'grid1_Position_119' -> 119.
    """
    if not tomo_name or not tomo_name.startswith(PREFIX):
        return None
    suffix = tomo_name[len(PREFIX):].strip()
    # Allow optional trailing non-digit part (e.g. 120_extra -> 120)
    m = re.match(r'^(\d+)', suffix)
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None


def split_tomo_coords(star_path, output_dir=None):
    """
    Read global table from star_path; split rows into singleshot (position < 120)
    and multishot (rest). Write two star files. Returns (n_singleshot, n_multishot).
    """
    star_path = os.path.abspath(star_path)
    if not os.path.isfile(star_path):
        raise FileNotFoundError(f"Star file not found: {star_path}")

    output_dir = output_dir or os.path.dirname(star_path)
    output_dir = os.path.abspath(output_dir)
    os.makedirs(output_dir, exist_ok=True)

    global_table = StarFile.getTableFromFile('global', star_path)
    col_names = global_table.getColumnNames()
    single_table = Table(col_names)
    multi_table = Table(col_names)

    for row in global_table:
        tomo_name = getattr(row, 'rlnTomoName', None)
        if tomo_name is None:
            multi_table.addRowValues(**row._asdict())
            continue
        num = _position_number(tomo_name)
        if num is not None and num < POSITION_THRESHOLD:
            single_table.addRowValues(**row._asdict())
        else:
            multi_table.addRowValues(**row._asdict())

    single_path = os.path.join(output_dir, SINGLESHOT_OUT)
    multi_path = os.path.join(output_dir, MULTISHOT_OUT)

    with StarFile(single_path, 'w') as sf:
        sf.writeTable('global', single_table, timeStamp=True, computeFormat='left')
    with StarFile(multi_path, 'w') as sf:
        sf.writeTable('global', multi_table, timeStamp=True, computeFormat='left')

    return len(single_table), len(multi_table)


def main():
    parser = argparse.ArgumentParser(
        description='Split tomo_coords.star into singleshot (grid1_Position_* < 120) and multishot.'
    )
    parser.add_argument(
        'input_star',
        help='Input star file (e.g. tomo_coords.star) with global table',
    )
    parser.add_argument(
        '--output-dir', '-o',
        dest='output_dir',
        default=None,
        help='Directory for output star files (default: same as input)',
    )
    args = parser.parse_args()

    try:
        n_single, n_multi = split_tomo_coords(args.input_star, output_dir=args.output_dir)
    except FileNotFoundError as e:
        print(e, file=sys.stderr)
        sys.exit(1)

    out_dir = args.output_dir or os.path.dirname(os.path.abspath(args.input_star))
    print(f"Input: {args.input_star}")
    print(f"Output dir: {out_dir}")
    print(f"  {SINGLESHOT_OUT}: {n_single} row(s)  (grid1_Position_* < {POSITION_THRESHOLD})")
    print(f"  {MULTISHOT_OUT}:  {n_multi} row(s)")
    sys.exit(0)


if __name__ == '__main__':
    main()
