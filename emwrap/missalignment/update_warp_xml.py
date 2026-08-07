#!/usr/bin/env python
"""Update physical image and volume dimensions in Warp tilt-series XML files."""

import argparse
from pathlib import Path

import torch
from warpylib import TiltSeries


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            'Update image_dimensions_physical and volume_dimensions_physical '
            'in Warp tilt-series XML files.'
        )
    )
    parser.add_argument(
        '--xml-directory',
        required=True,
        help='Directory containing Warp tilt-series XML files.',
    )
    parser.add_argument('--image-x', type=int, required=True)
    parser.add_argument('--image-y', type=int, required=True)
    parser.add_argument('--volume-x', type=int, required=True)
    parser.add_argument('--volume-y', type=int, required=True)
    parser.add_argument('--volume-z', type=int, required=True)
    parser.add_argument('--pixel-size', type=float, required=True)
    return parser.parse_args()


def update_xml_files(args):
    xml_directory = Path(args.xml_directory).expanduser().resolve()
    if not xml_directory.is_dir():
        raise NotADirectoryError(
            f'Warp XML directory does not exist: {xml_directory}'
        )

    xml_files = sorted(xml_directory.glob('*.xml'))
    if not xml_files:
        raise RuntimeError(
            f'No Warp tilt-series XML files found in {xml_directory}'
        )

    image_dimensions_physical = torch.tensor(
        [
            args.image_x * args.pixel_size,
            args.image_y * args.pixel_size,
        ],
        dtype=torch.float32,
    )
    volume_dimensions_physical = torch.tensor(
        [
            args.volume_x * args.pixel_size,
            args.volume_y * args.pixel_size,
            args.volume_z * args.pixel_size,
        ],
        dtype=torch.float32,
    )

    for xml_file in xml_files:
        tilt_series = TiltSeries(xml_file)
        tilt_series.image_dimensions_physical = image_dimensions_physical.clone()
        tilt_series.volume_dimensions_physical = volume_dimensions_physical.clone()
        tilt_series.save_meta(xml_file)
        print(f'Updated: {xml_file}')

    print(f'Updated {len(xml_files)} Warp XML files.')


def main():
    update_xml_files(parse_args())


if __name__ == '__main__':
    main()