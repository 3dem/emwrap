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
import sys
import argparse

from emtools.utils import Color

from .test_apof_warp import TestApoFWarp
from .test_apof_aretomo3 import TestAretomo3ApoF

tests_map = {
    'apof_warp': TestApoFWarp,
    'apof_aretomo3': TestAretomo3ApoF
}


def main(raw_args):
    """ Entry point for all available tests, exposed as subcommands. """

    p = argparse.ArgumentParser(prog="emwrap.tests")
    subparsers = p.add_subparsers(dest='test_name', required=False)

    subparsers.add_parser('list', help='List all available tests')

    for test_name, test_class in tests_map.items():
        test_parser = subparsers.add_parser(test_name, help=test_class.__doc__)
        test_class.set_args(test_parser)

    args = p.parse_args(raw_args)
    if not args.test_name or args.test_name == 'list':
        print(Color.bold(">>> Available tests:"))
        for test_name in tests_map.keys():
            print(Color.green(f"  {test_name}"))
        return

    test_class = tests_map[args.test_name]
    test_class.run_from_args(args)
