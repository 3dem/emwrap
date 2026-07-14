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

from .test_apof import TestApoF


class TestApoFWarpOtf(TestApoF):
    emwrap_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    workflow_template = os.path.join(
        emwrap_root, 'config', 'workflows', 'apof-warp-tutorial-otf.json.template')

    job_types = [
        'emw-import-ts',
        'emw-warp-mctf'
    ]

    expected_outputs = {
        'emw-import-ts': 'tilt_series.star',
        'emw-warp-mctf': 'tilt_series.star',
    }


if __name__ == '__main__':
    TestApoFWarpOtf.run_tests()
