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

from .test_apof import TestApoF


class TestAretomo3ApoF(TestApoF):
    workflow_template = TestApoF.get_workflow_template('apof-aretomo3')

    job_types = [
        'emw-import-ts',
        'emw-aretomo3',
    ]

    expected_outputs = {
        'emw-import-ts': 'tilt_series.star',
        'emw-aretomo3': ['aligned_tilt_series.star', 'tomograms.star'],
    }
    

if __name__ == '__main__':
    TestAretomo3ApoF.run_tests()
