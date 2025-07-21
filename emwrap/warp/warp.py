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


def get_warptools():
    varPath = 'WARPTOOLS_PATH'

    if program := os.environ.get(varPath, None):
        if not os.path.exists(program):
            raise Exception(f"PyTom path ({varPath}={program}) does not exists.")
    else:
        raise Exception(f"PyTom path variable {varPath} is not defined.")

    return program