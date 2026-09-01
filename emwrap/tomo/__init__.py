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

# This package is meant to be run as 'python -m emwrap.tomo' (see the
# generated './emh-tomo' entry-point script in install.sh). It intentionally
# does not import from '__main__' here, to avoid the module being imported
# twice (once as 'emwrap.tomo.__main__', once as '__main__') when run with
# 'python -m'.
