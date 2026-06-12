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


def getTomoPixelSize(row):
    """ Compute the tomogram pixel size in the row by multiplying the TS pixel size by the binning.
    """
    return float(row.rlnTomoTiltSeriesPixelSize) * float(row.rlnTomoTomogramBinning)


def getTomogram(row):
    """ Return tomogram path, tryng from different columns.
    """
    cols = ['rlnTomoReconstructedTomogram', 'rlnTomoReconstructedTomogramDenoised']
    for col in cols:
        if value := row.get(col):
            return value
    raise ValueError(f"No tomogram column ({', '.join(cols)}) found in row: {row}")
