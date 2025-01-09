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

from emtools.metadata import Table, StarFile


class RelionStar:

    @staticmethod
    def optics_table(acq, opticsGroup=1, opticsGroupName="opticsGroup1",
                     mtf=None, originalPixelSize=None):
        origPs = originalPixelSize or acq['pixel_size']

        values = {'rlnOpticsGroupName': opticsGroupName,
                'rlnOpticsGroup': opticsGroup,
                'rlnMicrographOriginalPixelSize': origPs,
                'rlnVoltage': acq['voltage'],
                'rlnSphericalAberration': acq['cs'],
                'rlnAmplitudeContrast': acq.get('amplitude_constrast', 0.1),
                'rlnMicrographPixelSize': acq['pixel_size']
        }
        if mtf:
            values['rlnMtfFileName'] = mtf
        return Table.fromDict(values)

    @staticmethod
    def micrograph_table(**kwargs):
        extra_cols = kwargs.get('extra_cols', [])
        return Table([
            'rlnMicrographName',
            'rlnOpticsGroup',
            'rlnCtfImage',
            'rlnDefocusU',
            'rlnDefocusV',
            'rlnCtfAstigmatism',
            'rlnDefocusAngle',
            'rlnCtfFigureOfMerit',
            'rlnCtfMaxResolution'
        ] + extra_cols)

    @staticmethod
    def coordinates_table(**kwargs):
        return Table(['rlnMicrographName', 'rlnMicrographCoordinates'])
