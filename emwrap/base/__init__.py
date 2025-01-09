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


class Acquisition(dict):
    """ Subclass from dict with some utilities related to Acquisition. """

    @property
    def pixel_size(self):
        return self['pixel_size']

    @pixel_size.setter
    def pixel_size(self, value):
        self['pixel_size'] = value

    @property
    def voltage(self):
        return self['voltage']

    @voltage.setter
    def voltage(self, value):
        self['voltage'] = value

    @property
    def cs(self):
        return self['cs']

    @cs.setter
    def cs(self, value):
        self['cs'] = value

    @property
    def amplitude_contrast(self):
        return self.get('amplitude_contrast', 0.1)

    @amplitude_contrast.setter
    def amplitude_contrast(self, value):
        self['amplitude_contrast'] = value
