import argparse
from glob import glob

from .ctffind import Ctffind

"""
            **   Welcome to Ctffind   **

            Version : 5.0.0
           Compiled : Feb 26 2024
    Library Version : 2.0.0-alpha-151-d42312b-dirty
        From Branch : ctffind5
               Mode : Interactive

Input image file name [input.mrc]                  : aligned_20170629_00023_frameImage.mrc
Output diagnostic image file name
[diagnostic_output.mrc]                            : aligned_20170629_00023_frameImage_ctf.mrc
Pixel size [1.0]                                   : 0.6485
Acceleration voltage [300.0]                       : 200
Spherical aberration [2.70]                        : 2.7
Amplitude contrast [0.07]                          : 0.1
Size of amplitude spectrum to compute [512]        : 512
Minimum resolution [30.0]                          :
Maximum resolution [5.0]                           :
Minimum defocus [5000.0]                           :
Maximum defocus [50000.0]                          :
Defocus search step [100.0]                        :
Do you know what astigmatism is present? [No]      :
Slower, more exhaustive search? [No]               :
Use a restraint on astigmatism? [No]               :
Find additional phase shift? [No]                  :
Determine sample tilt? [No]                        :
Determine sample thickness? [No]                  :
Do you want to set expert options? [No]            :
"""

if __name__ == '__main__':
    p = argparse.ArgumentParser(prog='emh-ctffind')
    p.add_argument('micrograph')
    p.add_argument('pixel_size')
    p.add_argument('voltage')
    p.add_argument('spherical_aberration')
    p.add_argument('amplitude_contrast')

    args = p.parse_args()

    ctffind = Ctffind(args.pixel_size, args.voltage,
                      args.spherical_aberration,
                      args.amplitude_contrast)

    if '*' in args.micrograph:
        mics = glob(args.micrograph)
    else:
        mics = [args.micrograph]

    for mic in mics:
        ctffind.process(mic, verbose=True)

