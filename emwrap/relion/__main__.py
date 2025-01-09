import argparse
from glob import glob

from .cryolo import CryoloPredict


if __name__ == '__main__':
    p = argparse.ArgumentParser(prog='emh-cryolo')
    p.add_argument('micrograph')
    p.add_argument('pixel_size')
    p.add_argument('voltage')
    p.add_argument('spherical_aberration')
    p.add_argument('amplitude_contrast')

    args = p.parse_args()

    ctffind = CryoloPredict()

    if '*' in args.micrograph:
        mics = glob(args.micrograph)
    else:
        mics = [args.micrograph]

    for mic in mics:
        ctffind.process(mic, verbose=True)

