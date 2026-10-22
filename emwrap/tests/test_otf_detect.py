"""Detection of the OTF type (SPA vs tomography) from the session data,
on small fake data folders (empty files and mdocs).

    python -m pytest emwrap/tests/test_otf_detect.py
"""
import os
import tempfile
import threading
import time
import unittest

from emwrap.mix.otf import OTF

EPU_PATTERN = 'Images-Disc*/GridSquare_*/Data/*fractions.tiff'


def _touch(path, text=''):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w') as f:
        f.write(text)


def _mdoc(angles, ts=None):
    text = ("PixelSpacing = 1.5\nImageSize = 4096 4096\n"
            "[T = Tilt axis angle = 85.0, binning = 1]\n")
    for i, a in enumerate(angles):
        text += f"\n[ZValue = {i}]\nTiltAngle = {a}\nExposureDose = 3\n"
        if ts:
            text += f"SubFramePath = X:\\frames\\{ts}_{i + 1:03d}_{a}.eer\n"
    return text


def _ctx(path, **conf):
    return {'raw': {'path': path}, 'wf_conf': conf, 'conf_acq': {}}


class TestOtfDetect(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()

    def _detect(self, path, **conf):
        """ Type detected without waiting, and the final one. """
        info = OTF.scanData(_ctx(path, **conf))
        return OTF.decideType(info)[0], OTF.decideType(info, final=True)[0]

    def test_tomo_mdocs(self):
        d = os.path.join(self.root, 'tomo')
        angles = [0.0, 3.0, -3.0, 6.0, -6.0]
        _touch(f'{d}/TS_01.mdoc', _mdoc(angles, 'TS_01'))
        for i, a in enumerate(angles):
            _touch(f'{d}/Frames/TS_01_{i + 1:03d}_{a}.eer')
        self.assertEqual(self._detect(d), ('tomo', 'tomo'))

    def test_tomo_movies_only(self):
        d = os.path.join(self.root, 'tomo_movies')
        for i, a in enumerate([0.0, 3.0, -3.0, 6.0]):
            _touch(f'{d}/Frames/Position_3_{i + 1:03d}[{a:.2f}]_EER.eer')
        self.assertEqual(self._detect(d), ('tomo', 'tomo'))

    def test_tomo_first_tilt(self):
        """ Not conclusive with a single tilt, tomo when final. """
        d = os.path.join(self.root, 'tomo_first')
        _touch(f'{d}/mdoc/Position_1.mdoc', _mdoc([0.0], 'Position_1'))
        self.assertEqual(self._detect(d), (None, 'tomo'))

    def test_spa_epu(self):
        d = os.path.join(self.root, 'spa_epu')
        for i in range(5):
            _touch(f'{d}/Images-Disc1/GridSquare_1/Data/'
                   f'FoilHole_1234567_Data_7654321_{i:03d}_20240101_120000_fractions.tiff')
        self.assertEqual(self._detect(d, images_pattern=EPU_PATTERN), ('spa', 'spa'))

    def test_spa_serialem_movie_mdocs(self):
        """ One mdoc per movie (SerialEM SPA) is not tomography. """
        d = os.path.join(self.root, 'spa_serialem')
        for i in range(6):
            _touch(f'{d}/grid1_{i:05d}.tif')
            _touch(f'{d}/grid1_{i:05d}.tif.mdoc', "[FrameSet = 0]\nExposureDose = 50\n")
        self.assertEqual(self._detect(d), ('spa', 'spa'))

    def test_empty(self):
        d = os.path.join(self.root, 'empty')
        os.makedirs(d)
        self.assertEqual(self._detect(d), (None, None))
        with self.assertRaises(Exception):
            OTF.detectType(_ctx(d), log=lambda m: None, timeout=0.3, sleep=0.1)

    def test_wait_for_tilts(self):
        """ detectType waits until the tilt series has enough tilts. """
        d = os.path.join(self.root, 'tomo_wait')
        fn = f'{d}/Position_1.mdoc'
        _touch(fn, _mdoc([0.0]))

        def _acquire():
            time.sleep(0.5)
            _touch(fn, _mdoc([0.0, 3.0, -3.0]))

        threading.Thread(target=_acquire).start()
        self.assertEqual(OTF.detectType(_ctx(d), log=lambda m: None,
                                        timeout=10, sleep=0.2), 'tomo')


if __name__ == '__main__':
    unittest.main()
