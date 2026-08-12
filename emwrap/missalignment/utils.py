import math
import xml.etree.ElementTree as ET
from pathlib import Path


def _read_float_array(root, tag):
    """Read a whitespace/comma/semicolon separated numeric Warp XML field."""
    element = root.find(tag)
    if element is None:
        raise ValueError(f"Missing <{tag}> in Warp XML.")

    text = "".join(element.itertext()).strip()
    if not text:
        raise ValueError(f"Empty <{tag}> in Warp XML.")

    # Warp serializes these arrays as text. Accept the common separators.
    text = text.replace(",", " ").replace(";", " ")
    try:
        return [float(value) for value in text.split()]
    except ValueError as exc:
        raise ValueError(
            f"Could not parse numeric values from <{tag}>: {text[:200]!r}"
        ) from exc


def _broadcast(values, n, name):
    """Allow a single constant value or one value per tilt."""
    if len(values) == n:
        return values
    if len(values) == 1:
        return values * n
    raise ValueError(
        f"{name} contains {len(values)} values, expected 1 or {n}."
    )


def warp_xml_to_imod_xf(
    xml_file,
    xf_file,
    xf_pixel_size,
):
    """Convert Warp global tilt-series alignment parameters to IMOD XF.

    Only the global per-tilt alignment stored in ``AxisAngle``,
    ``AxisOffsetX``, and ``AxisOffsetY`` is exported. Warp local/grid
    alignment terms are intentionally ignored because an IMOD ``.xf`` file
    stores only one global affine 2D transform per tilt image.

    Parameters
    ----------
    xml_file : str or pathlib.Path
        Warp TiltSeries XML file.
    xf_file : str or pathlib.Path
        Destination IMOD .xf file.
    xf_pixel_size : float
        Pixel size, in Angstrom/pixel, of the image stack to which the
        generated XF transforms will be applied. This is NOT the CTF
        PixelSize field in the Warp XML.

    Notes
    -----
    If ``alpha`` is Warp's ``AxisAngle``, the corresponding IMOD rotation is

        theta = -alpha

        R = [[ cos(theta), -sin(theta)],
             [ sin(theta),  cos(theta)]]

    Warp's axis offsets are in physical units. If

        o = [AxisOffsetX, AxisOffsetY]^T

    and ``p`` is the XF stack pixel size in Angstrom/pixel, then the IMOD
    translation in pixels is

        t = -(R @ o) / p

    Each output row is written as

        A11 A12 A21 A22 DX DY
    """
    xml_file = Path(xml_file).expanduser().resolve()
    xf_file = Path(xf_file).expanduser().resolve()

    xf_pixel_size = float(xf_pixel_size)
    if xf_pixel_size <= 0:
        raise ValueError(
            f"xf_pixel_size must be greater than zero, got {xf_pixel_size}."
        )

    if not xml_file.is_file():
        raise FileNotFoundError(f"Warp XML not found: {xml_file}")

    root = ET.parse(xml_file).getroot()
    if root.tag != "TiltSeries":
        raise ValueError(
            f"Expected <TiltSeries> root in {xml_file}, found <{root.tag}>."
        )

    offsets_x = _read_float_array(root, "AxisOffsetX")
    offsets_y = _read_float_array(root, "AxisOffsetY")

    if len(offsets_x) != len(offsets_y):
        raise ValueError(
            "AxisOffsetX/AxisOffsetY length mismatch: "
            f"{len(offsets_x)} vs {len(offsets_y)}."
        )

    n_tilts = len(offsets_x)
    axis_angles = _broadcast(
        _read_float_array(root, "AxisAngle"),
        n_tilts,
        "AxisAngle",
    )

    xf_file.parent.mkdir(parents=True, exist_ok=True)

    with xf_file.open("w", encoding="utf-8") as handle:
        for axis_angle, offset_x, offset_y in zip(
            axis_angles,
            offsets_x,
            offsets_y,
        ):
            theta = math.radians(-axis_angle)
            cos_theta = math.cos(theta)
            sin_theta = math.sin(theta)

            a11 = cos_theta
            a12 = -sin_theta
            a21 = sin_theta
            a22 = cos_theta

            dx = -(
                a11 * offset_x
                + a12 * offset_y
            ) / xf_pixel_size
            dy = -(
                a21 * offset_x
                + a22 * offset_y
            ) / xf_pixel_size

            handle.write(
                f"{a11:10.6f} {a12:10.6f} "
                f"{a21:10.6f} {a22:10.6f} "
                f"{dx:10.3f} {dy:10.3f}\n"
            )

    return xf_file