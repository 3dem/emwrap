# -----------------------------------------------------------------------------
# AreTomo3 - RELION tilt-geometry convention
# -----------------------------------------------------------------------------
#
# This pipeline converts AreTomo3 outputs into RELION-style tomo STAR metadata.
# The input STAR files already contain nominal microscope geometry, including:
#
#   rlnTomoNominalStageTiltAngle
#   rlnTomoNominalTiltAxisAngle
#
# However, when AreTomo3 is run with TiltCor enabled, the tilt geometry used by
# AreTomo3 may differ from the original import metadata. In particular:
#
#   - TiltCor / AlphaOffset can modify the effective per-image tilt angles.
#   - TiltAxis refinement can modify the tilt-axis angle used for alignment.
#
# For the AreTomo3-derived RELION output, the RELION-facing geometry should
# match the geometry actually used by AreTomo3, not necessarily the original
# microscope/import metadata.
#
# Therefore, for each output individual tilt-series STAR file:
#
#   rlnTomoNominalStageTiltAngle
#       is overwritten with the AreTomo3 corrected per-image tilt angle.
#       Prefer the corrected angle from the IMOD/AreTomo3 .tlt file; fall back
#       to the .aln TILT value or the original STAR value if needed.
#
#   rlnTomoNominalTiltAxisAngle
#       is overwritten with the refined AreTomo3 tilt-axis angle derived from
#       the .aln ROT value. This is a tilt-series-level value and should remain
#       constant across all tilt images in the series.
#
#   rlnTomoYTilt
#       is set to the corrected AreTomo3 per-image tilt angle, matching
#       rlnTomoNominalStageTiltAngle.
#
#   rlnTomoZRot
#       is set from the per-image IMOD .xf transform. This is an in-plane
#       image transform and is not the same as rlnTomoNominalTiltAxisAngle.
#
# To keep provenance and make debugging easier, the original import values are
# preserved in custom AreTomo3 labels:
#
#   at3OriginalNominalStageTiltAngle
#   at3OriginalNominalTiltAxisAngle
#
# and the AreTomo3-derived values are also stored explicitly as:
#
#   at3CorrectedTiltAngle
#   at3RefinedTiltAxisAngle
#
# This avoids mixing coordinate systems during RELION reconstruction or
# subtomogram averaging: particles picked from AreTomo3/TiltCor tomograms should
# be processed with the same corrected tilt geometry used to generate those
# tomograms.
# -----------------------------------------------------------------------------