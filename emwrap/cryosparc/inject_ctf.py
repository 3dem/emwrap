# **************************************************************************
# *
# * Authors:     Yunior C. Fonseca Reyna (cfonsecareyna82@gmail.com)
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

import argparse
import json
import math

from cryosparc.tools import CryoSPARC
from emtools.metadata.starfile import StarFile


def loadCredentials(path):
    """
    Loads CryoSPARC credentials from a JSON file.
    If the file is missing or invalid, it falls back to hardcoded defaults.
    Parameters:
        path (str): Path to the credentials JSON file.
    Returns:
        dict: Dictionary with CryoSPARC credentials.
    """
    try:
        with open(path, 'r') as f:
            credentials = json.load(f)
            print("✅ Credentials loaded from file.")
            return credentials
    except Exception as e:
        print(f"⚠️ Failed to load credentials from file ({e}). Using fallback credentials.")
        # Hardcoded fallback credentials (for development/testing)
        return {
            'license': 'd070878c-a032-11ef-aff5-1b53738ebc36',
            'host': 'localhost',
            'base_port': 39000,
            'email': 'cfonseca@cnb.csic.es',
            'password': '12345',
            'timeout': 300
        }


def injectCtfFromStar(output, starPath):
    """
        Injects CTF parameters into a CryoSPARC job output dictionary
        by parsing values from a STAR file containing micrograph CTF estimations.

        Parameters:
            output (cs DataSet): CryoSPARC job output dictionary to populate.
            starPath (str): Path to the STAR file with CTF estimation data.
        """
    md = StarFile(starPath)

    # Extract CTF constants from the optics group (assumes a single optics group)
    optics = next(md.iterTable('optics'))
    kvValue = optics.get('rlnVoltage')  # Accelerating voltage in kV
    acValue = optics.get('rlnAmplitudeContrast')  # Amplitude contrast
    csValue = optics.get('rlnSphericalAberration')  # Spherical aberration in mm

    # Get all micrograph rows from the STAR file
    micrographs = list(md.iterTable('micrographs'))

    # Populate output with per-micrograph and constant CTF values
    output["ctf/accel_kv"] = [kvValue] * len(micrographs)
    output["ctf/df1_A"] = [row.get('rlnDefocusU') for row in micrographs]
    output["ctf/df2_A"] = [row.get('rlnDefocusV') for row in micrographs]
    output["ctf/df_angle_rad"] = [math.radians(row.get('rlnDefocusAngle')) for row in micrographs]
    output["ctf/ctf_fit_to_A"] = [row.get('rlnCtfMaxResolution') for row in micrographs]
    output["ctf/fig_of_merit_gctf"] = [row.get('rlnCtfFigureOfMerit') for row in micrographs]
    output["ctf/amp_contrast"] = [acValue] * len(micrographs)
    output["ctf/cs_mm"] = [csValue] * len(micrographs)


def main(credentialsPath, micrographPath, ctfStarPath, projectId, workspaceId):
    """
    Main function to connect to CryoSPARC, load imported micrographs,
    inject CTF metadata from a .star file, and save the results in an external job.

    Parameters:
        credentialsPath (str): Path to CryoSPARC credentials JSON file.
        micrographPath (str): Path to micrographs.
        ctfStarPath (str): Path to the .star file containing CTF data.
        projectId (str): Project Id where the external job will be created
        workspaceId (str): Workspace Id where the external job will be created
    """
    # Load CryoSPARC credentials
    credentials = loadCredentials(credentialsPath)

    # Connect to CryoSPARC instance
    try:
        cs = CryoSPARC(
            license=credentials['license'],
            host=credentials['host'],
            base_port=int(credentials['base_port']),
            email=credentials['email'],
            password=credentials['password'],
            timeout=credentials['timeout']
        )
        if cs.test_connection():
            print("✅ Connection successful.")
    except Exception as e:
        raise Exception(f"⚠️ Error connecting to CryoSPARC: {e}")

    # Find the project
    project = cs.find_project(projectId)
    print("✅ Project found.")

    # Load micrographs from an existing import job (is only for testing proposes)
    # importJobId = 'J163'  # Can be parameterized if needed
    # importJob = cs.find_job(projectId, importJobId)
    # micrographs = importJob.load_output('imported_micrographs')

    # Load micrographs from an existing path
    importMicJob = cs.create_job(
        project_uid=projectId,
        workspace_uid=workspaceId,
        type='import_micrographs',
        title='import micrograph',
        params={
        'blob_paths': micrographPath,
        'psize_A': 1.0,
        'accel_kv': 300.0,
        'cs_mm': 2.7,
        'total_dose_e_per_A2': 0.1,
        'output_constant_ctf': True})

    importMicJob.queue()
    print(f"✅ Queued job: {importMicJob.uid}")
    importMicJob.wait_for_done()
    print(f"✅ Job completed.")
    micrographs = importMicJob.load_output('imported_micrographs')
    print("✅ Micrographs loaded.")

    # Extract relevant micrograph-related fields
    print("✅ Extracting relevant micrograph-related fields...")
    micDict = {
        key: value.__array__()
        for key, value in micrographs.items()
        if key.startswith(('micrograph', 'mscope', 'uid'))
    }

    # Create a new external job to inject CTF data
    externalJob = project.create_external_job(workspaceId, title="Batch CTF Injection")
    print(f"✅ External job created: {externalJob.uid}")

    # Define output slots for the new job
    slotSpec = [
        {"dtype": "ctf", "prefix": "ctf", "required": True},
        {"dtype": "ctf_stats", "prefix": "ctf_stats", "required": True},
        {"dtype": "micrograph_blob", "prefix": "micrograph_blob", "required": True},
        {"dtype": "micrograph_blob", "prefix": "micrograph_blob_non_dw", "required": True},
        {"dtype": "mscope_params", "prefix": "mscope_params", "required": True}
    ]

    output = externalJob.add_output(
        type="exposure",
        name="exposures",
        slots=slotSpec,
        title="Micrographs processed",
        alloc=len(micrographs)
    )
    print("✅ Output slots created.")

    # Copy micrograph data into the output
    for key, value in micDict.items():
        output[key] = value
    print("✅ Micrograph parameters updated.")

    # Inject CTF parameters parsed from the STAR file
    injectCtfFromStar(output, ctfStarPath)
    print("✅ CTF data injected from STAR file.")

    # Save results to the CryoSPARC external job
    with externalJob.run():
        externalJob.save_output("exposures", output)
        print("✅ External job output saved successfully.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Inject CTF info into a CryoSPARC external job.")
    parser.add_argument('--credentials', type=str, required=True, help='Path to CryoSPARC credentials JSON')
    parser.add_argument('--micrographs', type=str, required=True, help='Micrographs path or pattern (use quotes for patterns)')
    parser.add_argument('--ctf', type=str, required=True, help='Path to CTF .star file')
    parser.add_argument('--projectId', type=str, required=True, help='cryoSPARC project id where the new job will be created')
    parser.add_argument('--workspaceId', type=str, required=True, help='cryoSPARC workspace id where the new job will be created')

    args = parser.parse_args()
    main(args.credentials, args.micrographs, args.ctf, args.projectId, args.workspaceId)


