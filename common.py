####################################################################
#
# This file is part of libraries-2021 dataexplorer, https://nds.iaea.org/dataexplorer/.
# Copyright (C) 2022 International Atomic Energy Agency (IAEA)
#
# Contact:    nds.contact-point@iaea.org
#
# Change logs:
#    First release: 2021-08-20
#    Update libraries: 2022-09-05, JENDL4.0 and TENDL2019 have been replced by JENDL5.0 and TENDL2021
#
####################################################################


import os
import json

from config import DATA_DIR, EXFORTABLES_PY_GIT_REPO_PATH, ENDFTABLES_PATH
from submodules.utilities.elem import elemtoz
from submodules.utilities.reaction import convert_partial_reactionstr_to_inl
from submodules.utilities.util import get_str_from_string, get_number_from_string


def open_json(file):
    if os.path.exists(file):
        with open(file) as json_file:
            return json.load(json_file)
    else:
        return None


LIB_LIST_MAX = [
    "tendl.2023",
    # "tendl.2021",
    "endfb8.1",
    "eaf.2010",  # European Activation File
    "jeff4.0",
    "jendl5.0",
    # "jendl4.0",
    "iaea.2022",
    # "iaea.2019",
    "cendl3.2",
    "irdff2.0",
    "iaea.pd",
    "ibandl",
]
LIB_LIST_MAX.sort(reverse=True)


pageparam_to_sf6 = {
    "XS": "SIG", 
    "TH": "SIG", 
    "RP": "SIG", 
    "FY": "FY", 
    "DA": "DA",
    "DE": "DE",
    "TRN": "TRN",
}


pageparam_to_endftables_obs_type = {
    "XS": "xs", 
    "TH": "xs", 
    "RP": "residual", 
    "FY": "fission", 
    "DA": "angle",
    "DE": "energy",
    "TRN": None,
}


def generate_exfortables_file_path(input_store):
    obs_type = input_store.get("obs_type").upper()
    elem = input_store.get("target_elem")
    mass = input_store.get("target_mass")
    branch = input_store.get("branch")

    reaction = input_store.get("reaction")
    level_num = input_store.get("level_num")

    target = f"{elem.capitalize()}-{str(mass)}"
    exfiles = []

    if obs_type == "TH":
        obs_type = "XS"

    if level_num:
        reaction = convert_partial_reactionstr_to_inl(reaction)
        dir = os.path.join(
            EXFORTABLES_PY_GIT_REPO_PATH,
            reaction.split(",")[0].lower(),
            target,
            reaction.replace(",", "-").lower() + "-L" + str(level_num),
            obs_type.lower(),
        )

    elif obs_type == "FY":
        fy_type = input_store.get("fy_type")
        dir = os.path.join(
            EXFORTABLES_PY_GIT_REPO_PATH,
            reaction.split(",")[0].lower(),
            target,
            reaction.replace(",", "-").lower(),
            "fission/yield",
            fy_type.lower(),
        )

    else:
        dir = os.path.join(
            EXFORTABLES_PY_GIT_REPO_PATH,
            reaction.split(",")[0].lower(),
            target,
            reaction.replace(",", "-").lower(),
            obs_type.lower() if obs_type != "RP" else "xs",
        )

    if obs_type == "RP":
        ## Format is "Ag-109-M"
        rp_elem = input_store.get("rp_elem")
        rp_mass = input_store.get("rp_mass")
        residual = f"{rp_elem.capitalize()}-{str(rp_mass.lstrip('0'))}"

        # if not get_str_from_string(rp_mass):
        #     residual = f"{rp_elem.capitalize()}-{str(rp_mass.lstrip('0'))}"

        # else:
        #     residual = f"{rp_elem.capitalize()}-{str(rp_mass.lstrip('0'))}-{get_str_from_string(rp_mass).upper()}"

        if not get_str_from_string(rp_mass):
            residual = f"rp{ elemtoz(rp_elem.capitalize()).zfill(3)}{str(int(rp_mass)).zfill(3)}"

        else:
            residual = f"rp{ elemtoz(rp_elem.capitalize()).zfill(3)}{str(int(get_number_from_string(rp_mass))).zfill(3)}{get_str_from_string(rp_mass)}"

        if os.path.exists(dir):
            exfiles = [f for f in os.listdir(dir) if residual in f]

    else:
        if os.path.exists(dir):
            exfiles = os.listdir(dir)

    return dir, exfiles


def generate_endftables_file_path(input_store):
    """
    Generate the direct file links
    """
    obs_type = input_store.get("obs_type").upper()
    elem = input_store.get("target_elem")
    mass = input_store.get("target_mass")
    reaction = input_store.get("reaction")
    mt = input_store.get("mt")

    target = f"{elem.capitalize()}{str(mass).zfill(3)}"

    if obs_type == "TH":
        obs_type = "XS"

    libfiles = []
    for lib in LIB_LIST_MAX:
        if obs_type == "FY":
            dir = os.path.join(
                ENDFTABLES_PATH,
                "FY",
                reaction.split(",")[0].lower(),
                target,
                lib,
                "tables/FY",
            )
        else:
            dir = os.path.join(
                ENDFTABLES_PATH,
                reaction.split(",")[0].lower(),
                target,
                lib,
                "tables",
                obs_type.lower() if obs_type != "RP" else "residual",
            )

        if obs_type == "RP":
            rp_elem = input_store.get("rp_elem")
            rp_mass = input_store.get("rp_mass")

            if not get_str_from_string(rp_mass):
                residual = f"rp{ elemtoz(rp_elem.capitalize()).zfill(3)}{str(int(rp_mass)).zfill(3)}.{lib}"

            else:
                residual = f"rp{ elemtoz(rp_elem.capitalize()).zfill(3)}{str(int(get_number_from_string(rp_mass))).zfill(3)}{get_str_from_string(rp_mass)}.{lib}"

            if os.path.exists(dir):
                libfiles += [f for f in os.listdir(dir) if residual in f]

        else:
            if os.path.exists(dir):
                libfiles += [f for f in os.listdir(dir) if f"MT{mt.zfill(3)}" in f]

    return dir, libfiles


def generate_single_endftables_file_path(input_store):
    """
    Generate the direct file links
    """
    obs_type = input_store.get("obs_type").upper()
    projectile = input_store.get("projectile")
    lib = input_store.get("evaluation")
    mt = input_store.get("mt")
    target = input_store.get("target")
    libfiles = []
    if obs_type == "FY":
        dir = os.path.join(
            ENDFTABLES_PATH,
            "FY",
            projectile.lower(),
            target,
            lib,
            "tables/FY",
        )
    else:
        dir = os.path.join(
            ENDFTABLES_PATH,
            projectile.lower(),
            target,
            lib,
            "tables",
            obs_type.lower(),
        )

    if os.path.exists(dir):
        libfiles += [f for f in os.listdir(dir) if f"MT{str(mt).zfill(3)}" in f]

    if libfiles:
        return dir, libfiles[0]


def generate_link_of_files(dir, files):
    ## similar to list_link_of_files in dataexplorer/common.py
    flinks = []

    for f in sorted(files):
        filename = os.path.basename(f)
        dirname = os.path.dirname(f)
        linkdir = dirname.replace(DATA_DIR, "")

        fullpath = os.path.join(dir, filename)

        flinks.append(linkdir)
        # flinks.append(html.Br())

    return flinks
