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


def open_json(file):
    if os.path.exists(file):
        with open(file) as json_file:
            return json.load(json_file)
    else:
        return None


LIB_LIST_MAX = [
    "tendl.2021",
    "endfb8.0",
    "jeff3.3",
    "jendl5.0",
    "iaea.2019",
    "cendl3.2",
    "irdff2.0",
    "iaea.pd",
]
LIB_LIST_MAX.sort(reverse=True)




def generate_exfortables_file_path(input_store):
    type = input_store.get("type").upper()
    elem = input_store.get("target_elem")
    mass = input_store.get("target_mass")
    branch = input_store.get("branch")

    reaction = input_store.get("reaction")
    level_num = input_store.get("level_num")

    target = f"{elem.capitalize()}-{str(int(mass))}"
    exfiles = []

    if level_num:
        reaction = convert_partial_reactionstr_to_inl(reaction)
        dir = os.path.join(
            EXFORTABLES_PY_GIT_REPO_PATH,
            reaction.split(",")[0].lower(),
            target,
            reaction.replace(",", "-").lower() + "-L" + str(level_num),
            type.lower(),
        )

    elif type == "FY":
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
            type.lower() if type != "RP" else "xs",
        )

    if type == "RP":
        ## Format is "Ag-109-M"
        rp_elem = input_store.get("rp_elem")
        rp_mass = input_store.get("rp_mass")
        residual = f"{rp_elem.capitalize()}-{str(rp_mass.lstrip('0'))}"

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
    type = input_store.get("type").upper()
    elem = input_store.get("target_elem")
    mass = input_store.get("target_mass")
    reaction = input_store.get("reaction")
    mt = input_store.get("mt")

    target = f"{elem.capitalize()}{str(int(mass)).zfill(3)}"

    libfiles = []
    for lib in LIB_LIST_MAX:
        if type == "FY":
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
                type.lower() if type != "RP" else "residual",
            )

        if type == "RP":
            rp_elem = input_store.get("rp_elem")
            rp_mass = input_store.get("rp_mass")
            residual = f"rp{ elemtoz(rp_elem.capitalize()).zfill(3)}{str(int(rp_mass)).zfill(3)}.{lib}"
            if os.path.exists(dir):
                libfiles += [f for f in os.listdir(dir) if residual in f]

        else:
            if os.path.exists(dir):
                libfiles += [f for f in os.listdir(dir) if f"MT{mt.zfill(3)}" in f]

    return dir, libfiles





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




