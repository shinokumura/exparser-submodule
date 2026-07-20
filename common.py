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
from pathlib import Path

from config import (
    DATA_DIR,
    ENDFTABLES_PATH,
    EXFORTABLES_PY_GIT_REPO_PATH,
)
from submodules.utilities.elem import elemtoz
from submodules.utilities.reaction import convert_partial_reactionstr_to_inl
from submodules.utilities.util import get_str_from_string, get_number_from_string


def open_json(file):
    if os.path.exists(file):
        with open(file) as json_file:
            return json.load(json_file)
    else:
        return None



LIB_LIST_MAX = {
    "tendl.2023": "TENDL-2023-update",
    "tendl.2025": "TENDL-2025",
    # "tendl.2021",
    "endfb8.1": "ENDF/B-VIII.1",
    "eaf.2010" : "EAF-2010",  # European Activation File
    "fendl3.2" : "FENDL-3.2c", 
    "jeff4.0": "JEFF-4.0",
    "jendl5.0": "JENDL-5.0",
    # "jendl4.0",
    "iaea.2022": "IAEA-2022",
    # "iaea.2019",
    "cendl3.2": "CENDL-3.2",
    "irdff2.0": "IRDFF-2.0",
    "iaea.pd": "IAEA Photonuclear 2019",
    "ibandl": "IBANDL",
}


pageparam_to_sf6 = {
    "XS": "SIG",
    "TH": "SIG",
    "RP": "SIG",
    "FY": "FY",
    "DA": "DA",
    "DE": "DE",
    "TRN": "TRN",
    "DDX": "DA/DE",
}


pageparam_to_endftables_obs_type = {
    "XS": "xs",
    "TH": "xs",
    "RP": "residual",
    "FY": "fission",
    "DA": "angle",
    "DE": "energy",
    "TRN": None,
    "DDX": None,
}


LIGHT_ION_PROJECTILES = {
    "P": "p",
    "D": "d",
    "T": "t",
    "A": "a",
    "HE3": "He-3",
}


_EXFOR_SCALAR_GLOBS = {
    "TH": ("thermal/*.txt",),
    "RI": ("resonance_integral/*.txt",),
    "MACS": ("macs/*.txt",),
    "D0": ("resonance_spacing/**/*.txt",),
    "D1": ("resonance_spacing/**/*.txt", "resonance_parameter/**/*.txt"),
    "D2": ("resonance_spacing/**/*.txt", "resonance_parameter/**/*.txt"),
    "S0": ("strength_function/*.txt", "resonance_parameter/**/S0/*.txt"),
    "S1": ("strength_function/*.txt", "resonance_parameter/**/S1/*.txt"),
    "GG0": ("gamma_gamma/*.txt", "resonance_parameter/**/AV/*.txt"),
    "GG1": ("gamma_gamma/*.txt", "resonance_parameter/**/AV/*.txt"),
    "RAD": ("resonance_parameter/ARE/**/*.txt",),
    "STF": ("strength_function/*.txt",),
}

_THERMAL_REACTION_DIR = {
    "el": "el",
    "a": "na",
    "f": "nf",
    "g": "ng",
    "p": "np",
    "tot": "tot",
}

_RESONANCE_QUANTITY_DIR = {
    "D0": "D0",
    "D1": "D1",
    "D2": "D2",
    "S0": "S0",
    "S1": "S1",
    "GG0": "gamgam0",
    "GG1": "gamgam1",
    "RAD": "R",
    "STF": "S0",
}


def _glob_text_files(root, patterns):
    root = Path(root)
    if not root.is_dir():
        return []
    return sorted(
        {str(path) for pattern in patterns for path in root.glob(pattern) if path.is_file()}
    )


def _resonancetables_file_path(input_store):
    obs_type = (input_store.get("obs_type") or "").upper()
    reaction = (input_store.get("reaction") or "").split(",", 1)
    if len(reaction) != 2:
        return []

    process = reaction[1].lower()
    if obs_type == "TH":
        top_dir = "thermal"
        quantity = _THERMAL_REACTION_DIR.get(process)
    elif obs_type == "MACS" and process == "g":
        top_dir, quantity = "macs", "ng"
    elif obs_type == "RI":
        top_dir, quantity = "resonance", {"g": "Ig", "f": "If"}.get(process)
    else:
        top_dir, quantity = "resonance", _RESONANCE_QUANTITY_DIR.get(obs_type)

    if not quantity:
        return []

    elem = str(input_store.get("target_elem") or "").capitalize()
    mass = str(input_store.get("target_mass") or "")
    target = f"{elem}{mass}"
    directory = Path(DATA_DIR, "resonancetables", top_dir, quantity, "nuc")
    filename_quantity = "macs" if obs_type == "MACS" else quantity
    return _glob_text_files(directory, (f"{target}_{filename_quantity}.txt",))


def nuclide_reformat(code):
    parts = str(code).split("-")
    if len(parts) >= 3 and parts[0].isdigit():
        nuclide = parts[1].capitalize() + "-" + parts[2]
        if len(parts) > 3:
            nuclide += "-" + parts[3].lower()
        return nuclide
    return str(code)


def projectile_reformat(projectile):
    projectile = str(projectile).upper()
    if projectile in LIGHT_ION_PROJECTILES:
        return LIGHT_ION_PROJECTILES[projectile]
    return nuclide_reformat(projectile)


def is_ion_projectile(projectile):
    projectile = str(projectile).upper()
    if projectile in LIGHT_ION_PROJECTILES:
        return True
    if projectile in ("0", "N", "G"):
        return False
    parts = projectile.split("-")
    return len(parts) >= 3 and parts[0].isdigit() and int(parts[0]) > 0


def generate_exfortables_file_path(input_store):
    obs_type = (input_store.get("obs_type") or "").upper()
    elem = input_store.get("target_elem")
    mass = input_store.get("target_mass")
    branch = input_store.get("branch")

    reaction = input_store.get("reaction")
    level_num = input_store.get("level_num")

    target = f"{elem.capitalize()}-{str(mass)}"
    exfiles = []

    if obs_type in _EXFOR_SCALAR_GLOBS:
        reaction_dir = reaction.replace(",", "-").lower()
        directory = Path(
            EXFORTABLES_PY_GIT_REPO_PATH,
            reaction.split(",", 1)[0].lower(),
            target,
            reaction_dir,
        )
        return _glob_text_files(directory, _EXFOR_SCALAR_GLOBS[obs_type])

    if input_store.get("page_param") == "ion" or is_ion_projectile(
        reaction.split(",")[0]
    ):
        outgoing = reaction.split(",", 1)[1].lower()
        dir = os.path.join(
            EXFORTABLES_PY_GIT_REPO_PATH,
            "ion",
            target,
            projectile_reformat(reaction.split(",")[0]),
            outgoing if not level_num else outgoing + "-L" + str(level_num),
            obs_type.lower() if obs_type != "RP" else "xs",
        )

    elif level_num:
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

        if not get_str_from_string(rp_mass):
            residual = f"{rp_elem.capitalize()}-{str(rp_mass.lstrip('0'))}"

        else:
            residual = f"{rp_elem.capitalize()}-{str(rp_mass.lstrip('0'))}-{get_str_from_string(rp_mass).upper()}"

        if os.path.exists(dir):
            exfiles = [os.path.join(dir, f) for f in os.listdir(dir) if residual in f]

    else:
        if os.path.exists(dir):
            exfiles = [os.path.join(dir, f) for f in os.listdir(dir)]

    return exfiles 



def generate_endftables_file_path(input_store):
    """
    Generate the direct file links
    """
    obs_type = (input_store.get("obs_type") or "").upper()
    elem = input_store.get("target_elem")
    mass = input_store.get("target_mass")
    reaction = input_store.get("reaction")
    mt = input_store.get("mt")

    if obs_type in _EXFOR_SCALAR_GLOBS:
        return _resonancetables_file_path(input_store)

    target = f"{elem.capitalize()}{str(mass).zfill(3)}"

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
                libfiles += [os.path.join(dir, f) for f in os.listdir(dir) if residual in f]

        else:
            if os.path.exists(dir):
                libfiles += [os.path.join(dir, f) for f in os.listdir(dir) if f"MT{mt.zfill(3)}" in f]

    return libfiles


def sanitize_for_js(obj):
    """
    sanitize strings for JavaScript
    """
    if isinstance(obj, dict):
        return {
            str(k): sanitize_for_js(v)
            for k, v in obj.items()
        }

    elif isinstance(obj, list):
        return [sanitize_for_js(v) for v in obj]

    elif isinstance(obj, str):

        return (
            obj
            .replace("\u2028", "\\u2028")
            .replace("\u2029", "\\u2029")
        )

    return obj
