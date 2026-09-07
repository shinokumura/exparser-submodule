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
import re
from pathlib import Path

from config import (
    DATA_DIR,
    ENDFTABLES_PATH,
    EXFORTABLES_PY_GIT_REPO_PATH,
    RESONANCETABLES_GIT_REPO_PATH,
)
from submodules.utilities.elem import elemtoz
from submodules.utilities.obs_types import (
    EXFOR_ONLY_FILE_DOWNLOADS,
    GAMMA_PRODUCTION_MT,
    GAMMA_PRODUCTION_OBS_TYPE,
    GAMMA_PRODUCTION_SF4,
    sf6_to_dir,
)
from submodules.utilities.reaction import (
    convert_partial_reactionstr_to_inl,
    get_endf_mts,
)
from submodules.utilities.util import (
    get_number_from_string,
    get_str_from_string,
    libstyle_nuclide_expression,
)


def open_json(file):
    if os.path.exists(file):
        with open(file) as json_file:
            return json.load(json_file)
    else:
        return None



LIB_LIST_MAX = {
    "tendl.2023": "TENDL-2023",
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
    "GPROD": "SIG",
    "SFC": "SIG",
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
    "GPROD": "xs",
    "SFC": "xs",
    "TH": "xs",
    "RP": "residual",
    "FY": "fission",
    "DA": "angle",
    "DE": "energy",
    "TRN": None,
    "DDX": None,
}


PARTICLE_PROJECTILE_DIRS = {
    "0": "0",
    "A": "a",
    "D": "d",
    "E": "e",
    "G": "g",
    "H": "h",
    "HE3": "h",
    "N": "n",
    "P": "p",
    "T": "t",
}


_EXFOR_SCALAR_GLOBS = {
    "TH": ("thermal/*.txt",),
    "RI": ("resonance_integral/*.txt",),
    "MACS": ("macs/*.txt",),
    "D0": ("resonance_spacing/*.txt",),
    "D1": ("resonance_spacing/*.txt",),
    "D2": ("resonance_spacing/*.txt",),
    "S0": ("strength_function/*.txt",),
    "S1": ("strength_function/*.txt",),
    "GG0": ("gamma_gamma/*.txt",),
    "GG1": ("gamma_gamma/*.txt",),
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


def _exfor_file_energy_range(filename):
    """Read an energy-resolved EXFOR file's incident-energy range in MeV."""
    path = Path(filename)
    match = re.search(r"_E([0-9.eE+-]+)_", path.name)
    if match:
        energy = float(match.group(1)) / 1e6
        return energy, energy
    try:
        with path.open(encoding="utf-8") as stream:
            for _ in range(30):
                line = stream.readline()
                if not line:
                    break
                match = re.match(
                    r"#\s*Incident energy:\s*([0-9.eE+-]+)\s*MeV"
                    r"(?:\s*-\s*([0-9.eE+-]+)\s*MeV)?",
                    line,
                )
                if match:
                    lower = float(match.group(1))
                    upper = float(match.group(2) or match.group(1))
                    return min(lower, upper), max(lower, upper)
    except (OSError, UnicodeDecodeError, ValueError):
        return None
    return None


def _distance_to_energy_range(target, energy_range):
    lower, upper = energy_range
    if target < lower:
        return lower - target
    if target > upper:
        return target - upper
    return 0


def download_source_path(filename):
    """Return the Data Explorer download source and source-relative path."""
    path = Path(filename).absolute()
    source_roots = (
        ("exfortables_py", Path(EXFORTABLES_PY_GIT_REPO_PATH).absolute()),
        ("resonancetables", Path(RESONANCETABLES_GIT_REPO_PATH).absolute()),
        ("endftables", Path(ENDFTABLES_PATH).absolute()),
        ("data", Path(DATA_DIR).absolute()),
    )
    for source, root in source_roots:
        if path.is_relative_to(root):
            return source, path.relative_to(root)
    return None, None


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

    nuclide = libstyle_nuclide_expression(
        input_store.get("target_elem"),
        input_store.get("target_mass"),
    )
    directory = Path(RESONANCETABLES_GIT_REPO_PATH, top_dir, quantity, "nuc")
    return _glob_text_files(directory, (f"{nuclide}_*.txt",))


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
    if projectile in PARTICLE_PROJECTILE_DIRS:
        return PARTICLE_PROJECTILE_DIRS[projectile]
    return nuclide_reformat(projectile)


def _ion_projectile_directory(projectile):
    """Return the EXFORTABLES_py directory name for an ion projectile."""
    return projectile_reformat(projectile)


def is_ion_projectile(projectile):
    return is_heavy_ion_projectile(projectile)


def is_heavy_ion_projectile(projectile):
    """Return whether an EXFOR projectile is a nuclide heavier than helium."""
    projectile = str(projectile).upper()
    parts = projectile.split("-")
    return len(parts) >= 3 and parts[0].isdigit() and int(parts[0]) > 2


def generate_exfortables_file_path(input_store):
    obs_type = (input_store.get("obs_type") or "").upper()
    storage_obs_type = (
        "XS" if obs_type == GAMMA_PRODUCTION_OBS_TYPE else obs_type
    )
    elem = input_store.get("target_elem")
    mass = input_store.get("target_mass")
    branch = input_store.get("branch")

    reaction = input_store.get("reaction")
    level_num = input_store.get("level_num")

    target = f"{elem.capitalize()}-{str(mass)}"
    exfiles = []
    storage_dir = sf6_to_dir.get(
        pageparam_to_sf6.get(storage_obs_type, storage_obs_type),
        storage_obs_type.lower(),
    )

    if obs_type in _EXFOR_SCALAR_GLOBS:
        projectile, outgoing = reaction.split(",", 1)
        projectile_dir = projectile_reformat(projectile)
        reaction_dir = f"{projectile_dir}-{outgoing.lower()}"
        directory = Path(
            EXFORTABLES_PY_GIT_REPO_PATH,
            projectile_dir,
            target,
            reaction_dir,
        )
        return _glob_text_files(directory, _EXFOR_SCALAR_GLOBS[obs_type])

    projectile = reaction.split(",", 1)[0]
    path_reaction = (
        convert_partial_reactionstr_to_inl(reaction) if level_num else reaction
    )
    path_projectile, path_outgoing = path_reaction.split(",", 1)
    projectile_dir = projectile_reformat(path_projectile)
    reaction_dir = f"{projectile_dir}-{path_outgoing.lower()}"
    ion_reaction_dir = path_reaction.split(",", 1)[1].lower()
    if level_num:
        reaction_dir += "-L" + str(level_num)
        ion_reaction_dir += "-L" + str(level_num)

    if is_ion_projectile(projectile):
        dir = os.path.join(
            EXFORTABLES_PY_GIT_REPO_PATH,
            "ion",
            target,
            _ion_projectile_directory(projectile),
            ion_reaction_dir,
            storage_dir if obs_type != "RP" else "xs",
        )

    elif obs_type == "FY":
        fy_type = input_store.get("fy_type")
        dir = os.path.join(
            EXFORTABLES_PY_GIT_REPO_PATH,
            projectile_dir,
            target,
            reaction_dir,
            "fission/yield",
            fy_type.lower(),
        )

    else:
        dir = os.path.join(
            EXFORTABLES_PY_GIT_REPO_PATH,
            projectile_dir,
            target,
            reaction_dir,
            storage_dir if obs_type != "RP" else "xs",
        )

    if obs_type == "RP":
        ## Format is "Ag-109-M"
        rp_elem = input_store.get("rp_elem")
        rp_mass = input_store.get("rp_mass")
        mass_number = get_number_from_string(rp_mass)
        isomer = get_str_from_string(rp_mass)
        residual = f"{rp_elem.capitalize()}-{mass_number}"
        if isomer:
            residual += f"-{isomer.upper()}"

        # RP index results are selected by residual nuclide, not by one
        # process directory.  For example, a p,x search can contain both p,x
        # and p,2n datasets, so search all reaction directories for this
        # target/projectile and let the shared EXFOR entry IDs select the same
        # datasets that are loaded into the plot.
        if is_ion_projectile(projectile):
            projectile_root = Path(
                EXFORTABLES_PY_GIT_REPO_PATH,
                "ion",
                target,
                _ion_projectile_directory(projectile),
            )
        else:
            projectile_root = Path(
                EXFORTABLES_PY_GIT_REPO_PATH,
                projectile_dir,
                target,
            )
        exfiles = [
            str(file)
            for file in projectile_root.glob("*/xs/*.txt")
            if residual in file.name
        ]

    elif (
        obs_type == "DA"
        and level_num is None
        and reaction.split(",", 1)[1].lower() == "inl"
    ):
        if is_ion_projectile(projectile):
            reaction_root = Path(
                EXFORTABLES_PY_GIT_REPO_PATH,
                "ion",
                target,
                _ion_projectile_directory(projectile),
            )
            reaction_pattern = "inl*/angle/*.txt"
        else:
            reaction_root = Path(
                EXFORTABLES_PY_GIT_REPO_PATH,
                projectile_dir,
                target,
            )
            reaction_pattern = f"{projectile_dir}-inl*/angle/*.txt"
        exfiles = [str(file) for file in reaction_root.glob(reaction_pattern)]

    else:
        if os.path.exists(dir):
            exfiles = [os.path.join(dir, f) for f in os.listdir(dir)]
            if obs_type == GAMMA_PRODUCTION_OBS_TYPE:
                sf4_token = f"_{GAMMA_PRODUCTION_SF4.split('-', 1)[1]}_"
                exfiles = [
                    file for file in exfiles if sf4_token in Path(file).name
                ]

    if (
        obs_type in {"DA", "DDX"}
        and input_store.get("inc_en") is not None
    ):
        target_energy = float(input_store["inc_en"])
        tolerance_pct = float(
            input_store.get("inc_en_tolerance_pct") or 0
        )
        tolerance = max(
            abs(target_energy) * tolerance_pct / 100,
            1e-12,
        )
        matching_files = []
        for file in exfiles:
            energy_range = _exfor_file_energy_range(file)
            if (
                energy_range is not None
                and energy_range[0] <= target_energy + tolerance
                and energy_range[1] >= target_energy - tolerance
            ):
                matching_files.append(file)
        exfiles = matching_files

    entry_ids = input_store.get("exfor_entry_ids")
    if entry_ids is not None:
        entry_ids = {str(entry_id) for entry_id in entry_ids}
        exfiles = [
            file
            for file in exfiles
            if any(entry_id in Path(file).name for entry_id in entry_ids)
        ]
        if obs_type == "DA" and input_store.get("inc_en") is not None:
            target_energy = float(input_store["inc_en"])
            nearest_files = []
            for entry_id in entry_ids:
                entry_files = [
                    file for file in exfiles if entry_id in Path(file).name
                ]
                energy_ranges = {
                    file: energy_range
                    for file in entry_files
                    if (energy_range := _exfor_file_energy_range(file))
                    is not None
                }
                if energy_ranges:
                    nearest_distance = min(
                        _distance_to_energy_range(target_energy, energy_range)
                        for energy_range in energy_ranges.values()
                    )
                    nearest_files.extend(
                        file
                        for file, energy_range in energy_ranges.items()
                        if _distance_to_energy_range(
                            target_energy,
                            energy_range,
                        ) == nearest_distance
                    )
            exfiles = nearest_files
    if obs_type == "FY" and input_store.get("energy_range"):
        lower, upper = map(float, input_store["energy_range"])
        exfiles = [
            file
            for file in exfiles
            if (
                (energy_range := _exfor_file_energy_range(file)) is not None
                and energy_range[1] >= lower
                and energy_range[0] < upper
            )
        ]
    selected_residual = input_store.get("residual")
    if selected_residual:
        residual_token = f"_{selected_residual}_"
        exfiles = [
            file for file in exfiles if residual_token in Path(file).name
        ]

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
    page_param = (input_store.get("page_param") or "").upper()
    if (
        obs_type in EXFOR_ONLY_FILE_DOWNLOADS
        or page_param in EXFOR_ONLY_FILE_DOWNLOADS
    ):
        return []
    if obs_type in _EXFOR_SCALAR_GLOBS:
        return _resonancetables_file_path(input_store)
    storage_dir = pageparam_to_endftables_obs_type.get(obs_type)
    if storage_dir is None:
        return []

    target = f"{elem.capitalize()}{str(mass).zfill(3)}"

    selected_libraries = input_store.get("selected_libraries")
    libraries = (
        [lib for lib in selected_libraries if lib in LIB_LIST_MAX]
        if selected_libraries
        else LIB_LIST_MAX
    )
    mts = (
        (GAMMA_PRODUCTION_MT,)
        if obs_type == GAMMA_PRODUCTION_OBS_TYPE
        else get_endf_mts(
            reaction,
            mt,
            input_store.get("level_num"),
        )
    )
    mt_tokens = {f"MT{value:03d}" for value in mts}
    libfiles = []
    for lib in libraries:
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
                storage_dir,
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
                libfiles += [
                    os.path.join(dir, f)
                    for f in os.listdir(dir)
                    if any(token in f for token in mt_tokens)
                ]

    if obs_type == "DA" and input_store.get("inc_en") is not None:
        target_energy = float(input_store["inc_en"])
        matching_files = []
        for file in libfiles:
            match = re.search(r"-Eang([0-9.]+)\.", Path(file).name)
            if match and abs(float(match.group(1)) - target_energy) <= 5e-4:
                matching_files.append(file)
        libfiles = matching_files

    if obs_type == "FY" and input_store.get("energy_range"):
        lower, upper = map(float, input_store["energy_range"])
        matching_files = []
        for file in libfiles:
            match = re.search(
                r"-E([0-9.eE+-]+)\.",
                Path(file).name,
            )
            if match and lower <= float(match.group(1)) < upper:
                matching_files.append(file)
        libfiles = matching_files

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
