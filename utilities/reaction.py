####################################################################
#
# This file is part of exfor-parser.
# Copyright (C) 2022 International Atomic Energy Agency (IAEA)
#
# Disclaimer: The code is still under developments and not ready
#             to use. It has been made public to share the progress
#             among collaborators.
# Contact:    nds.contact-point@iaea.org
#
####################################################################
import os
import re
import json

from config import MT_PATH_JSON
from submodules.utilities.util import get_number_from_string, get_str_from_string



resid_mt_range = {
    "N": list(range(50, 90)),
    "P": list(range(600, 649)),
    "D": list(range(650, 699)),
    "T": list(range(700, 749)),
    "H": list(range(750, 799)),
    "A": list(range(800, 849)),
    "G": [102],
}  # not sure about photon induced case


def read_mt_json():
    if os.path.exists(MT_PATH_JSON):
        with open(MT_PATH_JSON) as map_file:
            return json.load(map_file)


MT_BRANCH_LIST_FY = {
    "Primary": {"branch": "PRE", "mt": "460"},
    "Independent": {"branch": "IND", "mt": "454"},
    "Cumulative": {"branch": "CUM", "mt": "459"},
}


def fy_branch(branch):
    if branch == "PRE":
        return ["PRE", "TER", "QTR", "PRV", "TER/CHG"]

    elif branch == "IND":
        return ["IND", "SEC", "MAS", "CHG", "SEC/CHN"]

    elif branch == "CUM":
        return ["CUM", "CHN"]

    else:
        return [branch]


def reaction_list(projectile):
    if not projectile:
        return read_mt_json()

    assert len(projectile) == 1

    all = read_mt_json()
    # "EL": {
    #   "mt": "2",
    #   "reaction": "(n,elas.)",
    #   "sf5-8": null
    #  },

    all = {
        "N" if projectile.upper() != "N" and k == "INL" else k: i
        for k, i in all.items()
    }
    partial = {}

    for p in resid_mt_range.keys():
        for n in range(len(resid_mt_range[p.upper()])):
            partial[f"{p.upper()}{str(n)}"] = {
                "mt": str(resid_mt_range[p.upper()][n]),
                "reaction": f"({projectile.lower()},{p.lower()}{str(n)})",
                "sf5-8": "PAR,SIG,,",
            }

    return dict(**all, **partial)


def exfor_reaction_list(projectile):
    if not projectile:
        return read_mt_json()

    assert len(projectile) == 1

    all = read_mt_json()
    # "EL": {
    #   "mt": "2",
    #   "reaction": "(n,elas.)",
    #   "sf5-8": null
    #  },

    all = {
        "N" if projectile.upper() != "N" and k == "INL" else k: i
        for k, i in all.items()
    }

    return all


def get_mt(reac):
    reactions = reaction_list(reac.split(",")[0].upper())
    return reactions[reac.split(",")[1].upper()]["mt"]





def convert_partial_reactionstr_to_inl(reaction):
    ## convert the  partial reaction string format to exfor code
    if reaction.split(",")[0].upper() == reaction.split(",")[1][0].upper():
        ## (n,n1) --> (N,INL) but if (n,p1) then next
        ## (a,a1) --> (A,INL)
        return f"{reaction.split(',')[0].upper()},INL"
    else:
        ## such as (n,a1) --> (N,A)
        return get_str_from_string(reaction).upper()


def convert_reaction_to_exfor_style(reaction):
    if reaction.split(",")[0].upper() == "H":
        ## incident "h" --> "HE3"
        reaction = f"HE3,{reaction.split(',')[1].upper()}"

    return reaction
