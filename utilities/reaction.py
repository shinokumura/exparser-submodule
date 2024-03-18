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
    #   "reaction": "elas.",
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
                "reaction": f"({projectile.lower()},{p.lower()}{str(n)}",
                "sf5-8": "PAR,SIG,,",
            }

    return dict(**all, **partial)



mt_list = {
 "1": "total",
 "2": "elastic",
 "3": "nonelastic",
 "4": "inelastic",
 "5": "X",
 "10": "continuum",
 "11": "2nd",
 "16": "2n",
 "17": "3n",
 "18": "f",
 "19": "0f",
 "20": "nf",
 "21": "2nf",
 "38": "3nf",
 "22": "na",
 "23": "n3a",
 "24": "2na",
 "25": "3na",
 "27": "abs",
 "28": "np",
 "29": "n2a",
 "30": "2n2a",
 "32": "nd",
 "33": "nt",
 "34": "nh",
 "35": "nd2a",
 "36": "nt2a",
 "37": "4n",
 "41": "2np",
 "42": "3np",
 "44": "n2p",
 "45": "npa",
 "101": "disapper",
#  "102": "g",
 "103": "p",
 "104": "d",
 "105": "t",
 "106": "h",
 "107": "a",
 "108": "2a",
 "109": "3a",
 "111": "2p",
 "112": "pa",
 "113": "t2a",
 "114": "d2a",
 "115": "pd",
 "116": "pt",
 "117": "da",
 "151": "RES",
 "152": "5n",
 "153": "6n",
 "154": "2nt",
 "155": "ta",
 "156": "4np",
 "157": "3nd",
 "158": "n'da",
 "159": "2npa",
 "160": "7n",
 "161": "8n",
 "162": "5np",
 "163": "6np",
 "164": "7np",
 "165": "4na",
 "166": "5na",
 "167": "6na",
 "168": "7na",
 "169": "4nd",
 "170": "5nd",
 "171": "6nd",
 "172": "3nt",
 "173": "4nt",
 "174": "5nt",
 "175": "6nt",
 "176": "2nh",
 "177": "3nh",
 "178": "4nh",
 "179": "3n2p",
 "180": "3n2a",
 "181": "3npa",
 "182": "dt",
 "183": "n'pd",
 "184": "n'pt",
 "185": "n'dt",
 "186": "n'ph",
 "187": "n'dh",
 "188": "n'th",
 "189": "n'ta",
 "190": "2n2p",
 "191": "ph",
 "192": "dh",
 "193": "ha",
 "194": "4n2p",
 "195": "4n2a",
 "196": "4npa",
 "197": "3p",
 "198": "n'3p",
 "199": "3n2pa",
 "200": "5n2p",
 "201": "Xn",
 "202": "Xg",
 "203": "Xp",
 "204": "Xd",
 "205": "Xt",
 "206": "Xh",
 "207": "Xa",
 "452": "nu_bar_t average total number of neutrons",
 "454": "Independent fission product yield data",
 "455": "nu_bar_d average number of delayed neutrons",
 "456": "nu_bar_p average number of prompt neutron",
 "457": "Radioactive decay data",
 "458": "Energy release in fission for incident neutrons",
 "459": "Cumulative fission product yield data",
}


def generate_mt_list(projectile):
    all = {}
    if len(projectile) == 1:
        all = { k: (f"{projectile.lower()},n" if projectile.upper() != "N" and k == "4" else f"{projectile.lower()},{i}") for (k, i) in mt_list.items() }
        partial = {}

        for p in resid_mt_range.keys():
            for n in range(len(resid_mt_range[p.upper()])):
                partial[str(resid_mt_range[p.upper()][n])] = f"({projectile.lower()},{p.lower()}{str(n)})"

    return dict(**all, **partial)





def exfor_reaction_list(projectile):
    if not projectile:
        return read_mt_json()

    assert len(projectile) == 1

    all = read_mt_json()
    # "EL": {
    #   "mt": "2",
    #   "reaction": "elas.",
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
