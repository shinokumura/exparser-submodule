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

from .util import get_str_from_string


## Previous mf3.json in MT_PATH_JSON
sf3_dict = {
    "TOT": {"mt": "1", "reaction": "(n,total)", "sf5-8": None},
    "EL": {"mt": "2", "reaction": "(n,elas.)", "sf5-8": None},
    "NON": {"mt": "3", "reaction": "(n,nonelas.)", "sf5-8": None},
    "INL": {"mt": "4", "reaction": "(n,inelas.)", "sf5-8": "SIG"},
    "F": {"mt": "18", "reaction": "(n,f)", "sf5-8": None},
    "G": {"mt": "102", "reaction": "(n,g)", "sf5-8": None},
    "P": {"mt": "103", "reaction": "(n,p)", "sf5-8": None},
    "D": {"mt": "104", "reaction": "(n,d)", "sf5-8": None},
    "T": {"mt": "105", "reaction": "(n,t)", "sf5-8": None},
    "HE3": {"mt": "106", "reaction": "(n,h)", "sf5-8": None},
    "A": {"mt": "107", "reaction": "(n,a)", "sf5-8": None},
    "ABS": {"mt": "27", "reaction": "(n,abs)", "sf5-8": None},
    "2N": {"mt": "16", "reaction": "(n,2n)", "sf5-8": None},
    "3N": {"mt": "17", "reaction": "(n,3n)", "sf5-8": None},
    "FIS": {"mt": "19", "reaction": "(n,0f)", "sf5-8": "first"},
    "N+F": {"mt": "20", "reaction": "(n,nf)", "sf5-8": "2nd"},
    "2N+F": {"mt": "21", "reaction": "(n,2nf)", "sf5-8": "3rd"},
    "3N+F": {"mt": "38", "reaction": "(n,3nf)", "sf5-8": "4th"},
    "X+N": {"mt": "201", "reaction": "(n,Xn)", "sf5-8": None},
    "X+G": {"mt": "202", "reaction": "(n,Xg)", "sf5-8": None},
    "X+P": {"mt": "203", "reaction": "(n,Xp)", "sf5-8": None},
    "X+D": {"mt": "205", "reaction": "(n,Xt)", "sf5-8": None},
    "X+HE3": {"mt": "206", "reaction": "(n,Xh)", "sf5-8": None},
    "X+A": {"mt": "207", "reaction": "(n,Xa)", "sf5-8": None},
    "N+A": {"mt": "22", "reaction": "(n,na)", "sf5-8": None},
    "N+3A": {"mt": "23", "reaction": "(n,n3a)", "sf5-8": None},
    "N+P": {"mt": "28", "reaction": "(n,np)", "sf5-8": None},
    "N+2A": {"mt": "29", "reaction": "(n,n2a)", "sf5-8": None},
    "N+D": {"mt": "32", "reaction": "(n,nd)", "sf5-8": None},
    "N+T": {"mt": "33", "reaction": "(n,nt)", "sf5-8": None},
    "N+H": {"mt": "34", "reaction": "(n,nh)", "sf5-8": None},
    "N+D+2A": {"mt": "35", "reaction": "(n,nd2a)", "sf5-8": None},
    "N+T+2A": {"mt": "36", "reaction": "(n,nt2a)", "sf5-8": None},
    "N+2P": {"mt": "44", "reaction": "(n,n2p)", "sf5-8": None},
    "N+P+A": {"mt": "45", "reaction": "(n,npa)", "sf5-8": None},
    "2N+A": {"mt": "24", "reaction": "(n,2na)", "sf5-8": None},
    "2N+D": {"mt": "11", "reaction": "(n,2nd)", "sf5-8": None},
    "2N+2A": {"mt": "30", "reaction": "(n,2n2a)", "sf5-8": None},
    "2N+P": {"mt": "41", "reaction": "(n,2np)", "sf5-8": None},
    "2N+T": {"mt": "154", "reaction": "(n,2nt)", "sf5-8": None},
    "2N+2P": {"mt": "190", "reaction": "(n,2n2p)", "sf5-8": None},
    "2N+P+A": {"mt": "159", "reaction": "(n,2npa)", "sf5-8": None},
    "3N+A": {"mt": "25", "reaction": "(n,3na)", "sf5-8": None},
    "3N+P": {"mt": "42", "reaction": "(n,3np)", "sf5-8": None},
    "3N+D": {"mt": "157", "reaction": "(n,3nd)", "sf5-8": None},
    "2A": {"mt": "108", "reaction": "(n,2a)", "sf5-8": None},
    "3A": {"mt": "109", "reaction": "(n,3a)", "sf5-8": None},
    "2P": {"mt": "111", "reaction": "(n,2p)", "sf5-8": None},
    "P+A": {"mt": "112", "reaction": "(n,pa)", "sf5-8": None},
    "T+2A": {"mt": "113", "reaction": "(n,t2a)", "sf5-8": None},
    "D+2A": {"mt": "114", "reaction": "(n,d2a)", "sf5-8": None},
    "P+D": {"mt": "115", "reaction": "(n,pd)", "sf5-8": None},
    "P+T": {"mt": "116", "reaction": "(n,pt)", "sf5-8": None},
    "D+A": {"mt": "117", "reaction": "(n,da)", "sf5-8": None},
    "4N": {"mt": "37", "reaction": "(n,4n)", "sf5-8": None},
    "4N+P": {"mt": "156", "reaction": "(n,4np)", "sf5-8": None},
    "5N": {"mt": "152", "reaction": "(n,5n)", "sf5-8": None},
    "6N": {"mt": "153", "reaction": "(n,6n)", "sf5-8": None},
    "T+A": {"mt": "155", "reaction": "(n,ta)", "sf5-8": None},
    "N+D+A": {"mt": "158", "reaction": "(n,n'da)", "sf5-8": None},
    "7N": {"mt": "160", "reaction": "(n,7n)", "sf5-8": None},
    "8N": {"mt": "161", "reaction": "(n,8n)", "sf5-8": None},
    "5N+P": {"mt": "162", "reaction": "(n,5np)", "sf5-8": None},
    "6N+P": {"mt": "163", "reaction": "(n,6np)", "sf5-8": None},
    "7N+P": {"mt": "164", "reaction": "(n,7np)", "sf5-8": None},
    "4N+A": {"mt": "165", "reaction": "(n,4na)", "sf5-8": None},
    "5N+A": {"mt": "166", "reaction": "(n,5na)", "sf5-8": None},
    "6N+A": {"mt": "167", "reaction": "(n,6na)", "sf5-8": None},
    "7N+A": {"mt": "168", "reaction": "(n,7na)", "sf5-8": None},
    "4N+D": {"mt": "169", "reaction": "(n,4nd)", "sf5-8": None},
    "5N+D": {"mt": "170", "reaction": "(n,5nd)", "sf5-8": None},
    "6N+D": {"mt": "171", "reaction": "(n,6nd)", "sf5-8": None},
    "3N+T": {"mt": "172", "reaction": "(n,3nt)", "sf5-8": None},
    "4N+T": {"mt": "173", "reaction": "(n,4nt)", "sf5-8": None},
    "5N+T": {"mt": "174", "reaction": "(n,5nt)", "sf5-8": None},
    "6N+T": {"mt": "175", "reaction": "(n,6nt)", "sf5-8": None},
    "2N+HE3": {"mt": "176", "reaction": "(n,2nh)", "sf5-8": None},
    "3N+HE3": {"mt": "177", "reaction": "(n,3nh)", "sf5-8": None},
    "4N+HE3": {"mt": "178", "reaction": "(n,4nh)", "sf5-8": None},
    "3N+2P": {"mt": "179", "reaction": "(n,3n2p)", "sf5-8": None},
    "3N+2": {"mt": "180", "reaction": "(n,3n2a)", "sf5-8": None},
    "3N+P+A": {"mt": "181", "reaction": "(n,3npa)", "sf5-8": None},
    "D+T": {"mt": "182", "reaction": "(n,dt)", "sf5-8": None},
    "N+P+D": {"mt": "183", "reaction": "(n,n'pd)", "sf5-8": None},
    "N+P+T": {"mt": "184", "reaction": "(n,n'pt)", "sf5-8": None},
    "N+D+T": {"mt": "185", "reaction": "(n,n'dt)", "sf5-8": None},
    "N+P+HE3": {"mt": "186", "reaction": "(n,n'ph)", "sf5-8": None},
    "N+D+HE3": {"mt": "187", "reaction": "(n,n'dh)", "sf5-8": None},
    "N+T+HE3": {"mt": "188", "reaction": "(n,n'th)", "sf5-8": None},
    "N+T+A": {"mt": "189", "reaction": "(n,n'ta)", "sf5-8": None},
    "P+HE3": {"mt": "191", "reaction": "(n,ph)", "sf5-8": None},
    "D+HE": {"mt": "192", "reaction": "(n,dh)", "sf5-8": None},
    "HE+A": {"mt": "193", "reaction": "(n,ha)", "sf5-8": None},
    "4N+2P": {"mt": "194", "reaction": "(n,4n2p)", "sf5-8": None},
    "4N+2A": {"mt": "195", "reaction": "(n,4n2a)", "sf5-8": None},
    "4N+P+A": {"mt": "196", "reaction": "(n,4npa)", "sf5-8": None},
    "3P": {"mt": "197", "reaction": "(n,3p)", "sf5-8": None},
    "N+3P": {"mt": "198", "reaction": "(n,n'3p)", "sf5-8": None},
    "3N+2P+A": {"mt": "199", "reaction": "(n,3n2pa)", "sf5-8": None},
    "5N+2P": {"mt": "200", "reaction": "(n,5n2p)", "sf5-8": None},
    "X": {"mt": "10", "reaction": "(n,contin.)", "sf5-8": "CON,SIG"},
}

mt_range = {
    "N": list(range(50, 92)),
    "P": list(range(600, 649)),
    "D": list(range(650, 699)),
    "T": list(range(700, 749)),
    "H": list(range(750, 799)),
    "A": list(range(800, 849)),
    "G": list(range(102)),
}



resid_mt_range = {
    "N": list(range(50, 90)),
    "P": list(range(600, 649)),
    "D": list(range(650, 699)),
    "T": list(range(700, 749)),
    "H": list(range(750, 799)),
    "A": list(range(800, 849)),
    "G": [102],
}  # not sure about photon induced case



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
    "451": "Descriptive Data and Directory",
    "452": "nu_bar_t average total number of neutrons",
    "454": "Independent fission product yield data",
    "455": "nu_bar_d average number of delayed neutrons",
    "456": "nu_bar_p average number of prompt neutron",
    "457": "Radioactive decay data",
    "458": "Energy release in fission for incident neutrons",
    "459": "Cumulative fission product yield data",
    "460": "Delayed Photon Data",
}

## The allowed SF5 for SIG data
sig_sf5 = {"IND": "RP", "CUM": "RP", "(CUM)": "RP", "M+": "RP", "M-": "RP", "(M)": "RP"}


## The MT number allocation and directory name for FY data
mt_fy_sf5 = {
    "CUM": {"mt": "459", "name": "cumulative"},
    "CHN": {"mt": "459", "name": "cumulative"},
    "IND": {"mt": "454", "name": "independent"},
    "SEC": {"mt": "454", "name": "independent"},
    "MAS": {"mt": "454", "name": "independent"},
    "SEC/CHN": {"mt": "454", "name": "independent"},
    "CHG": {"mt": "454", "name": "independent"},
    "PRE": {"mt": "460", "name": "primary"},
    "PRV": {"mt": "460", "name": "primary"},
    "TER": {"mt": "460", "name": "primary"},
    "QTR": {"mt": "460", "name": "primary"},
    "PR": {"mt": "460", "name": "primary"},
}

## The MT number allocation and directory name for NU (neutron observables) data
mt_nu_sf5 = {
    "": {"mt": "452", "name": ""},
    "PR": {"mt": "456", "name": "prompt"},
    "SEC": "",
    "SEC/PR": {"mt": None, "name": "miscellaneous"},
    "DL": {"mt": "455", "name": "delayed"},
    "TER": {"mt": "456", "name": "prompt"},
    "DL/CUM": {"mt": "455", "name": "delayed"},
    "DL/GRP": {"mt": "455", "name": "delayed"},
    "PR/TER": {"mt": None, "name": "miscellaneous"},
    "PRE/PR": {"mt": None, "name": "miscellaneous"},
    "SEC/PR": {"mt": None, "name": "miscellaneous"},
    "PRE/PR/FRG": {"mt": None, "name": "miscellaneous"},
    "PR/FRG": {"mt": None, "name": "miscellaneous"},
    "PR/PAR": {"mt": None, "name": "miscellaneous"},
    "PR/NUM": {"mt": None, "name": "miscellaneous"},
    "NUM": {"mt": None, "name": "miscellaneous"},
}


MT_BRANCH_LIST_FY = {
    "Primary": {"branch": "PRE", "mt": "460"},
    "Independent": {"branch": "IND", "mt": "454"},
    "Cumulative": {"branch": "CUM", "mt": "459"},
}


sf_to_mf = {
    "NU": "1",
    "WID": "2",
    "ARE": "2",
    "D": "2",
    "EN": "2",
    "J": "2",
    "SIG": "3",
    "DA": "4",
    "DE": "5",
    "FY": "8",
    "DA/DE": "6",
}


sf6_to_dir = {
    "SIG": "xs",
    "DA": "angle",
    "DE": "energy",
    "NU": "neutrons",
    "DL": "neutrons",
    "NU/DE": "neutrons/energy",
    "FY": "fission/yield",
    "FY/DE": "fission/energy",
    "KE": "kinetic_energy",
    "AKE": "kinetic_energy/average",
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
        return sf3_dict

    assert len(projectile) == 1

    all = sf3_dict
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
                "reaction": f"({projectile.lower()},{p.lower()}{str(n)}",
                "sf5-8": "PAR,SIG,,",
            }

    return dict(**all, **partial)



def exfor_reaction_list(projectile):
    if not projectile:
        return sf3_dict

    assert len(projectile) == 1

    all = sf3_dict
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

def generate_mt_list(projectile):
    all = {}
    if len(projectile) == 1:
        all = {
            k: (
                f"{projectile.lower()},n"
                if projectile.upper() != "N" and k == "4"
                else f"{projectile.lower()},{i}"
            )
            for (k, i) in mt_list.items()
        }
        partial = {}

        for p in resid_mt_range.keys():
            for n in range(len(resid_mt_range[p.upper()])):
                partial[
                    str(resid_mt_range[p.upper()][n])
                ] = f"{projectile.lower()},{p.lower()}{str(n)}"
    # print(all, partial)
    return dict(**all, **partial)



def get_mf(react_dict):

    if sf_to_mf.get(react_dict["sf6"]):
        if react_dict["sf6"] == "NU":
            return int(sf_to_mf[react_dict["sf6"]])

        elif react_dict["sf4"] == "0-G-0":
            return 12  # Multiplicity of photon production

        else:
            return int(sf_to_mf[react_dict["sf6"]])

    else:
        return 9999


def get_mt(react_dict):

    if react_dict["sf6"] == "FY":
        return (
            int(mt_fy_sf5[react_dict["sf5"]]["mt"])
            if react_dict["sf5"] and mt_fy_sf5.get(react_dict["sf5"])
            else None
        )

    elif react_dict["sf6"] == "NU":
        return (
            int(mt_nu_sf5[react_dict["sf5"]]["mt"])
            if react_dict["sf5"]
            and mt_nu_sf5.get(react_dict["sf5"])
            and mt_nu_sf5[react_dict["sf5"]]["mt"]
            else None
        )

    else:
        if react_dict["process"] == "N,INL":
            return 4

        elif (
            react_dict["process"].split(",")[0] != "N"
            and react_dict["process"].split(",")[1] == "N"
        ):
            return 4

        else:
            return (
                int(sf3_dict[react_dict["process"].split(",")[1]]["mt"])
                if react_dict["process"]
                and sf3_dict.get(react_dict["process"].split(",")[1])
                else None
            )


def e_lvl_to_mt(level_num, process):
    ## This is definition of outgoing particle
    ## N,INL = 4
    ## N,G = 102
    ## N,P = 103  --> N,P or P,P to the excitation states are MT=600-649
    inc_part, out_part = process.split(",")

    if not out_part in sf3_dict.keys():
        return None
    
    if out_part == "INL":
        out_part = inc_part


    if level_num is None:
        return None 

    elif not out_part in mt_range:
        return None

    elif int(level_num) < len(mt_range[out_part]):
        return int(mt_range[out_part][int(level_num)])

    else:
        return max(mt_range[out_part])