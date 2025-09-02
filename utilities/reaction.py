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
## SF3 in EXFOR reaction code mapping to MT number
## temp = {    "0": {"mt": None, "reaction": "resonance energy", "sf5-8": None},
# "SCT": {"mt": None, "reaction": "elastic scattering plus inelastic scattering", "sf5-8": None},
# "X": {"mt": None, "reaction": "Production cross section", "sf5-8": None},
# "N": {"mt": "2", "reaction": "(n,inelas.)", "sf5-8": "SIG"},}

exfor_sf3_dict = {

}


sf3_dict = {
    "0": {"mt": None, "reaction": "resonance energy", "sf5-8": None, "endf": False},
    "SCT": {"mt": None, "reaction": "elastic scattering plus inelastic scattering", "sf5-8": None, "endf": False},
    "X": {"mt": None, "reaction": "Production cross section", "sf5-8": None, "endf": False},
    "N": {"mt": "2", "reaction": "(n,inelas.)", "sf5-8": "SIG", "endf": False},
    "TOT": {"mt": "1", "reaction": "(n,total)", "sf5-8": None, "endf": True},
    "EL": {"mt": "2", "reaction": "(n,elas.)", "sf5-8": None, "endf": True},
    "NON": {"mt": "3", "reaction": "(n,nonelas.)", "sf5-8": None, "endf": True},
    "INL": {"mt": "4", "reaction": "(n,inelas.)", "sf5-8": "SIG", "endf": True},
    "F": {"mt": "18", "reaction": "(n,f)", "sf5-8": None, "endf": True},
    "G": {"mt": "102", "reaction": "(n,g)", "sf5-8": None, "endf": True},
    "P": {"mt": "103", "reaction": "(n,p)", "sf5-8": None, "endf": True},
    "D": {"mt": "104", "reaction": "(n,d)", "sf5-8": None, "endf": True},
    "T": {"mt": "105", "reaction": "(n,t)", "sf5-8": None, "endf": True},
    "HE3": {"mt": "106", "reaction": "(n,h)", "sf5-8": None, "endf": True},
    "A": {"mt": "107", "reaction": "(n,a)", "sf5-8": None, "endf": True},
    "ABS": {"mt": "27", "reaction": "(n,abs)", "sf5-8": None, "endf": True},
    "2N": {"mt": "16", "reaction": "(n,2n)", "sf5-8": None, "endf": True},
    "3N": {"mt": "17", "reaction": "(n,3n)", "sf5-8": None, "endf": True},
    "FIS": {"mt": "19", "reaction": "(n,0f)", "sf5-8": "first", "endf": True},
    "N+F": {"mt": "20", "reaction": "(n,nf)", "sf5-8": "2nd", "endf": True},
    "2N+F": {"mt": "21", "reaction": "(n,2nf)", "sf5-8": "3rd", "endf": True},
    "3N+F": {"mt": "38", "reaction": "(n,3nf)", "sf5-8": "4th", "endf": True},
    "X+N": {"mt": "201", "reaction": "(n,Xn)", "sf5-8": None, "endf": True},
    "X+G": {"mt": "202", "reaction": "(n,Xg)", "sf5-8": None, "endf": True},
    "X+P": {"mt": "203", "reaction": "(n,Xp)", "sf5-8": None, "endf": True},
    "X+D": {"mt": "205", "reaction": "(n,Xt)", "sf5-8": None, "endf": True},
    "X+HE3": {"mt": "206", "reaction": "(n,Xh)", "sf5-8": None, "endf": True},
    "X+A": {"mt": "207", "reaction": "(n,Xa)", "sf5-8": None, "endf": True},
    "N+A": {"mt": "22", "reaction": "(n,na)", "sf5-8": None, "endf": True},
    "N+3A": {"mt": "23", "reaction": "(n,n3a)", "sf5-8": None, "endf": True},
    "N+P": {"mt": "28", "reaction": "(n,np)", "sf5-8": None, "endf": True},
    "N+2A": {"mt": "29", "reaction": "(n,n2a)", "sf5-8": None, "endf": True},
    "N+D": {"mt": "32", "reaction": "(n,nd)", "sf5-8": None, "endf": True},
    "N+T": {"mt": "33", "reaction": "(n,nt)", "sf5-8": None, "endf": True},
    "N+H": {"mt": "34", "reaction": "(n,nh)", "sf5-8": None, "endf": True},
    "N+D+2A": {"mt": "35", "reaction": "(n,nd2a)", "sf5-8": None, "endf": True},
    "N+T+2A": {"mt": "36", "reaction": "(n,nt2a)", "sf5-8": None, "endf": True},
    "N+2P": {"mt": "44", "reaction": "(n,n2p)", "sf5-8": None, "endf": True},
    "N+P+A": {"mt": "45", "reaction": "(n,npa)", "sf5-8": None, "endf": True},
    "2N+A": {"mt": "24", "reaction": "(n,2na)", "sf5-8": None, "endf": True},
    "2N+D": {"mt": "11", "reaction": "(n,2nd)", "sf5-8": None, "endf": True},
    "2N+2A": {"mt": "30", "reaction": "(n,2n2a)", "sf5-8": None, "endf": True},
    "2N+P": {"mt": "41", "reaction": "(n,2np)", "sf5-8": None, "endf": True},
    "2N+T": {"mt": "154", "reaction": "(n,2nt)", "sf5-8": None, "endf": True},
    "2N+2P": {"mt": "190", "reaction": "(n,2n2p)", "sf5-8": None, "endf": True},
    "2N+P+A": {"mt": "159", "reaction": "(n,2npa)", "sf5-8": None, "endf": True},
    "3N+A": {"mt": "25", "reaction": "(n,3na)", "sf5-8": None, "endf": True},
    "3N+P": {"mt": "42", "reaction": "(n,3np)", "sf5-8": None, "endf": True},
    "3N+D": {"mt": "157", "reaction": "(n,3nd)", "sf5-8": None, "endf": True},
    "2A": {"mt": "108", "reaction": "(n,2a)", "sf5-8": None, "endf": True},
    "3A": {"mt": "109", "reaction": "(n,3a)", "sf5-8": None, "endf": True},
    "2P": {"mt": "111", "reaction": "(n,2p)", "sf5-8": None, "endf": True},
    "P+A": {"mt": "112", "reaction": "(n,pa)", "sf5-8": None, "endf": True},
    "T+2A": {"mt": "113", "reaction": "(n,t2a)", "sf5-8": None, "endf": True},
    "D+2A": {"mt": "114", "reaction": "(n,d2a)", "sf5-8": None, "endf": True},
    "P+D": {"mt": "115", "reaction": "(n,pd)", "sf5-8": None, "endf": True},
    "P+T": {"mt": "116", "reaction": "(n,pt)", "sf5-8": None, "endf": True},
    "D+A": {"mt": "117", "reaction": "(n,da)", "sf5-8": None, "endf": True},
    "4N": {"mt": "37", "reaction": "(n,4n)", "sf5-8": None, "endf": True},
    "4N+P": {"mt": "156", "reaction": "(n,4np)", "sf5-8": None, "endf": True},
    "5N": {"mt": "152", "reaction": "(n,5n)", "sf5-8": None, "endf": True},
    "6N": {"mt": "153", "reaction": "(n,6n)", "sf5-8": None, "endf": True},
    "T+A": {"mt": "155", "reaction": "(n,ta)", "sf5-8": None, "endf": True},
    "N+D+A": {"mt": "158", "reaction": "(n,n'da)", "sf5-8": None, "endf": True},
    "7N": {"mt": "160", "reaction": "(n,7n)", "sf5-8": None, "endf": True},
    "8N": {"mt": "161", "reaction": "(n,8n)", "sf5-8": None, "endf": True},
    "5N+P": {"mt": "162", "reaction": "(n,5np)", "sf5-8": None, "endf": True},
    "6N+P": {"mt": "163", "reaction": "(n,6np)", "sf5-8": None, "endf": True},
    "7N+P": {"mt": "164", "reaction": "(n,7np)", "sf5-8": None, "endf": True},
    "4N+A": {"mt": "165", "reaction": "(n,4na)", "sf5-8": None, "endf": True},
    "5N+A": {"mt": "166", "reaction": "(n,5na)", "sf5-8": None, "endf": True},
    "6N+A": {"mt": "167", "reaction": "(n,6na)", "sf5-8": None, "endf": True},
    "7N+A": {"mt": "168", "reaction": "(n,7na)", "sf5-8": None, "endf": True},
    "4N+D": {"mt": "169", "reaction": "(n,4nd)", "sf5-8": None, "endf": True},
    "5N+D": {"mt": "170", "reaction": "(n,5nd)", "sf5-8": None, "endf": True},
    "6N+D": {"mt": "171", "reaction": "(n,6nd)", "sf5-8": None, "endf": True},
    "3N+T": {"mt": "172", "reaction": "(n,3nt)", "sf5-8": None, "endf": True},
    "4N+T": {"mt": "173", "reaction": "(n,4nt)", "sf5-8": None, "endf": True},
    "5N+T": {"mt": "174", "reaction": "(n,5nt)", "sf5-8": None, "endf": True},
    "6N+T": {"mt": "175", "reaction": "(n,6nt)", "sf5-8": None, "endf": True},
    "2N+HE3": {"mt": "176", "reaction": "(n,2nh)", "sf5-8": None, "endf": True},
    "3N+HE3": {"mt": "177", "reaction": "(n,3nh)", "sf5-8": None, "endf": True},
    "4N+HE3": {"mt": "178", "reaction": "(n,4nh)", "sf5-8": None, "endf": True},
    "3N+2P": {"mt": "179", "reaction": "(n,3n2p)", "sf5-8": None, "endf": True},
    "3N+2": {"mt": "180", "reaction": "(n,3n2a)", "sf5-8": None, "endf": True},
    "3N+P+A": {"mt": "181", "reaction": "(n,3npa)", "sf5-8": None, "endf": True},
    "D+T": {"mt": "182", "reaction": "(n,dt)", "sf5-8": None, "endf": True},
    "N+P+D": {"mt": "183", "reaction": "(n,n'pd)", "sf5-8": None, "endf": True},
    "N+P+T": {"mt": "184", "reaction": "(n,n'pt)", "sf5-8": None, "endf": True},
    "N+D+T": {"mt": "185", "reaction": "(n,n'dt)", "sf5-8": None, "endf": True},
    "N+P+HE3": {"mt": "186", "reaction": "(n,n'ph)", "sf5-8": None, "endf": True},
    "N+D+HE3": {"mt": "187", "reaction": "(n,n'dh)", "sf5-8": None, "endf": True},
    "N+T+HE3": {"mt": "188", "reaction": "(n,n'th)", "sf5-8": None, "endf": True},
    "N+T+A": {"mt": "189", "reaction": "(n,n'ta)", "sf5-8": None, "endf": True},
    "P+HE3": {"mt": "191", "reaction": "(n,ph)", "sf5-8": None, "endf": True},
    "D+HE": {"mt": "192", "reaction": "(n,dh)", "sf5-8": None, "endf": True},
    "HE+A": {"mt": "193", "reaction": "(n,ha)", "sf5-8": None, "endf": True},
    "4N+2P": {"mt": "194", "reaction": "(n,4n2p)", "sf5-8": None, "endf": True},
    "4N+2A": {"mt": "195", "reaction": "(n,4n2a)", "sf5-8": None, "endf": True},
    "4N+P+A": {"mt": "196", "reaction": "(n,4npa)", "sf5-8": None, "endf": True},
    "3P": {"mt": "197", "reaction": "(n,3p)", "sf5-8": None, "endf": True},
    "N+3P": {"mt": "198", "reaction": "(n,n'3p)", "sf5-8": None, "endf": True},
    "3N+2P+A": {"mt": "199", "reaction": "(n,3n2pa)", "sf5-8": None, "endf": True},
    "5N+2P": {"mt": "200", "reaction": "(n,5n2p)", "sf5-8": None, "endf": True},
}


mt_range = {
    "N": list(range(50, 93)),  # up to 92
    "P": list(range(600, 650)),  # up to 649
    "D": list(range(650, 700)),  # up to 699
    "T": list(range(700, 750)),  # up to 749
    "H": list(range(750, 800)),  # up to 799
    "A": list(range(800, 850)),  # up to 849
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
    # "451": "Descriptive Data and Directory",
    # "452": "nu_bar_t average total number of neutrons",
    # "454": "Independent fission product yield data",
    # "455": "nu_bar_d average number of delayed neutrons",
    # "456": "nu_bar_p average number of prompt neutron",
    # "457": "Radioactive decay data",
    # "458": "Energy release in fission for incident neutrons",
    # "459": "Cumulative fission product yield data",
    # "460": "Delayed Photon Data",
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


sf6_to_mf = {
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
    print(projectile)
    all_reactions = {
        (
            "N"
            if (projectile.upper() != "N" and k == "INL" and i.get("endf"))
            else k
        ): i
        for k, i in sf3_dict.items()
        if i.get("endf")  # ★ ここで endf=True のみにフィルタ
    }

    partial = {}

    for p in mt_range.keys():
        for n, mt in enumerate(mt_range[p.upper()]):
            partial[f"{p.upper()}{n}"] = {
                "mt": str(mt),
                "reaction": f"({projectile.lower()},{p.lower()}{n})",
                "sf5-8": "PAR,SIG,,",
                "endf": True,
            }

    return dict(**all_reactions, **partial) 


def exfor_reaction_list(projectile):
    if not projectile:
        return sf3_dict

    assert len(projectile) == 1

    all = sf3_dict

    all = {
        "N" if projectile.upper() != "N" and k == "INL" else k: i
        for k, i in all.items()
    }

    return all


def get_mf(reaction):
    # dict ならキーから取り出す
    if isinstance(reaction, dict):
        sf6 = reaction.get("sf6")
        sf4 = reaction.get("sf4")
    else:
        # 文字列は "sf6,sf4" の形式を想定
        parts = reaction.split(",")
        sf6 = parts[0].strip()
        sf4 = parts[1].strip() if len(parts) > 1 else None

    if sf6 in sf6_to_mf:
        if sf6 == "NU":
            return int(sf6_to_mf[sf6])
        elif sf4 == "0-G-0":
            return 12  # Multiplicity of photon production
        else:
            return int(sf6_to_mf[sf6])
    else:
        return 9999


def get_mt(reaction):
    # exforparser send reaction as react_dict
    if isinstance(reaction, dict):
        reaction = reaction["process"]

    parts = reaction.split(",")
    particle = parts[0].upper()
    if particle == "HE3":
        particle = "H"

    try:
        reactions = reaction_list(particle)
        return reactions[parts[1].upper()]["mt"]
    except Exception:
        return None


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

        for p in mt_range.keys():
            for n in range(len(mt_range[p.upper()])):
                partial[str(mt_range[p.upper()][n])] = (
                    f"{projectile.lower()},{p.lower()}{str(n)}"
                )
    # print(all, partial)
    return dict(**all, **partial)



def get_mt_non_xs(react_dict):

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


def e_lvl_to_mt(level_num, process):
    ## This is a definition of outgoing particle
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


def mt_to_process(projectile, type, mt):
    if type == "residual":
        return "x"

    if mt and int(mt) == 4 and projectile == "n":
        return "inelastic"

    elif mt and int(mt) == 4 and projectile != "n":
        return "n"

    elif mt and mt_list.get(str(int(mt))):
        return mt_list[str(int(mt))]

    else:
        return mt_to_discretelevel(mt)


def mt_to_discretelevel(mt):
    for outpart, range in mt_range.items():
        if int(mt) in range:
            index = range.index(int(mt))
            return f"{outpart.lower()}_{str(index)}"


def full_mf3_mt_numbers():

    full_list = []
    full_list += list(mt_list.keys())
    for _, l in mt_range.items():

        full_list += l

    return sorted([int(mt) for mt in full_list])
