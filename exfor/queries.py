import sys
import importlib
import pandas as pd
from collections import OrderedDict
from operator import getitem
from sqlalchemy import func


try:
    from config import  engines, session
except:
    module_name =  sys.modules[__name__].split(".")[0]
    config = importlib.import_module(f"{module_name}.config")
    from config import engines, session

from exforparser.sql.models import Exfor_Bib, Exfor_Reactions, Exfor_Data, Exfor_Indexes

from ..utilities.util import elemtoz_nz
from ..utilities.util import (
    get_number_from_string,
    get_str_from_string,
    x4style_nuclide_expression,
)
from ..utilities.reaction import convert_partial_reactionstr_to_inl
from ..utilities.reaction import (
    convert_reaction_to_exfor_style,
    convert_partial_reactionstr_to_inl,
)

connection = engines["exfor"].connect()


def get_exfor_bib_table():
    df = pd.read_sql_table("exfor_bib", connection)
    return df

def get_exfor_reference_table():
    df = pd.read_sql_table("exfor_reference", connection)
    return df

def get_exfor_experimental_condition_table():
    df = pd.read_sql_table("exfor_experimental_condition", connection)
    return df



def get_exfor_reactions_table():
    df = pd.read_sql_table("exfor_reactions", connection)
    return df


def get_exfor_indexes_table():
    df = pd.read_sql_table("exfor_index", connection)
    return df


########  -------------------------------------------- ##########
##  EXFOR entry queries for the dataexplorer/api/exfor/search  ##
########  -------------------------------------------- ##########
def entries_query(**kwargs):
    # https://zenn.dev/shimakaze_soft/articles/6e5e47851459f5
    # connection = engines["exfor"].connect()
    queries = []

    types = kwargs.get("types")
    elem = kwargs.get("target_elem")
    mass = kwargs.get("target_mass")
    inc_pt = kwargs.get("inc_pt")
    reaction = kwargs.get("reaction")

    facilities = kwargs.get("facilities")
    facility_types = kwargs.get("facility_types")

    first_author = kwargs.get("first_author")
    authors = kwargs.get("authors")

    # rp_elem = kwargs.get("rp_elem")
    # rp_mass = kwargs.get("rp_mass")

    ## From reaction code
    sf5 = kwargs.get("sf5")  # SF6
    sf4 = kwargs.get("sf4")  # SF6
    sf7 = kwargs.get("sf7")  # SF6
    sf8 = kwargs.get("sf8")  # SF6

    if types:
        queries.append(Exfor_Reactions.sf6.in_(tuple(types)))

    if elem and not mass:
        queries.append(
            Exfor_Reactions.target.like(f"%{elemtoz_nz(elem)}-{elem.upper()}-%")
        )

    if elem and mass:
        target = x4style_nuclide_expression(elem, mass)
        queries.append(Exfor_Reactions.target == target)

    if inc_pt and not reaction:
        queries.append(Exfor_Reactions.projectile == inc_pt.upper())

    if reaction:
        rr = []
        for r in reaction:
            rr += [r.upper()]

        reactions_exfor_format = list(dict.fromkeys(rr))
        queries.append(Exfor_Reactions.process.in_(tuple(reactions_exfor_format)))

    if first_author:
        queries.append(Exfor_Bib.first_author.like(f"%{first_author.capitalize()}%"))

    if authors:
        queries.append(Exfor_Bib.authors.like(f"%{authors.capitalize()}%"))

    if sf4:
        queries.append(Exfor_Reactions.sf4 == sf4.upper())

    if facilities:
        facilities = [f"({fa})" for fa in facilities]
        queries.append(Exfor_Bib.main_facility_institute.in_(tuple(facilities)))

    if facility_types:
        facility_types = [f"({fa})" for fa in facility_types]
        queries.append(Exfor_Bib.main_facility_type.in_(tuple(facility_types)))

    # followings must have been None unless it is specified
    if sf5:
        queries.append(Exfor_Reactions.sf5.in_(sf5))

    if sf7:
        queries.append(Exfor_Reactions.sf7 == sf7.upper())

    if sf8:
        queries.append(Exfor_Reactions.sf8.in_(sf8))

    reac = (
        session()
        .query(
            # Exfor_Reactions, Exfor_Bib
            Exfor_Reactions.entry,
            Exfor_Reactions.entry_id,
            Exfor_Reactions.target,
            Exfor_Reactions.projectile,
            Exfor_Reactions.process,
            Exfor_Reactions.sf4,
            # Exfor_Reactions.residual,
            # Exfor_Reactions.level_num,
            Exfor_Reactions.sf5,
            Exfor_Reactions.sf6,
            Exfor_Reactions.sf7,
            Exfor_Reactions.sf8,
            Exfor_Reactions.x4_code,
            Exfor_Bib.first_author,
            Exfor_Bib.authors,
            Exfor_Bib.year,
            Exfor_Bib.main_reference,
            Exfor_Bib.main_doi,
            Exfor_Bib.main_facility_institute,
            Exfor_Bib.main_facility_type,
            func.min(Exfor_Indexes.e_inc_min).label("e_inc_min"),
            func.max(Exfor_Indexes.e_inc_max).label("e_inc_max"),
        )
        .join(Exfor_Bib, Exfor_Reactions.entry == Exfor_Bib.entry, isouter=True)
        .join(
            Exfor_Indexes,
            Exfor_Indexes.entry_id == Exfor_Reactions.entry_id,
            isouter=True,
        )
        .group_by(Exfor_Reactions.entry_id)
        .order_by(Exfor_Bib.year.desc())
        .filter(*queries)
    )

    df = pd.read_sql(
        sql=reac.statement,
        con=connection,
    )

    return df


def facility_query(facility_code, facility_type):
    # https://zenn.dev/shimakaze_soft/articles/6e5e47851459f5
    # connection = engines["exfor"].connect()

    queries = [
        Exfor_Indexes.main_facility_institute == facility_code,
        Exfor_Indexes.main_facility_type == facility_type.upper(),
    ]

    reac = (
        session()
        .query(Exfor_Indexes, Exfor_Bib)
        .filter(*queries)
        .join(Exfor_Bib, Exfor_Indexes.entry == Exfor_Bib.entry, isouter=True)
        .distinct()
    )

    df = pd.read_sql(
        sql=reac.statement,
        con=connection,
    )

    return df


########  -------------------------------------- ##########
##         Reaction queries for the dataexplorer
########  -------------------------------------- ##########
def index_query(input_store) -> dict:

    type = input_store.get("type").upper()
    elem = input_store.get("target_elem")
    mass = input_store.get("target_mass")
    reaction = convert_reaction_to_exfor_style(input_store.get("reaction"))

    target = x4style_nuclide_expression(elem, mass)
    queries = [Exfor_Indexes.target == target, Exfor_Indexes.arbitrary_data == False]

    if type == "FY":
        branch = input_store.get("branch")
        level_num = input_store.get("level_num")
        reac_product_fy = input_store.get("reac_product_fy")
        mesurement_opt_fy = input_store.get("mesurement_opt_fy")

        if branch:
            queries.append(
                Exfor_Indexes.sf5 == branch.upper()
                if type != "FY"
                else Exfor_Indexes.sf5.in_(tuple(fy_branch(branch.upper())))
            )
        elif isinstance(level_num, int):
            reaction = convert_partial_reactionstr_to_inl(reaction)
            queries.extend(
                [Exfor_Indexes.sf5 == "PAR", Exfor_Indexes.level_num == level_num]
            )
        elif input_store.get("excl_junk_switch"):
            queries.append(Exfor_Indexes.sf5 == None)

        queries.append(
            Exfor_Indexes.sf4 == "MASS"
            if mesurement_opt_fy == "A"
            else Exfor_Indexes.sf4 == "ELEM"
            if mesurement_opt_fy == "Z"
            else Exfor_Indexes.sf4.isnot(None)
        )

        if reac_product_fy:
            queries.append(Exfor_Indexes.residual.in_(reac_product_fy))

    else:
        branch = input_store.get("branch")
        rp_elem = input_store.get("rp_elem")
        rp_mass = input_store.get("rp_mass")
        level_num = input_store.get("level_num")  # Moved this line

        if branch:
            queries.append(
                Exfor_Indexes.sf5 == branch.upper()
                if type != "FY"
                else Exfor_Indexes.sf5.in_(tuple(fy_branch(branch.upper())))
            )
        elif isinstance(level_num, int):
            reaction = convert_partial_reactionstr_to_inl(reaction)
            queries.extend(
                [Exfor_Indexes.sf5 == "PAR", Exfor_Indexes.level_num == level_num]
            )
        elif input_store.get("excl_junk_switch") or not branch:
            queries.append(Exfor_Indexes.sf5 == None)

        if type == "RP":
            reaction = reaction.split(",")[0].upper()
            rp_mass = (
                rp_elem.capitalize()
                + "-"
                + str(get_number_from_string(rp_mass))
                + "-"
                + get_str_from_string(str(rp_mass)).upper()
                if rp_mass.endswith(("m", "M", "g", "G", "L", "M1", "M2", "m1", "m2"))
                else rp_elem.capitalize() + "-" + str(rp_mass)
            )
            queries.extend(
                [
                    Exfor_Indexes.projectile == reaction,
                    Exfor_Indexes.residual == rp_mass,
                ]
            )
        else:
            queries.append(
                Exfor_Indexes.process == reaction.replace("total", "tot").upper()
            )
            if not any(r in reaction for r in ("tot", "f")):
                queries.extend(
                    [
                        ~Exfor_Indexes.sf4.endswith(f"-{suffix}")
                        for suffix in ("G", "M", "L", "M1", "M2")
                    ]
                )

    if input_store.get("excl_junk_switch"):
        queries.extend([Exfor_Indexes.sf7 == None, Exfor_Indexes.sf8 == None, Exfor_Indexes.sf9 == None])


    type_map = {"XS": "SIG", "TH": "SIG", "RP": "SIG", "FY": "FY"}
    type = type_map.get(input_store.get("type").upper(), "SIG")

    queries.extend([Exfor_Indexes.sf6 == type.upper()])
    reac = session().query(Exfor_Indexes).filter(*queries).all()

    entries = (
        {
            ent.entry_id: {
                "level_num": ent.level_num,
                "e_inc_min": ent.e_inc_min,
                "e_inc_max": ent.e_inc_max,
                "points": ent.points,
                "x4_code": ent.x4_code,
                "sf4": ent.sf4,
                "sf5": ent.sf5,
                "sf6": ent.sf6,
                "sf7": ent.sf7,
                "sf8": ent.sf8,
                "sf9": ent.sf9,
                "mt": ent.mt,
                "mf": ent.mf,
            }
            for ent in reac
        }
        if reac
        else {}
    )

    return entries


def get_entry_bib(entries):
    bib = session().query(Exfor_Bib).filter(Exfor_Bib.entry.in_(tuple(entries))).all()

    legend = {}

    for b in bib:
        legend[b.entry] = {
            "author": b.first_author,
            "year": b.year if b.year else 1900,  ## Should be int in SQL
        }

    return OrderedDict(
        sorted(legend.items(), key=lambda x: getitem(x[1], "year"), reverse=True),
    )


def entry_query_by_id(entries):
    queries = [Exfor_Bib.entry.in_(tuple(entries))]
    # bib = session().query(Exfor_Indexes).filter().all()

    indexes = session().query(Exfor_Bib).filter(*queries)
    df = pd.read_sql(
        sql=indexes.statement,
        con=connection,
    )

    return df


def reaction_query_by_id(entries):
    queries = [Exfor_Reactions.entry.in_(tuple(entries))]
    # bib = session().query(Exfor_Indexes).filter().all()

    indexes = session().query(Exfor_Reactions).filter(*queries)
    df = pd.read_sql(
        sql=indexes.statement,
        con=connection,
    )

    return df


def index_query_by_id(entries):
    queries = [Exfor_Indexes.entry.in_(tuple(entries))]
    # bib = session().query(Exfor_Indexes).filter().all()

    indexes = session().query(Exfor_Indexes).filter(*queries)
    df = pd.read_sql(
        sql=indexes.statement,
        con=connection,
    )

    return df




def data_query(input_store, entids):
    # connection = engines["exfor"].connect()

    type = input_store.get("type").upper()
    level_num = input_store.get("level_num")

    if type.upper() == "XS":
        type = "SIG"

    queries = [Exfor_Data.entry_id.in_(tuple(entids))]

    if level_num:
        queries.append(Exfor_Data.level_num == level_num)

    # data = session().query(Exfor_Data).filter(*queries)
    if type.upper() == "SIG":
        data = (
            session()
            .query(
                Exfor_Data.entry_id,
                Exfor_Data.en_inc,
                Exfor_Data.den_inc,
                Exfor_Data.level_num,
                Exfor_Data.residual,
                Exfor_Data.data,
                Exfor_Data.ddata,
            )
            .filter(*queries)
        )
    elif type.upper() == "RP":
        data = (
            session()
            .query(
                Exfor_Data.entry_id,
                Exfor_Data.en_inc,
                Exfor_Data.den_inc,
                Exfor_Data.residual,
                Exfor_Data.data,
                Exfor_Data.ddata,
            )
            .filter(*queries)
        )
    elif type.upper() == "FY":
        data = (
            session()
            .query(
                Exfor_Data.entry_id,
                Exfor_Data.en_inc,
                Exfor_Data.den_inc,
                Exfor_Data.charge,
                Exfor_Data.mass,
                Exfor_Data.isomer,
                Exfor_Data.residual,
                Exfor_Data.data,
                Exfor_Data.ddata,
            )
            .filter(*queries)
        )
    elif type.upper() == "DA":
        data = (
            session()
            .query(
                Exfor_Data.entry_id,
                Exfor_Data.en_inc,
                Exfor_Data.den_inc,
                Exfor_Data.angle,
                Exfor_Data.dangle,
                Exfor_Data.data,
                Exfor_Data.ddata,
            )
            .filter(*queries)
        )
    elif type.upper() == "DE":
        data = (
            session()
            .query(
                Exfor_Data.entry_id,
                Exfor_Data.en_inc,
                Exfor_Data.den_inc,
                Exfor_Data.e_out,
                Exfor_Data.de_out,
                Exfor_Data.data,
                Exfor_Data.ddata,
            )
            .filter(*queries)
        )
    elif type.upper() == "TH":
        """
        to limit the data near the thermal energy
        """
        queries.append(Exfor_Data.en_inc >= 2.52E-8)
        queries.append(Exfor_Data.en_inc <= 2.54E-8)
        data = (
            session()
            .query(
                Exfor_Data.entry_id,
                Exfor_Data.en_inc,
                Exfor_Data.den_inc,
                Exfor_Data.data,
                Exfor_Data.ddata,
            )
            .filter(*queries)
        )
    else:
        data = session().query(Exfor_Data).filter(*queries)

    df = pd.read_sql(
        sql=data.statement,
        con=connection,
    )

    return df


######## -------------------------------------- ########
#    Queries for FY
######## -------------------------------------- ########
def fy_branch(branch):
    if branch == "PRE":
        return ["PRE", "TER", "QTR", "PRV", "TER/CHG"]

    elif branch == "IND":
        return ["IND", "SEC", "MAS", "CHG", "SEC/CHN"]

    elif branch == "CUM":
        return ["CUM", "CHN"]

    else:
        return [branch]


def index_query_fission(type, elem, mass, reaction, branch, lower, upper):
    # https://zenn.dev/shimakaze_soft/articles/6e5e47851459f5
    sf4 = None
    sf5 = None
    sf6 = None
    target = x4style_nuclide_expression(elem, mass)

    queries = [
        Exfor_Indexes.target == target,
        Exfor_Indexes.process == reaction.upper(),
        Exfor_Indexes.arbitrary_data == False,
    ]

    if branch == "nu_n":
        sf5 = ["PR"]
        sf6 = ["NU"]
    elif branch == "nu_g":
        sf4 = "0-G-0"
        sf5 = ["PR"]
        sf6 = ["FY"]
    elif branch == "dn":
        sf5 = ["DL"]
        sf6 = ["NU"]
    elif branch == "pfns":
        sf5 = ["PR"]
        sf6 = ["NU/DE"]
    elif branch == "pfgs":
        sf4 = "0-G-0"
        sf5 = ["PR"]
        sf6 = ["FY/DE"]
    else:
        ## to avoid large query
        return None, None

    if sf4:
        queries.append(Exfor_Indexes.sf4 == sf4)

    if sf5:
        queries.append(Exfor_Indexes.sf5.in_(tuple(sf5)))

    if sf6:
        queries.append(Exfor_Indexes.sf6.in_(tuple(sf6)))

    if lower and upper:
        # lower, upper = energy_range_conversion(energy_range)
        queries.append(Exfor_Indexes.e_inc_min >= lower)
        queries.append(Exfor_Indexes.e_inc_max <= upper)

    reac = session().query(Exfor_Indexes).filter(*queries).all()

    entids = {}
    entries = []

    for ent in reac:
        entids[ent.entry_id] = {
            "e_inc_min": ent.e_inc_min,
            "e_inc_max": ent.e_inc_max,
            "points": ent.points,
            "sf5": ent.sf5,
            "sf8": ent.sf8,
            "x4_code": ent.x4_code,
        }
        entries += [ent.entry]

    return entids, entries


########  -------------------------------------- ##########
##         Join table for AGGrid
########  -------------------------------------- ##########
def join_reaction_bib():
    # connection = engines["exfor"].connect()
    all = (
        session()
        .query(
            # Exfor_Reactions
            Exfor_Reactions.entry,
            Exfor_Reactions.entry_id,
            Exfor_Reactions.target,
            Exfor_Reactions.projectile,
            Exfor_Reactions.process,
            Exfor_Reactions.sf4,
            Exfor_Reactions.sf6,
            Exfor_Bib.first_author,
            Exfor_Bib.first_author_institute,
            Exfor_Bib.title,
            Exfor_Bib.main_reference,
            Exfor_Bib.main_doi,
            Exfor_Bib.authors,
            Exfor_Bib.year,
            Exfor_Bib.main_facility_institute,
            Exfor_Bib.main_facility_type,
            func.min(Exfor_Indexes.e_inc_min).label("e_inc_min"),
            func.max(Exfor_Indexes.e_inc_max).label("e_inc_max"),
            # Exfor_Indexes.e_inc_min
        )
        .join(
            Exfor_Bib,
            Exfor_Reactions.entry == Exfor_Bib.entry,
        )
        .join(
            Exfor_Indexes,
            Exfor_Indexes.entry_id == Exfor_Reactions.entry_id,
        )
        .group_by(Exfor_Reactions.entry_id)
        .order_by(Exfor_Bib.year.desc())
    )

    df = pd.read_sql(
        sql=all.statement,
        con=connection,
    )

    return df


def join_index_bib():
    # connection = engines["exfor"].connect()
    all = (
        session()
        .query(
            Exfor_Indexes.entry_id,
            Exfor_Indexes.target,
            Exfor_Indexes.process,
            Exfor_Indexes.residual,
            Exfor_Indexes.e_inc_min,
            Exfor_Indexes.e_inc_max,
            Exfor_Indexes.sf5,
            Exfor_Indexes.sf6,
            Exfor_Indexes.sf7,
            Exfor_Indexes.sf8,
            Exfor_Bib.entry,
            Exfor_Bib.authors,
            Exfor_Bib.year,
            Exfor_Bib.main_facility_institute,
            Exfor_Bib.main_facility_type,
        )
        .join(Exfor_Bib, Exfor_Bib.entry == Exfor_Indexes.entry)
        .order_by(Exfor_Bib.year.desc())
    )

    df = pd.read_sql(
        sql=all.statement,
        con=connection,
    )

    return df