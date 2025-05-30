import sys
import importlib
import pandas as pd
from collections import OrderedDict
from operator import getitem
from sqlalchemy import select, and_, not_, func
from sqlalchemy.dialects import sqlite

try:
    from config import engines, session
except:
    module_name = sys.modules[__name__].split(".")[0]
    config = importlib.import_module(f"{module_name}.config")
    from config import engines, session

from exforparser.sql.models import Exfor_Bib, Exfor_Reactions, Exfor_Data, Exfor_Indexes
from exforparser.sql.models_core import (
    exfor_bib,
    exfor_reactions,
    exfor_data,
    exfor_indexes,
)

from ..common import pageparam_to_sf6
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


# connection = engines["exfor"].connect()


def get_exfor_bib_table():
    with engines["exfor"].connect() as connection:
        df = pd.read_sql_table("exfor_bib", connection)
        return df


def get_exfor_reference_table():
    with engines["exfor"].connect() as connection:
        df = pd.read_sql_table("exfor_reference", connection)
        return df


def get_exfor_experimental_condition_table():
    with engines["exfor"].connect() as connection:
        df = pd.read_sql_table("exfor_experimental_condition", connection)
        return df


def get_exfor_reactions_table():
    with engines["exfor"].connect() as connection:
        df = pd.read_sql_table("exfor_reactions", connection)
        return df


def get_exfor_indexes_table():
    with engines["exfor"].connect() as connection:
        df = pd.read_sql_table("exfor_index", connection)
        return df


########  -------------------------------------------- ##########
##  EXFOR entry queries for the dataexplorer/api/exfor/search  ##
########  -------------------------------------------- ##########
def entries_query(**kwargs):
    queries = []

    # 条件の取得
    types = kwargs.get("types")
    elem = kwargs.get("target_elem")
    mass = kwargs.get("target_mass")
    inc_pt = kwargs.get("inc_pt")
    reaction = kwargs.get("reaction")

    facilities = kwargs.get("facilities")
    facility_types = kwargs.get("facility_types")

    first_author = kwargs.get("first_author")
    authors = kwargs.get("authors")

    sf5 = kwargs.get("sf5")
    sf4 = kwargs.get("sf4")
    sf7 = kwargs.get("sf7")
    sf8 = kwargs.get("sf8")

    # 条件を動的に組み立て
    if types:
        queries.append(exfor_reactions.c.sf6.in_(types))

    if elem and not mass:
        queries.append(
            exfor_reactions.c.target.like(f"%{elemtoz_nz(elem)}-{elem.upper()}-%")
        )

    if elem and mass:
        target = x4style_nuclide_expression(elem, mass)
        queries.append(exfor_reactions.c.target == target)

    if inc_pt and not reaction:
        queries.append(exfor_reactions.c.projectile == inc_pt.upper())

    if reaction:
        reactions_exfor_format = [r.upper() for r in reaction]
        queries.append(exfor_reactions.c.process.in_(reactions_exfor_format))

    if first_author:
        queries.append(exfor_bib.c.first_author.like(f"%{first_author.capitalize()}%"))

    if authors:
        queries.append(exfor_bib.c.authors.like(f"%{authors.capitalize()}%"))

    if sf4:
        queries.append(exfor_reactions.c.sf4 == sf4.upper())

    if facilities:
        facilities = [f"({fa})" for fa in facilities]
        queries.append(exfor_bib.c.main_facility_institute.in_(facilities))

    if facility_types:
        facility_types = [f"({fa})" for fa in facility_types]
        queries.append(exfor_bib.c.main_facility_type.in_(facility_types))

    if sf5:
        queries.append(exfor_reactions.c.sf5.in_(sf5))

    if sf7:
        queries.append(exfor_reactions.c.sf7 == sf7.upper())

    if sf8:
        queries.append(exfor_reactions.c.sf8.in_(sf8))

    # SELECT クエリ構築
    stmt = (
        select(
            exfor_reactions.c.entry,
            exfor_reactions.c.entry_id,
            exfor_reactions.c.target,
            exfor_reactions.c.projectile,
            exfor_reactions.c.process,
            exfor_reactions.c.sf4,
            exfor_reactions.c.sf5,
            exfor_reactions.c.sf6,
            exfor_reactions.c.sf7,
            exfor_reactions.c.sf8,
            exfor_reactions.c.x4_code,
            exfor_bib.c.first_author,
            exfor_bib.c.authors,
            exfor_bib.c.year,
            exfor_bib.c.main_reference,
            exfor_bib.c.main_doi,
            exfor_bib.c.main_facility_institute,
            exfor_bib.c.main_facility_type,
            func.min(exfor_indexes.c.e_inc_min).label("e_inc_min"),
            func.max(exfor_indexes.c.e_inc_max).label("e_inc_max"),
        )
        .select_from(
            exfor_reactions.join(
                exfor_bib, exfor_reactions.c.entry == exfor_bib.c.entry, isouter=True
            ).join(
                exfor_indexes,
                exfor_indexes.c.entry_id == exfor_reactions.c.entry_id,
                isouter=True,
            )
        )
        .where(and_(*queries))
        .group_by(exfor_reactions.c.entry_id)
        .order_by(exfor_bib.c.year.desc())
    )

    with engines["exfor"].connect() as conn:
        df = pd.read_sql(stmt, conn)

    return df


def facility_query(facility_code, facility_type):
    # クエリ条件
    queries = [
        exfor_indexes.c.main_facility_institute == facility_code,
        exfor_indexes.c.main_facility_type == facility_type.upper(),
    ]

    # SQLAlchemy Core クエリ構築
    stmt = (
        select(exfor_indexes, exfor_bib)
        .select_from(
            exfor_indexes.join(
                exfor_bib, exfor_indexes.c.entry == exfor_bib.c.entry, isouter=True
            )
        )
        .where(and_(*queries))
        .distinct()
    )

    with engines["exfor"].connect() as conn:
        df = pd.read_sql(stmt, conn)

    return df


########  -------------------------------------- ##########
##         Reaction queries for the dataexplorer
########  -------------------------------------- ##########



def exfor_index_query(input_store) -> dict:
    obs_type = input_store.get("obs_type").upper()
    elem = input_store.get("target_elem")
    mass = input_store.get("target_mass")
    projectile = input_store.get("inc_pt")

    ## Convert into EXFOR expression
    reaction = convert_reaction_to_exfor_style(input_store.get("reaction"))
    target = x4style_nuclide_expression(elem, mass)
    queries = [
        exfor_indexes.c.target == target,
        exfor_indexes.c.arbitrary_data == False,
        exfor_indexes.c.projectile == projectile.upper(),
    ]

    if obs_type in ["XS", "TH"]:
        branch = input_store.get("branch")
        level_num = input_store.get("level_num")

        if branch:
            queries.append(exfor_indexes.c.sf5 == branch.upper())

        elif isinstance(level_num, int):
            ## override reacton 
            reaction = convert_partial_reactionstr_to_inl(reaction)
            queries.extend(
                [exfor_indexes.c.sf5 == "PAR", exfor_indexes.c.level_num == level_num]
            )
        elif input_store.get("excl_junk_switch") or not branch:
            queries.append(exfor_indexes.c.sf5 == None)

        queries.append(exfor_indexes.c.process == reaction.replace("total", "tot").upper())

        if not any(r in reaction for r in ("tot", "f")):
            queries.extend(
                [
                    not_(exfor_indexes.c.sf4.endswith(f"-{suffix}"))
                    for suffix in ("G", "M", "L", "M1", "M2")
                ]
            )

    if obs_type == "RP":
        rp_elem = input_store.get("rp_elem")
        rp_mass = input_store.get("rp_mass")
        # projectile = reaction.split(",")[0].upper()

        # Convert format because the format is Mg-28, Sc-44-M in residual column
        if rp_mass.endswith(("m", "M", "g", "G", "L", "M1", "M2", "m1", "m2")):
            rp_mass_fmt = f"{rp_elem.capitalize()}-{get_number_from_string(rp_mass)}-{get_str_from_string(str(rp_mass)).upper()}"
        else:
            rp_mass_fmt = f"{rp_elem.capitalize()}-{rp_mass}"

        queries.extend(
            [
                exfor_indexes.c.projectile == projectile,
                exfor_indexes.c.residual == rp_mass_fmt,
            ]
        )

    if obs_type == "FY":
        branch = input_store.get("branch")
        reac_product_fy = input_store.get("reac_product_fy")
        mesurement_opt_fy = input_store.get("mesurement_opt_fy")

        if branch:
            queries.append(exfor_indexes.c.sf5.in_(tuple(fy_branch(branch.upper()))))
        elif isinstance(level_num, int):
            reaction = convert_partial_reactionstr_to_inl(reaction)
            queries.extend(
                [exfor_indexes.c.sf5 == "PAR", exfor_indexes.c.level_num == level_num]
            )
        elif input_store.get("excl_junk_switch"):
            queries.append(exfor_indexes.c.sf5 == None)

        queries.append(
            exfor_indexes.c.sf4 == "MASS"
            if mesurement_opt_fy == "A"
            else (
                exfor_indexes.c.sf4 == "ELEM"
                if mesurement_opt_fy == "Z"
                else exfor_indexes.c.sf4.isnot(None)
            )
        )

        if reac_product_fy:
            queries.append(exfor_indexes.c.residual.in_(reac_product_fy))

    if obs_type == "DA":
        level_num = input_store.get("level_num")

        if isinstance(level_num, int):
            ## override reacton 
            reaction = convert_partial_reactionstr_to_inl(reaction)
            queries.extend(
                [exfor_indexes.c.sf5 == "PAR", exfor_indexes.c.level_num == level_num]
            )

    if input_store.get("excl_junk_switch"):
        queries.extend(
            [
                exfor_indexes.c.sf7 == None,
                exfor_indexes.c.sf8 == None,
                exfor_indexes.c.sf9 == None,
            ]
        )


    sf6 = pageparam_to_sf6.get(obs_type, "SIG")

    # Excute query
    queries.append(exfor_indexes.c.sf6 == sf6.upper())
    stmt = select(exfor_indexes).where(and_(*queries))
    print(stmt.compile(dialect=sqlite.dialect(), compile_kwargs={"literal_binds": True}))
    with engines["exfor"].connect() as conn:
        result = conn.execute(stmt).fetchall()

    entries = (
        {
            row.entry_id: {
                "level_num": row.level_num,
                "e_inc_min": row.e_inc_min,
                "e_inc_max": row.e_inc_max,
                "points": row.points,
                "x4_code": row.x4_code,
                "sf4": row.sf4,
                "sf5": row.sf5,
                "sf6": row.sf6,
                "sf7": row.sf7,
                "sf8": row.sf8,
                "sf9": row.sf9,
                "mt": row.mt,
                "mf": row.mf,
            }
            for row in result
        }
        if result
        else {}
    )

    return entries


def get_entry_bib(entries):
    stmt = select(exfor_bib).where(exfor_bib.c.entry.in_(entries))

    with engines["exfor"].connect() as connection:
        result = connection.execute(stmt).fetchall()

    legend = {
        row.entry: {
            "author": row.first_author,
            "year": row.year if row.year else 1900,
        }
        for row in result
    }

    return OrderedDict(
        sorted(legend.items(), key=lambda x: getitem(x[1], "year"), reverse=True)
    )


def entry_query_by_id(entries):
    stmt = select(exfor_bib).where(exfor_bib.c.entry.in_(entries))

    with engines["exfor"].connect() as connection:
        df = pd.read_sql(stmt, connection)

    return df


def reaction_query_by_id(entries):
    stmt = select(exfor_reactions).where(exfor_reactions.c.entry.in_(entries))

    with engines["exfor"].connect() as connection:
        df = pd.read_sql(stmt, connection)

    return df


def index_query_by_id(entries):
    queries = exfor_indexes.entry.in_(tuple(entries))

    stmt = select(exfor_indexes).where(queries)

    with engines["exfor"].connect() as connection:
        result = connection.execute(stmt)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())

    return df


def data_query(input_store, entids):
    obs_type = input_store.get("obs_type", "").upper()
    level_num = input_store.get("level_num")

    if obs_type == "XS":
        obs_type = "SIG"

    filters = [exfor_data.c.entry_id.in_(tuple(entids))]

    if level_num is not None:
        filters.append(exfor_data.c.level_num == level_num)

    if obs_type == "SIG":
        columns = [
            exfor_data.c.entry_id,
            exfor_data.c.en_inc,
            exfor_data.c.den_inc,
            exfor_data.c.level_num,
            exfor_data.c.residual,
            exfor_data.c.data,
            exfor_data.c.ddata,
        ]
    elif obs_type == "RP":
        columns = [
            exfor_data.c.entry_id,
            exfor_data.c.en_inc,
            exfor_data.c.den_inc,
            exfor_data.c.residual,
            exfor_data.c.data,
            exfor_data.c.ddata,
        ]
    elif obs_type == "FY":
        columns = [
            exfor_data.c.entry_id,
            exfor_data.c.en_inc,
            exfor_data.c.den_inc,
            exfor_data.c.charge,
            exfor_data.c.mass,
            exfor_data.c.isomer,
            exfor_data.c.residual,
            exfor_data.c.data,
            exfor_data.c.ddata,
        ]
    elif obs_type == "DA":
        columns = [
            exfor_data.c.entry_id,
            exfor_data.c.en_inc,
            exfor_data.c.den_inc,
            exfor_data.c.angle,
            exfor_data.c.dangle,
            exfor_data.c.data,
            exfor_data.c.ddata,
        ]
    elif obs_type == "DE":
        columns = [
            exfor_data.c.entry_id,
            exfor_data.c.en_inc,
            exfor_data.c.den_inc,
            exfor_data.c.e_out,
            exfor_data.c.de_out,
            exfor_data.c.data,
            exfor_data.c.ddata,
        ]
    elif obs_type == "TH":
        # thermal energy 限定
        filters.append(exfor_data.c.en_inc >= 2.52e-8)
        filters.append(exfor_data.c.en_inc <= 2.54e-8)
        columns = [
            exfor_data.c.entry_id,
            exfor_data.c.en_inc,
            exfor_data.c.den_inc,
            exfor_data.c.data,
            exfor_data.c.ddata,
        ]
    else:
        # その他は全カラム取得（非推奨？）
        columns = [exfor_data]

    stmt = select(*columns).where(and_(*filters))

    with engines["exfor"].connect() as conn:
        result = conn.execute(stmt)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())

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


def index_query_fission(obs_type, elem, mass, reaction, branch, lower, upper):
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

    with engines["exfor"].connect() as connection:
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

    with engines["exfor"].connect() as connection:
        df = pd.read_sql(
            sql=all.statement,
            con=connection,
        )

    return df
