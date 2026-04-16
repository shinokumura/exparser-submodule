import sys
import numpy as np
import importlib
import pandas as pd
from collections import OrderedDict
from operator import getitem
from sqlalchemy import select, and_, not_, func

try:
    # from app.py
    from config import engines
except ImportError:
    # for unit test
    module_name = sys.modules[__name__].split(".")[0]
    config = importlib.import_module(f"{module_name}.config")
    from config import engines

from exforparser.sql.models_core import (
    exfor_bib,
    exfor_reactions,
    exfor_data,
    exfor_indexes,
)

from ..utilities.util import (
    elemtoz_nz,
    get_number_from_string,
    get_str_from_string,
    x4style_nuclide_expression,
)
from ..utilities.reaction import (
    convert_partial_reactionstr_to_inl,
    convert_reaction_to_exfor_style,
    convert_partial_reactionstr_to_inl,
)


def get_exfor_bib_table():
    with engines["exfor"].connect() as connection:
        df = pd.read_sql_table("exfor_bib", connection)
        return df


def get_exfor_reference_table():
    with engines["exfor"].connect() as connection:
        df = pd.read_sql_table("exfor_references", connection)
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
        df = pd.read_sql_table("exfor_indexes", connection)
        return df


########  -------------------------------------------- ##########
##  EXFOR entry queries for the dataexplorer/api/exfor/search  ##
########  -------------------------------------------- ##########
def entries_query(**kwargs):
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

    sf5 = kwargs.get("sf5")
    sf4 = kwargs.get("sf4")
    sf7 = kwargs.get("sf7")
    sf8 = kwargs.get("sf8")

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
            func.min(exfor_indexes.c.en_inc_min).label("en_inc_min"),
            func.max(exfor_indexes.c.en_inc_max).label("en_inc_max"),
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
    queries = [
        exfor_indexes.c.main_facility_institute == facility_code,
        exfor_indexes.c.main_facility_type == facility_type.upper(),
    ]

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

def _exfor_cond_xs(input_store: dict, reaction: str) -> tuple[list, str]:
    """Extra conditions for XS, TH, and RI queries (SIG observables)."""
    branch = input_store.get("branch")
    level_num = input_store.get("level_num")
    conditions = []

    if branch:
        conditions.append(exfor_indexes.c.sf5 == branch.upper())
    elif isinstance(level_num, int):
        reaction = convert_partial_reactionstr_to_inl(reaction)
        conditions += [exfor_indexes.c.sf5 == "PAR", exfor_indexes.c.level_num == level_num]
    elif input_store.get("excl_junk_switch") or not branch:
        conditions.append(exfor_indexes.c.sf5 == None)

    conditions.append(
        exfor_indexes.c.process == reaction.replace("total", "tot").upper()
    )

    if not any(r in reaction for r in ("tot", "f")):
        conditions += [
            not_(exfor_indexes.c.sf4.endswith(f"-{suffix}"))
            for suffix in ("G", "M", "L", "M1", "M2")
        ]

    return conditions, reaction


def _exfor_cond_rp(input_store: dict, reaction: str) -> tuple[list, str]:
    """Extra conditions for residual product (RP) queries.
    Residual nuclides are stored as e.g. 'Mg-28' or 'Sc-44-M' in the index table.
    """
    rp_elem = input_store.get("rp_elem")
    rp_mass = input_store.get("rp_mass")
    if rp_mass.endswith(("m", "M", "g", "G", "L", "M1", "M2", "m1", "m2")):
        rp_mass_fmt = f"{rp_elem.capitalize()}-{get_number_from_string(rp_mass)}-{get_str_from_string(str(rp_mass)).upper()}"
    else:
        rp_mass_fmt = f"{rp_elem.capitalize()}-{rp_mass}"
    return [exfor_indexes.c.residual == rp_mass_fmt], reaction


def _exfor_cond_fy(input_store: dict, reaction: str) -> tuple[list, str]:
    """Extra conditions for fission yield (FY) queries."""
    branch = input_store.get("branch")
    conditions = []
    if branch:
        conditions.append(exfor_indexes.c.sf5.in_(tuple(fy_branch(branch.upper()))))
    elif input_store.get("excl_junk_switch"):
        conditions.append(exfor_indexes.c.sf5 == None)
    return conditions, reaction


def _exfor_cond_da(input_store: dict, reaction: str) -> tuple[list, str]:
    """Extra conditions for angular distribution (DA) queries."""
    level_num = input_store.get("level_num")
    conditions = []
    if isinstance(level_num, int):
        reaction = convert_partial_reactionstr_to_inl(reaction)
        conditions += [exfor_indexes.c.sf5 == "PAR", exfor_indexes.c.level_num == level_num]
    return conditions, reaction


def _exfor_cond_macs(input_store: dict, reaction: str) -> tuple[list, str]:
    """Extra conditions for MACS (Maxwellian-Averaged Cross Section) queries.
    MACS entries carry sf8 in ['MXW', 'MXW/MSC', 'MXW/FCT', 'SPA'] to distinguish
    them from ordinary cross sections at the same energy.  sf5 and sf7 must be absent.
    Config sets fixed_sf8=True so excl_junk_switch does not also require sf8==None.
    """
    conditions = [
        exfor_indexes.c.sf5 == None,
        exfor_indexes.c.sf7 == None,
        exfor_indexes.c.sf8.in_(["MXW", "MXW/MSC", "MXW/FCT", "MXW+", "SPA"]),
        exfor_indexes.c.process == reaction.upper(),
    ]
    return conditions, reaction


def _exfor_cond_gg(input_store: dict, reaction: str) -> tuple[list, str]:
    """Extra conditions for gamma-gamma width (GG) queries.
    GG entries carry sf6='WID' and sf8='AV' (averaged over resolved resonances).
    Config sets fixed_sf8=True so excl_junk_switch does not also require sf8==None.
    """
    conditions = [
        exfor_indexes.c.sf5 == None,
        exfor_indexes.c.sf7 == None,
        exfor_indexes.c.sf8 == "AV",
        exfor_indexes.c.process == reaction.upper(),
    ]
    return conditions, reaction


def _exfor_cond_d(input_store: dict, reaction: str) -> tuple[list, str]:
    """Extra conditions for resonance spacing (D) queries.
    Typically reaction is 'N,0' (zero-level EXFOR code for level spacing).
    """
    conditions = [
        exfor_indexes.c.sf5 == None,
        exfor_indexes.c.sf7 == None,
        exfor_indexes.c.process == reaction.upper(),
    ]
    return conditions, reaction


# Maps the page-level obs_type key to:
#   sf6      : the EXFOR SF6 field value used in the index table
#   extra    : callable(input_store, reaction) -> (conditions, reaction)
#              returns obs_type-specific query conditions and the (possibly overridden) reaction string
#   fixed_sf8: when True the builder already pins sf8, so excl_junk_switch must NOT also
#              require sf8==None (which would produce zero results for MACS/GG)
# To add a new observable: add one entry here and implement its condition builder above.
EXFOR_OBS_TYPE_CONFIG: dict = {
    "XS":   {"sf6": "SIG", "extra": _exfor_cond_xs,           "fixed_sf8": False},
    "TH":   {"sf6": "SIG", "extra": _exfor_cond_xs,           "fixed_sf8": False},  # energy filter applied in data_query
    "RI":   {"sf6": "RI",  "extra": _exfor_cond_xs,           "fixed_sf8": False},
    "RP":   {"sf6": "SIG", "extra": _exfor_cond_rp,           "fixed_sf8": False},
    "FY":   {"sf6": "FY",  "extra": _exfor_cond_fy,           "fixed_sf8": False},
    "DA":   {"sf6": "DA",  "extra": _exfor_cond_da,           "fixed_sf8": False},
    "DE":   {"sf6": "DE",  "extra": lambda _, r: ([], r),     "fixed_sf8": False},
    "MACS": {"sf6": "SIG", "extra": _exfor_cond_macs,         "fixed_sf8": True},   # sf8=MXW/* set by builder
    "GG":   {"sf6": "WID", "extra": _exfor_cond_gg,           "fixed_sf8": True},   # sf8=AV    set by builder
    "D":    {"sf6": "D",   "extra": _exfor_cond_d,            "fixed_sf8": False},
}


def exfor_index_query(input_store) -> dict:
    obs_type = input_store.get("obs_type").upper()
    config = EXFOR_OBS_TYPE_CONFIG[obs_type]

    reaction = convert_reaction_to_exfor_style(input_store.get("reaction"))
    target = x4style_nuclide_expression(
        input_store.get("target_elem"), input_store.get("target_mass")
    )
    projectile = input_store.get("inc_pt")

    queries = [
        exfor_indexes.c.target == target,
        exfor_indexes.c.arbitrary_data == False,
        exfor_indexes.c.projectile == projectile.upper(),
    ]

    extra_conditions, reaction = config["extra"](input_store, reaction)
    queries.extend(extra_conditions)

    if input_store.get("excl_junk_switch"):
        queries += [exfor_indexes.c.sf7 == None, exfor_indexes.c.sf9 == None]
        if not config.get("fixed_sf8"):
            # Skip sf8==None for obs types where sf8 carries a meaningful value
            # (e.g. MACS: sf8='MXW', GG: sf8='AV') — already constrained by the builder.
            queries.append(exfor_indexes.c.sf8 == None)

    queries.append(exfor_indexes.c.sf6 == config["sf6"].upper())
    stmt = select(exfor_indexes).where(and_(*queries))

    with engines["exfor"].connect() as conn:
        result = conn.execute(stmt).fetchall()

    entries = (
        {
            row.entry_id: {
                "level_num": row.level_num,
                "en_inc_min": (
                    (row.en_inc_min / 1e6) if row.en_inc_min is not None else np.nan
                ),
                "en_inc_max": (
                    (row.en_inc_max / 1e6) if row.en_inc_max is not None else np.nan
                ),
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
        # restrict to thermal energy range (0.0253 eV)
        filters.append(exfor_data.c.en_inc >= 2.52e-2)  # in eV
        filters.append(exfor_data.c.en_inc <= 2.54e-2)  # in eV
        columns = [
            exfor_data.c.entry_id,
            exfor_data.c.en_inc,
            exfor_data.c.den_inc,
            exfor_data.c.data,
            exfor_data.c.ddata,
        ]
    elif obs_type in ("RI", "MACS", "GG", "D"):
        # Scalar observables: x = incident energy (or kT for MACS, resonance energy for GG/D)
        columns = [
            exfor_data.c.entry_id,
            exfor_data.c.en_inc,
            exfor_data.c.den_inc,
            exfor_data.c.data,
            exfor_data.c.ddata,
        ]
    else:
        # fallback: fetch all columns (not recommended)
        columns = [exfor_data]

    stmt = select(*columns).where(and_(*filters))

    with engines["exfor"].connect() as conn:
        result = conn.execute(stmt)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())

        ## Convert eV to MeV
        df["en_inc"] = df["en_inc"] / 1e6  # eV to MeV
        df["den_inc"] = df["den_inc"] / 1e6  # eV to MeV

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
    sf4 = None
    sf5 = None
    sf6 = None
    target = x4style_nuclide_expression(elem, mass)

    queries = [
        exfor_indexes.c.target == target,
        exfor_indexes.c.process == reaction.upper(),
        exfor_indexes.c.arbitrary_data == False,
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
        queries.append(exfor_indexes.c.sf4 == sf4)

    if sf5:
        queries.append(exfor_indexes.c.sf5.in_(tuple(sf5)))

    if sf6:
        queries.append(exfor_indexes.c.sf6.in_(tuple(sf6)))

    if lower and upper:
        # lower, upper = energy_range_conversion(energy_range)
        queries.append(exfor_indexes.c.en_inc_min >= lower)
        queries.append(exfor_indexes.c.en_inc_max <= upper)

    stmt = select(exfor_indexes).where(*queries)

    with engines["exfor"].connect() as conn:
            entries = conn.execute(stmt).fetchall()

    entids = {}

    for ent in entries:
        entids[ent.entry_id] = {
            "en_inc_min": ent.en_inc_min,
            "en_inc_max": ent.en_inc_max,
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
    stmt = (
        select(
            exfor_reactions.c.entry,
            exfor_reactions.c.entry_id,
            exfor_reactions.c.target,
            exfor_reactions.c.projectile,
            exfor_reactions.c.process,
            exfor_reactions.c.sf4,
            exfor_reactions.c.sf6,
            exfor_bib.c.first_author,
            exfor_bib.c.first_author_institute,
            exfor_bib.c.title,
            exfor_bib.c.main_reference,
            exfor_bib.c.main_doi,
            exfor_bib.c.authors,
            exfor_bib.c.year,
            exfor_bib.c.main_facility_institute,
            exfor_bib.c.main_facility_type,
            func.min(exfor_indexes.c.en_inc_min).label("en_inc_min"),
            func.max(exfor_indexes.c.en_inc_max).label("en_inc_max"),
        )
        .select_from(
            exfor_reactions
            .join(exfor_bib, exfor_reactions.c.entry == exfor_bib.c.entry)
            .join(exfor_indexes, exfor_indexes.c.entry_id == exfor_reactions.c.entry_id)
        )
        .group_by(exfor_reactions.c.entry_id)
        .order_by(exfor_bib.c.year.desc())
    )

    with engines["exfor"].connect() as connection:
        df = pd.read_sql(
            sql=stmt,
            con=connection,
        )

    return df


def join_index_bib():
    all = (
        select(
            exfor_indexes.c.entry_id,
            exfor_indexes.c.target,
            exfor_indexes.c.process,
            exfor_indexes.c.residual,
            exfor_indexes.c.en_inc_min,
            exfor_indexes.c.en_inc_max,
            exfor_indexes.c.sf5,
            exfor_indexes.c.sf6,
            exfor_indexes.c.sf7,
            exfor_indexes.c.sf8,
            exfor_bib.c.entry,
            exfor_bib.c.authors,
            exfor_bib.c.year,
            exfor_bib.c.main_facility_institute,
            exfor_bib.c.main_facility_type,
        )
        .join(exfor_bib, exfor_bib.c.entry == exfor_indexes.c.entry)
        .order_by(exfor_bib.c.year.desc())
    )

    with engines["exfor"].connect() as connection:
        df = pd.read_sql(
            sql=all,
            con=connection,
        )

    return df
