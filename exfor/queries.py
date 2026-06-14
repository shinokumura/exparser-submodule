import sys
import numpy as np
import importlib
import json
import pandas as pd
from functools import lru_cache
from collections import OrderedDict
from operator import getitem
from sqlalchemy import select, and_, not_, or_, func, text, distinct

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
    exfor_native_data,
    exfor_indexes,
    exfor_institute_geo,
    exfor_entry_dois,
    exfor_histories,
)
try:
    from exforparser.sql.stored_insert import ensure_exfor_data_frame_columns
except ImportError:
    def ensure_exfor_data_frame_columns(connection):
        return None

from ..utilities.util import (
    elemtoz_nz,
    get_number_from_string,
    get_str_from_string,
    x4style_nuclide_expression,
)
from ..utilities.reaction import (
    convert_partial_reactionstr_to_inl,
    convert_reaction_to_exfor_style,
)
from ..utilities.obs_types import SCALAR_OBS, L_WAVE_OBS


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


def _format_native_values(values):
    if values is None:
        return ""
    try:
        parsed = json.loads(values)
    except (TypeError, json.JSONDecodeError):
        parsed = values
    if isinstance(parsed, list):
        return ", ".join(str(value) for value in parsed if value is not None)
    return str(parsed)


def trn_thickness_query(entry_ids):
    """Return raw EXFOR THICKNESS values with original units for TRN entries."""
    entry_ids = tuple(entry_id for entry_id in entry_ids if entry_id)
    if not entry_ids:
        return {}

    stmt = (
        select(
            exfor_native_data.c.entry_id,
            exfor_native_data.c.unit,
            exfor_native_data.c.data,
        )
        .where(
            and_(
                exfor_native_data.c.entry_id.in_(entry_ids),
                exfor_native_data.c.head == "THICKNESS",
            )
        )
        .order_by(exfor_native_data.c.entry_id, exfor_native_data.c.column_index)
    )

    with engines["exfor"].connect() as conn:
        rows = conn.execute(stmt).fetchall()

    out = {}
    for row in rows:
        values = _format_native_values(row.data)
        if not values:
            continue
        label = f"{values} {row.unit}".strip()
        out.setdefault(row.entry_id, []).append(label)

    return {entry_id: "; ".join(values) for entry_id, values in out.items()}


def trn_areal_density_query(entry_ids):
    """Return numeric TRN THICKNESS values in atoms/barn keyed by entry id."""
    entry_ids = tuple(entry_id for entry_id in entry_ids if entry_id)
    if not entry_ids:
        return {}

    stmt = (
        select(
            exfor_native_data.c.entry_id,
            exfor_native_data.c.unit,
            exfor_native_data.c.data,
        )
        .where(
            and_(
                exfor_native_data.c.entry_id.in_(entry_ids),
                exfor_native_data.c.head == "THICKNESS",
                exfor_native_data.c.unit == "ATOMS/B",
            )
        )
        .order_by(exfor_native_data.c.entry_id, exfor_native_data.c.column_index)
    )

    with engines["exfor"].connect() as conn:
        rows = conn.execute(stmt).fetchall()

    out = {}
    for row in rows:
        try:
            values = json.loads(row.data)
        except (TypeError, json.JSONDecodeError):
            values = [row.data]

        numeric_values = []
        for value in values if isinstance(values, list) else [values]:
            try:
                numeric_values.append(float(value))
            except (TypeError, ValueError):
                continue

        if numeric_values:
            out.setdefault(row.entry_id, []).extend(numeric_values)

    return out


def entry_doi_query(entry: str) -> dict:
    stmt = select(
        exfor_entry_dois.c.exfor_main_reference,
        exfor_entry_dois.c.main_reference_doi,
    ).where(exfor_entry_dois.c.entry == str(entry)[:5])

    with engines["exfor"].connect() as connection:
        rows = connection.execute(stmt).fetchall()

    return {
        row.exfor_main_reference: row.main_reference_doi
        for row in rows
        if row.exfor_main_reference and row.main_reference_doi
    }


########  -------------------------------------------- ##########
##  EXFOR entry queries for the dataexplorer/api/exfor/search  ##
########  -------------------------------------------- ##########
def entries_query(**kwargs):
    # not used, use indexes_query instead
    queries = []

    obs_type = kwargs.get("obs_type")
    obs_types = kwargs.get("obs_types")
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

    if obs_types:
        if isinstance(obs_types, str):
            obs_types = [obs_types]
        queries.append(exfor_reactions.c.sf6.in_(obs_types))

    elif obs_type:
        queries.append(exfor_reactions.c.sf6 == obs_type)

    if elem and not mass:
        queries.append(
            exfor_reactions.c.target.like(f"%{elemtoz_nz(elem)}-{elem.upper()}-%")
        )

    elif elem and mass:
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
##         Index query for the dataexplorer
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

    conditions += [
            exfor_indexes.c.process == reaction.replace("total", "tot").upper(),
            exfor_indexes.c.arbitrary_data == False
        ]

    if not any(r in reaction for r in ("tot", "f")):
        conditions += [
            not_(exfor_indexes.c.sf4.endswith(f"-{suffix}"))
            for suffix in ("G", "M", "L", "M1", "M2")
        ]

    return conditions, reaction


def _exfor_cond_th(input_store: dict, reaction: str) -> tuple[list, str]:
    """Extra conditions for thermal neutrons, energy in eV."""
    conditions = []
    if input_store.get("rp_elem") and input_store.get("rp_mass"):
        rp_conditions, _ = _exfor_cond_rp(input_store, reaction)
        conditions += rp_conditions
        
    conditions += [
        exfor_indexes.c.process == reaction.upper(),
        exfor_indexes.c.sf5.is_(None),
        exfor_indexes.c.sf6 == "SIG",
        exfor_indexes.c.sf7.is_(None),
        exfor_indexes.c.en_inc_min >= 0.024,
        exfor_indexes.c.en_inc_max <= 0.026,
        exfor_indexes.c.arbitrary_data == False
    ]
    return conditions, reaction


def _exfor_cond_rp(input_store: dict, reaction: str) -> tuple[list, str]:
    """Extra conditions for residual product (RP) queries.
    Residual nuclides are stored as e.g. 'Mg-28' or 'Sc-44-M' in the index table.
    """
    conditions = []
    if input_store.get("rp_elem") and input_store.get("rp_mass"):
        conditions = [exfor_indexes.c.arbitrary_data == False]
        rp_elem = input_store.get("rp_elem")
        rp_mass = input_store.get("rp_mass")
        if rp_mass.endswith(("m", "M", "g", "G", "L", "M1", "M2", "m1", "m2")):
            rp_mass_fmt = f"{rp_elem.capitalize()}-{get_number_from_string(rp_mass)}-{get_str_from_string(str(rp_mass)).upper()}"
        else:
            rp_mass_fmt = f"{rp_elem.capitalize()}-{rp_mass}"
        conditions.append(exfor_indexes.c.residual == rp_mass_fmt)
    return conditions, reaction


def _exfor_cond_fy(input_store: dict, reaction: str) -> tuple[list, str]:
    """Extra conditions for fission yield (FY) queries."""
    branch = input_store.get("branch")
    conditions = [exfor_indexes.c.arbitrary_data == False]
    if branch:
        conditions.append(exfor_indexes.c.sf5.in_(tuple(fy_branch(branch.upper()))))
    elif input_store.get("excl_junk_switch"):
        conditions.append(exfor_indexes.c.sf5 == None)
    return conditions, reaction


def _exfor_cond_da(input_store: dict, reaction: str) -> tuple[list, str]:
    """Extra conditions for angular distribution (DA) queries."""
    level_num = input_store.get("level_num")
    conditions = [exfor_indexes.c.arbitrary_data == False]
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
        or_(
            exfor_indexes.c.sf8.in_(["MXW", "MXW/MSC", "MXW/FCT", "MXW+", "SPA"]),
            exfor_indexes.c.sf8 == None
        ),        exfor_indexes.c.process == reaction.upper(),
        or_(
            exfor_indexes.c.x_head.like("KT%"),
            exfor_indexes.c.x_head.like("EN%")
        ),
        exfor_indexes.c.en_inc_min >= 23000,
        exfor_indexes.c.en_inc_max <= 35000,
        exfor_indexes.c.arbitrary_data == False,
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
        exfor_indexes.c.arbitrary_data == False,
    ]
    return conditions, reaction


def _exfor_cond_d(input_store: dict, reaction: str) -> tuple[list, str]:
    """Extra conditions for resonance spacing (D) queries.
    Typically reaction is 'N,0' (zero-level EXFOR code for level spacing).
    L-wave filtering (D0/D1/D2) is applied in data_query via the flags column.
    """
    conditions = [
        exfor_indexes.c.sf5 == None,
        exfor_indexes.c.sf7 == None,
        exfor_indexes.c.process == reaction.upper(),
        exfor_indexes.c.arbitrary_data == False,
    ]
    return conditions, reaction


def _exfor_cond_ri(input_store: dict, reaction: str):
    conditions = [
        exfor_indexes.c.sf5 == None,
        exfor_indexes.c.sf7 == None,
        exfor_indexes.c.process == reaction.upper(),
        ## STF: DATA unit is always NO-DIM
        exfor_indexes.c.arbitrary_data == False,
    ]
    return conditions, reaction


def _exfor_cond_de(input_store, reaction):
    return [], ""


def _exfor_cond_trn(input_store: dict, reaction: str) -> tuple[list, str]:
    """No extra conditions for TRN — sf6='TRN' in the base query is sufficient.
    fixed_sf8=True in the config prevents the excl_junk_switch sf8==None filter,
    since TRN entries legitimately carry sf8 values (e.g. 'FCT')."""
    return [], reaction


def _exfor_cond_ddx(input_store: dict, reaction: str) -> tuple[list, str]:
    """Extra conditions for double-differential cross section (DA/DE) queries."""
    level_num = input_store.get("level_num")
    conditions = [exfor_indexes.c.arbitrary_data == False]
    sf4 = input_store.get("sf4")
    sf5 = input_store.get("sf5")
    sf7 = input_store.get("sf7")
    sf8 = input_store.get("sf8")

    if sf4:
        conditions.append(exfor_indexes.c.sf4 == str(sf4).upper())
    if sf5:
        sf5_values = sf5 if isinstance(sf5, (list, tuple)) else [sf5]
        conditions.append(exfor_indexes.c.sf5.in_(tuple(sf5_values)))
    if sf7:
        conditions.append(exfor_indexes.c.sf7 == str(sf7).upper())
    if sf8:
        sf8_values = sf8 if isinstance(sf8, (list, tuple)) else [sf8]
        conditions.append(exfor_indexes.c.sf8.in_(tuple(sf8_values)))

    if isinstance(level_num, int):
        reaction = convert_partial_reactionstr_to_inl(reaction)
        conditions += [exfor_indexes.c.sf5 == "PAR", exfor_indexes.c.level_num == level_num]
    return conditions, reaction


def _exfor_cond_stf(input_store: dict, reaction: str) -> tuple[list, str]:
    """Extra conditions for strength function (STF) queries."""
    conditions = [
        exfor_indexes.c.sf5 == None,
        exfor_indexes.c.sf7 == None,
        exfor_indexes.c.process == reaction.upper(),
        exfor_indexes.c.arbitrary_data == True,
    ]
    return conditions, reaction


def _exfor_cond_rad(input_store: dict, reaction: str) -> tuple[list, str]:
    """Extra conditions for scattering radius (RAD) queries."""
    conditions = [
        exfor_indexes.c.sf5 == None,
        exfor_indexes.c.sf7 == None,
        exfor_indexes.c.process == reaction.upper(),
        exfor_indexes.c.arbitrary_data == False,
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
    "RP":   {"sf6": "SIG", "extra": _exfor_cond_rp,           "fixed_sf8": False},
    "FY":   {"sf6": "FY",  "extra": _exfor_cond_fy,           "fixed_sf8": False},
    "DA":   {"sf6": "DA",  "extra": _exfor_cond_da,           "fixed_sf8": False},
    "DE":   {"sf6": "DE",  "extra": _exfor_cond_de,           "fixed_sf8": False},
    "TH":   {"sf6": "SIG", "extra": _exfor_cond_th,           "fixed_sf8": False},  # energy filter applied in data_query
    "RI":   {"sf6": "RI",  "extra": _exfor_cond_ri,           "fixed_sf8": False},
    "MACS": {"sf6": "SIG", "extra": _exfor_cond_macs,         "fixed_sf8": True},   # sf8=MXW/* set by builder
    # Per-quantity aliases — same EXFOR conditions as their parent type;
    # reaction parameter distinguishes D0 (n,0) from D1 (n,el) etc.
    "D0":   {"sf6": "D",   "extra": _exfor_cond_d,            "fixed_sf8": False},
    "D1":   {"sf6": "D",   "extra": _exfor_cond_d,            "fixed_sf8": False},
    "D2":   {"sf6": "D",   "extra": _exfor_cond_d,            "fixed_sf8": False},
    "S0":   {"sf6": "STF", "extra": _exfor_cond_stf,          "fixed_sf8": False},
    "S1":   {"sf6": "STF", "extra": _exfor_cond_stf,          "fixed_sf8": False},
    "GG0":  {"sf6": "WID", "extra": _exfor_cond_gg,           "fixed_sf8": True},
    "GG1":  {"sf6": "WID", "extra": _exfor_cond_gg,           "fixed_sf8": True},
    "RAD":  {"sf6": "RAD", "extra": _exfor_cond_rad,          "fixed_sf8": False},
    "TRN":  {"sf6": "TRN", "extra": _exfor_cond_trn,          "fixed_sf8": True},
    "DDX":  {"sf6": "DA/DE", "extra": _exfor_cond_ddx,        "fixed_sf8": False},
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
        exfor_indexes.c.projectile == projectile.upper(),
        exfor_indexes.c.sf6 == config["sf6"].upper(),
        ~exfor_indexes.c.entry_id.like("V%"),
    ]

    extra_conditions, reaction = config["extra"](input_store, reaction)
    queries.extend(extra_conditions)

    if input_store.get("excl_junk_switch"):
        if not input_store.get("sf7"):
            queries.append(exfor_indexes.c.sf7 == None)
        queries.append(exfor_indexes.c.sf9 == None)
        if not config.get("fixed_sf8") and not input_store.get("sf8"):
            # Skip sf8==None for obs types where sf8 carries a meaningful value
            # (e.g. MACS: sf8='MXW', GG: sf8='AV') — already constrained by the builder.
            queries.append(exfor_indexes.c.sf8 == None)

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


@lru_cache(maxsize=2048)
def exfor_available_reactions_query(obs_type, elem, mass, projectile):
    """Return EXFOR reactions available for one observable/target/projectile.

    The dropdown only needs to know whether any data exists, so this query reads
    distinct rows from exfor_indexes instead of probing each reaction option.
    """
    obs_type = (obs_type or "").upper()
    config = EXFOR_OBS_TYPE_CONFIG.get(obs_type)
    if config is None:
        return []

    target = x4style_nuclide_expression(elem, mass)
    projectile = (projectile or "").upper()

    queries = [
        exfor_indexes.c.target == target,
        exfor_indexes.c.projectile == projectile,
        exfor_indexes.c.sf6 == config["sf6"].upper(),
        ~exfor_indexes.c.entry_id.like("V%"),
    ]

    if obs_type in {"XS", "DA", "DDX"}:
        queries.append(exfor_indexes.c.arbitrary_data == False)

    stmt = (
        select(
            exfor_indexes.c.process,
            exfor_indexes.c.sf5,
            exfor_indexes.c.level_num,
        )
        .distinct()
        .where(and_(*queries))
    )

    with engines["exfor"].connect() as conn:
        results = conn.execute(stmt).fetchall()

    return [
        {
            "process": row.process,
            "sf5": row.sf5,
            "level_num": row.level_num,
        }
        for row in results
    ]


@lru_cache(maxsize=2048)
def exfor_available_projectiles_query(obs_type, elem=None, mass=None, exclude_projectiles=("N", "G", "E")):
    """Return projectiles with EXFOR data for an observable/target."""
    obs_type = (obs_type or "").upper()
    config = EXFOR_OBS_TYPE_CONFIG.get(obs_type)
    if config is None:
        return []

    queries = [
        exfor_indexes.c.sf6 == config["sf6"].upper(),
        exfor_indexes.c.projectile.is_not(None),
        ~exfor_indexes.c.entry_id.like("V%"),
    ]
    if exclude_projectiles:
        queries.append(~exfor_indexes.c.projectile.in_(tuple(exclude_projectiles)))

    if elem and mass:
        queries.append(exfor_indexes.c.target == x4style_nuclide_expression(elem, mass))

    if obs_type in {"XS", "DA", "DDX"}:
        queries.append(exfor_indexes.c.arbitrary_data == False)

    stmt = (
        select(
            exfor_indexes.c.projectile,
            func.count(distinct(exfor_indexes.c.process)).label("processes"),
            func.count(distinct(exfor_indexes.c.entry_id)).label("entries"),
        )
        .where(and_(*queries))
        .group_by(exfor_indexes.c.projectile)
        .order_by(func.count(distinct(exfor_indexes.c.entry_id)).desc(), exfor_indexes.c.projectile)
    )

    with engines["exfor"].connect() as conn:
        results = conn.execute(stmt).fetchall()

    return [
        {
            "projectile": row.projectile,
            "processes": row.processes,
            "entries": row.entries,
        }
        for row in results
    ]


def ddx_product_options_query(elem, mass, projectile):
    target = x4style_nuclide_expression(elem, mass)
    projectile = (projectile or "").upper()
    stmt = (
        select(
            exfor_indexes.c.sf4,
            func.count(distinct(exfor_indexes.c.entry_id)).label("entries"),
            func.sum(exfor_indexes.c.points).label("points"),
        )
        .where(
            and_(
                exfor_indexes.c.target == target,
                exfor_indexes.c.projectile == projectile,
                exfor_indexes.c.sf6 == "DA/DE",
                exfor_indexes.c.arbitrary_data == False,
                ~exfor_indexes.c.entry_id.like("V%"),
            )
        )
        .group_by(exfor_indexes.c.sf4)
        .order_by(func.count(distinct(exfor_indexes.c.entry_id)).desc(), exfor_indexes.c.sf4)
    )

    with engines["exfor"].connect() as conn:
        results = conn.execute(stmt).fetchall()

    return [
        {
            "sf4": row.sf4,
            "entries": row.entries or 0,
            "points": row.points or 0,
        }
        for row in results
    ]



def residual_nuclide_list(input_store):

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
        exfor_indexes.c.sf6 == config["sf6"].upper()
    ]

    stmt = select(exfor_indexes).where(and_(*queries))

    with engines["exfor"].connect() as conn:
        results = conn.execute(stmt).fetchall()

    return [row.residual for row in results] if results else []



########  -------------------------------------- ##########
##         Entry query for the dataexplorer
########  -------------------------------------- ##########

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
    queries = exfor_indexes.c.entry.in_(tuple(entries))

    stmt = select(exfor_indexes).where(queries)

    with engines["exfor"].connect() as connection:
        result = connection.execute(stmt)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())

    return df


INDEX_ROW_LIMIT = 5000


def index_search_query(entry_ids: list, sf6_filter: list = None):
    """
    Query exfor_indexes for specific entry_ids, selecting only the columns
    needed for the reaction index results table (columnDefsIndexSearchRes).

    sf6_filter: optional list of SF6 codes to narrow the result when the
    user selected a specific observable (obs_type) or group (obs_group).

    Returns (df, truncated) — truncated=True when rows were capped at
    INDEX_ROW_LIMIT to avoid serialising huge payloads to the browser.
    """
    if not entry_ids:
        return pd.DataFrame(), False

    cols = [
        exfor_indexes.c.entry_id,
        exfor_indexes.c.target,
        exfor_indexes.c.process,
        exfor_indexes.c.sf4,
        exfor_indexes.c.residual,
        exfor_indexes.c.level_num,
        exfor_indexes.c.sf5,
        exfor_indexes.c.sf6,
        exfor_indexes.c.sf7,
        exfor_indexes.c.sf8,
        exfor_indexes.c.en_inc_min,
        exfor_indexes.c.en_inc_max,
    ]

    conditions = [exfor_indexes.c.entry_id.in_(entry_ids)]
    if sf6_filter:
        conditions.append(exfor_indexes.c.sf6.in_(sf6_filter))

    stmt = (
        select(*cols)
        .distinct()
        .where(and_(*conditions))
        .limit(INDEX_ROW_LIMIT + 1)
    )

    with engines["exfor"].connect() as conn:
        df = pd.read_sql(stmt, conn)

    truncated = len(df) > INDEX_ROW_LIMIT
    if truncated:
        df = df.iloc[:INDEX_ROW_LIMIT]

    return df, truncated


def data_query(input_store, entids):
    obs_type = input_store.get("obs_type", "").upper()
    level_num = input_store.get("level_num")

    if obs_type == "XS":
        obs_type = "SIG"
    elif obs_type == "DDX":
        obs_type = "DA/DE"

    filters = [
        exfor_data.c.entry_id.in_(tuple(entids)),
        ~exfor_data.c.entry_id.like("V%"),
    ]
    en_cols = [
        exfor_data.c.en_inc,
        exfor_data.c.den_inc,
        *([exfor_data.c.en_inc_frame] if "en_inc_frame" in exfor_data.c else []),
    ]
    y_cols = [
        exfor_data.c.data,
        exfor_data.c.ddata,
        *([exfor_data.c.data_frame] if "data_frame" in exfor_data.c else []),
    ]
    e_out_cols = [
        exfor_data.c.e_out,
        exfor_data.c.de_out,
        *([exfor_data.c.e_out_frame] if "e_out_frame" in exfor_data.c else []),
    ]
    angle_cols = [
        exfor_data.c.angle,
        exfor_data.c.dangle,
        *([exfor_data.c.angle_frame] if "angle_frame" in exfor_data.c else []),
    ]

    if level_num is not None:
        filters.append(exfor_data.c.level_num == level_num)

    if obs_type == "SIG":
        columns = [
            exfor_data.c.entry_id,
            *en_cols,
            exfor_data.c.level_num,
            exfor_data.c.residual,
            *y_cols,
        ]
    elif obs_type == "RP":
        columns = [
            exfor_data.c.entry_id,
            *en_cols,
            exfor_data.c.residual,
            *y_cols,
        ]
    elif obs_type == "FY":
        columns = [
            exfor_data.c.entry_id,
            *en_cols,
            exfor_data.c.charge,
            exfor_data.c.mass,
            exfor_data.c.isomer,
            exfor_data.c.residual,
            *y_cols,
        ]
    elif obs_type == "DA":
        columns = [
            exfor_data.c.entry_id,
            *en_cols,
            *angle_cols,
            *y_cols,
        ]
    elif obs_type == "DE":
        columns = [
            exfor_data.c.entry_id,
            *en_cols,
            *e_out_cols,
            *y_cols,
        ]
    elif obs_type == "TH":
        # restrict to thermal energy range (0.0253 eV)
        filters.append(exfor_data.c.en_inc >= 2.52e-2)  # in eV
        filters.append(exfor_data.c.en_inc <= 2.54e-2)  # in eV
        columns = [
            exfor_data.c.entry_id,
            *en_cols,
            *y_cols,
        ]
    elif obs_type == "TRN":
        columns = [
            exfor_data.c.entry_id,
            *en_cols,
            *y_cols,
        ]
    elif obs_type == "DA/DE":
        columns = [
            exfor_data.c.entry_id,
            *en_cols,
            *angle_cols,
            *e_out_cols,
            *y_cols,
        ]
    elif obs_type in SCALAR_OBS - {"TH"}:
        # Scalar observables: fetch flags too for L-wave filtering (D0/D1/D2, S0/S1)
        columns = [
            exfor_data.c.entry_id,
            *en_cols,
            *y_cols,
            exfor_data.c.flags,
        ]
    else:
        # fallback: fetch all columns (not recommended)
        columns = [exfor_data]

    stmt = select(*columns).where(and_(*filters))

    with engines["exfor"].begin() as conn:
        ensure_exfor_data_frame_columns(conn)
        result = conn.execute(stmt)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())

        ## Convert eV to MeV
        df["en_inc"] = df["en_inc"] / 1e6  # eV to MeV
        df["den_inc"] = df["den_inc"] / 1e6  # eV to MeV

    # Filter by Momentum L for D0/D1/D2/S0/S1
    if obs_type in L_WAVE_OBS and "flags" in df.columns:
        import json as _json
        expected_l = L_WAVE_OBS[obs_type]

        def _get_momentum_l(flags_val):
            if not flags_val:
                return None
            try:
                d = _json.loads(flags_val) if isinstance(flags_val, str) else flags_val
                return d.get("MOMENTUM L", {}).get("data")
            except Exception:
                return None

        df["momentum_l"] = df["flags"].apply(_get_momentum_l)
        # Keep rows where L matches expected OR L is unknown (None/NaN)
        df = df[df["momentum_l"].isna() | (df["momentum_l"] == expected_l)]
        df = df.drop(columns=["flags", "momentum_l"])
    elif "flags" in df.columns:
        df = df.drop(columns=["flags"])

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
    entry_list = []

    for ent in entries:
        entids[ent.entry_id] = {
            "en_inc_min": ent.en_inc_min,
            "en_inc_max": ent.en_inc_max,
            "points": ent.points,
            "sf5": ent.sf5,
            "sf8": ent.sf8,
            "x4_code": ent.x4_code,
        }
        entry_list.append(ent.entry)

    return entids, entry_list


########  -------------------------------------- ##########
##         Join table for AGGrid
########  -------------------------------------- ##########

def join_reaction_bib():
    stmt = (
        select(
            exfor_bib.c.entry,
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
            func.max(exfor_entry_dois.c.main_reference_doi).label("main_doi"),
            exfor_bib.c.authors,
            exfor_bib.c.year,
            exfor_bib.c.main_facility_institute,
            exfor_bib.c.main_facility_type,
            func.min(exfor_indexes.c.en_inc_min).label("en_inc_min"),
            func.max(exfor_indexes.c.en_inc_max).label("en_inc_max"),
            func.max(exfor_histories.c.recorded_at).label("recorded_at"),
            func.count(distinct(exfor_histories.c.id)).label("n_history"),
        )
        .select_from(
            exfor_bib
            .join(exfor_reactions, exfor_reactions.c.entry == exfor_bib.c.entry, isouter=True)
            .join(exfor_indexes, exfor_indexes.c.entry_id == exfor_reactions.c.entry_id, isouter=True)
            .join(exfor_entry_dois, exfor_entry_dois.c.entry == exfor_bib.c.entry, isouter=True)
            .join(exfor_histories, exfor_histories.c.entry == exfor_bib.c.entry, isouter=True)
        )
        .group_by(exfor_bib.c.entry, exfor_reactions.c.entry_id)
        .order_by(exfor_bib.c.year.desc())
    )

    with engines["exfor"].connect() as connection:
        df = pd.read_sql(sql=stmt, con=connection)
        geo_df = pd.read_sql(
            select(
                exfor_institute_geo.c.x4_code,
                exfor_institute_geo.c.name,
                exfor_institute_geo.c.lat,
                exfor_institute_geo.c.lng,
            ),
            connection,
        )

    # Strip parens from facility institute code and merge geo data in pandas,
    # avoiding a slow SQL string-function join condition on the main query.
    df["_fc"] = df["main_facility_institute"].str.replace(r"[()]", "", regex=True)
    df = df.merge(geo_df, left_on="_fc", right_on="x4_code", how="left").drop(
        columns=["_fc", "x4_code"]
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


########  -------------------------------------- ##########
##         EXFOR Statistics Queries
########  -------------------------------------- ##########

def get_total_data_points() -> int:
    """Return total number of data points across all EXFOR indexes."""
    stmt = select(func.sum(exfor_indexes.c.points))
    with engines["exfor"].connect() as conn:
        result = conn.execute(stmt).scalar()
    return int(result or 0)


def get_data_points_for_entries(entry_ids: list) -> int:
    """Return total data points for a filtered set of entry_ids."""
    if not entry_ids:
        return 0
    stmt = select(func.sum(exfor_indexes.c.points)).where(
        exfor_indexes.c.entry_id.in_(entry_ids)
    )
    with engines["exfor"].connect() as conn:
        result = conn.execute(stmt).scalar()
    return int(result or 0)


# def get_committed_at_per_entry() -> dict:
#     """Return {entry: committed_at} for all current entries in exfor_history."""
#     sql = text("SELECT entry, committed_at FROM exfor_history WHERE is_current = 1")
#     try:
#         with engines["exfor"].connect() as conn:
#             df = pd.read_sql(sql, conn)
#         return dict(zip(df["entry"], df["committed_at"]))
#     except Exception:
#         return {}




########  -------------------------------------- ##########
##         Index creation
########  -------------------------------------- ##########

def ensure_indexes():
    """Create missing indexes on the EXFOR database.

    Safe to call repeatedly — every statement uses IF NOT EXISTS.
    Call once at application startup.
    """
    ddl = [
        # exfor_reactions.entry — join key with exfor_bib; not indexed in models_core
        "CREATE INDEX IF NOT EXISTS ix_exfor_reactions_entry       ON exfor_reactions (entry)",
        # exfor_indexes.entry — used by join_index_bib and cross-table lookups
        "CREATE INDEX IF NOT EXISTS ix_exfor_indexes_entry         ON exfor_indexes (entry)",
        # exfor_bib.year — ORDER BY target in entries_query / join_reaction_bib
        "CREATE INDEX IF NOT EXISTS ix_exfor_bib_year              ON exfor_bib (year)",
        # exfor_data.en_inc — range filter for thermal and energy-restricted queries
        "CREATE INDEX IF NOT EXISTS ix_exfor_data_en_inc           ON exfor_data (en_inc)",
        # Composite covering the hot path in exfor_index_query:
        #   WHERE target=? AND projectile=? AND sf6=?  [± sf5/sf7/sf8/sf9]
        "CREATE INDEX IF NOT EXISTS ix_exfor_indexes_tgt_proj_sf6  ON exfor_indexes (target, projectile, sf6)",
        # Wider composite for queries that also filter sf5 (branch / PAR)
        "CREATE INDEX IF NOT EXISTS ix_exfor_indexes_tgt_proj_sf56 ON exfor_indexes (target, projectile, sf5, sf6)",
        # exfor_histories composite — used by join_reaction_bib direct join
        "CREATE INDEX IF NOT EXISTS ix_exfor_histories_entry_cur   ON exfor_history (entry, is_current)",
    ]
    with engines["exfor"].connect() as conn:
        for stmt in ddl:
            conn.execute(text(stmt))
        conn.commit()
