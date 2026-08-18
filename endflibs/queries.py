import importlib
from functools import lru_cache
import math
import numpy as np
import pandas as pd
from sqlalchemy import select, and_, distinct, text

try:
    from config import engines
except Exception:
    module_name = __name__.split(".")[0]
    config = importlib.import_module(f"{module_name}.config")
    engines = config.engines

from endftables_sql.scripts.models_core import (
    endf_reactions,
    endf_xs_data,
    endf_angle_data,
    endf_residual_data,
    endf_n_residual_data,
    endf_fy_data,
    endf_ddx_data,
    resonancetable_data,
)
from ..utilities.util import get_number_from_string, libstyle_nuclide_expression
from ..utilities.elem import elemtoz
from ..utilities.obs_types import GAMMA_PRODUCTION_OBS_TYPE, SCALAR_OBS



######## -------------------------------------- ########
#    Index queries for endftables
######## -------------------------------------- ########

def _lib_cond_mt(input_store: dict) -> list:
    """Extra conditions for obs types that support MT filtering (XS, TH, DA, FY).

    For level-specific inelastic reactions (e.g. n,inl1 / n,inl2) get_mt()
    returns None because the level suffix is not in the reaction table.  Map
    level_num → MT via the ENDF convention MT = 50 + level_num so the library
    query stays narrow (MT 51 for level 1, MT 52 for level 2, …).
    """
    mt = input_store.get("mt")
    if mt is None:
        level_num = input_store.get("level_num")
        if level_num is not None:
            mt = 50 + int(level_num)
    return [endf_reactions.c.mt == int(mt)] if mt else []


def _lib_cond_residual(input_store: dict) -> list:
    """Extra conditions for residual product (RP) queries."""
    if input_store.get("rp_elem") and input_store.get("rp_mass"):
        residual = libstyle_nuclide_expression(
            input_store.get("rp_elem"), input_store.get("rp_mass")
        )
        return [endf_reactions.c.residual == residual]
    else:
        return []


# Maps EXFOR SF4 codes (used on the DDX page to select the emitted particle)
# to the outgoing-particle convention stored in endf_reactions.residual for
# obs_type="ddx" (set by endftables_sql's load_ddx loader).
_DDX_SF4_TO_OUTGOING: dict = {
    "0-NN-1": "n", 
    "0-G-0": "g", 
    "1-H-1": "p", 
    "1-H-2": "d",
    "1-H-3": "t", 
    "2-HE-3": "he3", 
    "2-HE-4": "a",
}


def _lib_cond_ddx(input_store: dict) -> list:
    """Extra conditions for DDX queries — restrict to the emitted particle (SF4)."""
    outgoing = _DDX_SF4_TO_OUTGOING.get(str(input_store.get("sf4") or "").upper())
    return [endf_reactions.c.residual == outgoing] if outgoing else []


def _lib_cond_trn(input_store: dict) -> list:
    """Transmission is derived from total cross section, MF=3/MT=1."""
    return [endf_reactions.c.mt == 1]


# def _lib_cond_resonance(input_store: dict) -> list:
#     """Extra conditions for resonance parameters query."""
#     [endf_reactions.c.residual == residual]
#     return 


# Maps the page-level obs_type key to the corresponding endf_reactions.obs_type
# column value and a callable that returns any additional query conditions.
# db_obs_type=None means no endf_reactions entry — use resonancetable_data instead.
LIB_OBS_TYPE_CONDITION: dict = {
    "XS":   {"db_obs_type": "xs",       "extra": _lib_cond_mt},
    GAMMA_PRODUCTION_OBS_TYPE: {
        "db_obs_type": "gamma_production",
        "extra": _lib_cond_mt,
    },
    "SF":   {"db_obs_type": "xs",       "extra": _lib_cond_mt},
    "SFC":  {"db_obs_type": "xs",       "extra": _lib_cond_mt},
    "RP":   {"db_obs_type": "residual", "extra": _lib_cond_residual},
    "DA":   {"db_obs_type": "angle",    "extra": _lib_cond_mt},
    "DDX":  {"db_obs_type": "ddx",      "extra": _lib_cond_ddx},
    "FY":   {"db_obs_type": "fy",       "extra": _lib_cond_mt},
    "DE":   {"db_obs_type": "energy",   "extra": lambda _: []},
    "TRN":  {"db_obs_type": "xs",       "extra": _lib_cond_trn},
    # These use resonancetable_data, not endf_reactions → db_obs_type=None
    "RESONANCE":   {"db_obs_type": None, "extra": lambda _: []},
    "TH":   {"db_obs_type": "thermal",  "extra": _lib_cond_residual},
    "MACS": {"db_obs_type": "macs",     "extra": lambda _: []},
    "RI":   {"db_obs_type": "integral", "extra": lambda _: []},
    # Wildcard patterns — matched with LIKE in lib_index_query
    # Per-quantity entries (exact match)
    "D0":   {"db_obs_type": "D0",       "extra": lambda _: []},
    "D1":   {"db_obs_type": "D1",       "extra": lambda _: []},
    "D2":   {"db_obs_type": "D2",       "extra": lambda _: []},
    "S0":   {"db_obs_type": "S0",       "extra": lambda _: []},
    "S1":   {"db_obs_type": "S1",       "extra": lambda _: []},
    "GG0":  {"db_obs_type": "gamgam0",  "extra": lambda _: []},
    "GG1":  {"db_obs_type": "gamgam1",  "extra": lambda _: []},
    "RAD":  {"db_obs_type": "R",        "extra": lambda _: []},
    "STF":  {"db_obs_type": "STF",      "extra": lambda _: []},
}


def lib_index_query(input_store):
    obs_type = input_store.get("obs_type", "").upper()
    condition = LIB_OBS_TYPE_CONDITION.get(obs_type)

    projectile = input_store.get("inc_pt", "").lower() if input_store.get("inc_pt").upper() != "HE3" else "h"
    process = input_store.get("reaction", "").split(",")[1]
    target = libstyle_nuclide_expression(
        input_store.get("target_elem"), input_store.get("target_mass")
    )

    if condition is None or condition["db_obs_type"] is None:
        return {}  # no endf_reactions entry for this obs_type — use resonancetable_data instead

    queries = [
        endf_reactions.c.target == target,
        endf_reactions.c.projectile == projectile,
        endf_reactions.c.obs_type == condition["db_obs_type"],
        *condition["extra"](input_store),
    ]

    if obs_type in {"TH", "MACS", "RI"}:
        queries += [endf_reactions.c.process == process]

    if obs_type in SCALAR_OBS:
        stmt = select(endf_reactions.c.reaction_id, 
                      endf_reactions.c.evaluation, 
                      endf_reactions.c.residual, 
                      endf_reactions.c.year).where(and_(*queries))

        with engines["endftables"].connect() as conn:
            results = conn.execute(stmt).fetchall()

        return {row.reaction_id:{
            "evaluation": row.evaluation, 
            "residual": row.residual, 
            "year": row.year
            } for row in results}
    
    else:
        stmt = select(endf_reactions.c.reaction_id, 
                      endf_reactions.c.evaluation).where(
            and_(*queries)
        )
        with engines["endftables"].connect() as conn:
            results = conn.execute(stmt).fetchall()
        return {row.reaction_id: row.evaluation for row in results}


@lru_cache(maxsize=2048)
def lib_available_reactions_query(obs_type, elem, mass, projectile):
    """Return evaluated-library reaction identifiers available for one target.

    This intentionally reads only the compact endf_reactions index and returns
    distinct process/MT pairs so the reaction dropdown can be annotated without
    issuing one query per option.
    """
    obs_type = (obs_type or "").upper()
    condition = LIB_OBS_TYPE_CONDITION.get(obs_type)
    if condition is None or condition["db_obs_type"] is None:
        return []

    target = libstyle_nuclide_expression(elem, mass)
    projectile = "h" if (projectile or "").upper() == "HE3" else (projectile or "").lower()

    stmt = (
        select(
            endf_reactions.c.process,
            endf_reactions.c.mt,
        )
        .distinct()
        .where(
            and_(
                endf_reactions.c.target == target,
                endf_reactions.c.projectile == projectile,
                endf_reactions.c.obs_type == condition["db_obs_type"],
                *([endf_reactions.c.mt == 1] if obs_type == "TRN" else []),
                *(
                    [endf_reactions.c.mt == 202]
                    if obs_type == "GPROD" else []
                ),
            )
        )
    )

    with engines["endftables"].connect() as conn:
        results = conn.execute(stmt).fetchall()

    return [
        {"process": row.process, "mt": row.mt}
        for row in results
    ]


def lib_residual_nuclide_list(elem, mass, inc_pt):
    target = libstyle_nuclide_expression(elem, mass)

    stmt = select(endf_reactions.c.residual).where(
        endf_reactions.c.obs_type == "residual",
        endf_reactions.c.projectile == inc_pt.lower(),
        endf_reactions.c.target == target,
    )

    with engines["endftables"].connect() as conn:
        results = conn.execute(stmt).fetchall()

    return [row.residual for row in results] if results else []



######## -------------------------------------- ########
#    Data queries for endftables
######## -------------------------------------- ########

def lib_data_query(input_store, ids):
    obs_type = input_store["obs_type"].upper()
    if obs_type in {"XS", GAMMA_PRODUCTION_OBS_TYPE}:
        return lib_xs_data_query(ids, thermal=False)
    elif obs_type in {"SF", "SFC"}:
        return lib_sfactor_data_query(input_store, ids)
    elif obs_type == "TRN":
        return lib_trn_data_query(input_store, ids)
    elif obs_type == "FY":
        return lib_fy_data_query(ids)
    elif obs_type == "DA":
        return lib_da_data_query(ids)
    elif obs_type == "RP":
        inc_pt = input_store["reaction"].split(",")[0].lower()
        return lib_residual_data_query(inc_pt, ids)
    elif obs_type in SCALAR_OBS:
        return lib_resonance_data_query(ids)

    return pd.DataFrame()


def lib_xs_data_query(ids, thermal, energy_range=None):
    queries = [endf_xs_data.c.reaction_id.in_(ids)]
    if thermal:
        queries.append(endf_xs_data.c.en_inc == 2.53e-8)
    energy_range = energy_range or [None, None]
    start_mev = energy_range[0] if len(energy_range) > 0 else None
    end_mev = energy_range[1] if len(energy_range) > 1 else None
    if start_mev is not None:
        queries.append(endf_xs_data.c.en_inc >= float(start_mev))
    if end_mev is not None:
        queries.append(endf_xs_data.c.en_inc <= float(end_mev))
    stmt = select(endf_xs_data).where(and_(*queries))
    with engines["endftables"].connect() as conn:
        df = pd.DataFrame(
            conn.execute(stmt).fetchall(), columns=stmt.selected_columns.keys()
        )
    return df


_SFACTOR_PROJECTILES = {
    "P": (1, 1),
    "D": (1, 2),
    "T": (1, 3),
    "HE3": (2, 3),
    "H": (2, 3),  # endftables convention for a helium-3 projectile
    "A": (2, 4),
}


def sfactor_system_properties(target_elem, target_mass, projectile):
    """Return charges, reduced mass, and the lab-to-CM energy factor.

    Nuclear masses are approximated by mass numbers, consistently with the
    nuclide information already used elsewhere in Data Explorer.
    """
    projectile_key = str(projectile or "").upper()
    projectile_properties = _SFACTOR_PROJECTILES.get(projectile_key)
    target_elem = str(target_elem or "")
    target_z = (
        target_elem
        if target_elem.isnumeric()
        else elemtoz(target_elem.capitalize())
    )
    target_a = get_number_from_string(str(target_mass or ""))
    if projectile_properties is None or not target_z or not target_a:
        raise ValueError("S-factor construction requires isotopic target and projectile")

    projectile_z, projectile_a = projectile_properties
    target_z = int(target_z)
    target_a = int(target_a)
    reduced_mass = projectile_a * target_a / (projectile_a + target_a)
    return {
        "projectile_z": projectile_z,
        "target_z": target_z,
        "reduced_mass_u": reduced_mass,
        "lab_to_cm": target_a / (projectile_a + target_a),
    }


def construct_sfactor_from_xs(xs_df, target_elem, target_mass, projectile):
    """Construct an astrophysical S factor from evaluated cross sections.

    ``endf_xs_data`` stores laboratory incident energy in MeV and cross
    section in barns.  The returned ``en_inc`` is center-of-mass energy in
    MeV and ``data`` is S(E) in eV*barn.
    """
    if xs_df is None or xs_df.empty:
        return pd.DataFrame()

    try:
        properties = sfactor_system_properties(
            target_elem, target_mass, projectile
        )
    except (AttributeError, KeyError, TypeError, ValueError):
        return pd.DataFrame()

    df = xs_df.copy()
    lab_energy = pd.to_numeric(df["en_inc"], errors="coerce")
    cross_section = pd.to_numeric(df["data"], errors="coerce")
    cm_energy = lab_energy * properties["lab_to_cm"]

    valid = (
        lab_energy.notna()
        & cross_section.notna()
        & (cm_energy > 0)
        & (cross_section >= 0)
    )
    df = df.loc[valid].copy()
    if df.empty:
        return df

    lab_energy = lab_energy.loc[valid].astype(float)
    cm_energy = cm_energy.loc[valid].astype(float)
    cross_section = cross_section.loc[valid].astype(float)

    # IAEA/EMPIRE convention: E in MeV, masses in u.
    two_pi_eta = (
        0.9895106848
        * properties["projectile_z"]
        * properties["target_z"]
        * np.sqrt(properties["reduced_mass_u"] / cm_energy)
    )

    sfactor = np.zeros(len(df), dtype=float)
    nonzero = cross_section.to_numpy() > 0
    log_sfactor = (
        np.log(cross_section.to_numpy()[nonzero])
        + np.log(cm_energy.to_numpy()[nonzero] * 1.0e6)
        + two_pi_eta.to_numpy()[nonzero]
    )
    finite = log_sfactor <= np.log(np.finfo(float).max)
    sfactor_nonzero = np.full(log_sfactor.shape, np.nan)
    sfactor_nonzero[finite] = np.exp(log_sfactor[finite])
    sfactor[nonzero] = sfactor_nonzero

    df["en_inc_lab"] = lab_energy.to_numpy()
    df["en_inc"] = cm_energy.to_numpy()
    df["xs_data"] = cross_section.to_numpy()
    df["data"] = sfactor
    df["sfactor_unit"] = "eV*b"
    df["reduced_mass_u"] = properties["reduced_mass_u"]
    return df[np.isfinite(df["data"])].reset_index(drop=True)


def lib_sfactor_data_query(input_store, ids):
    xs_df = lib_xs_data_query(
        ids,
        thermal=False,
        energy_range=input_store.get("energy_range"),
    )
    return construct_sfactor_from_xs(
        xs_df,
        input_store.get("target_elem"),
        input_store.get("target_mass"),
        input_store.get("inc_pt"),
    )


def lib_trn_data_query(input_store, ids):
    """Return derived transmission from total cross section.

    The TRN page passes areal number densities in atoms/barn.  With sigma in
    barns, transmission is exp(-N*sigma).  One output curve is produced for
    each (reaction_id, areal_density) pair.
    """
    thicknesses = input_store.get("trn_areal_densities") or []
    if not thicknesses:
        return pd.DataFrame()

    xs_df = lib_xs_data_query(
        ids,
        thermal=False,
        energy_range=input_store.get("energy_range"),
    )
    if xs_df.empty:
        return xs_df

    frames = []
    for thickness in thicknesses:
        try:
            areal_density = float(thickness)
        except (TypeError, ValueError):
            continue
        if areal_density <= 0:
            continue

        df = xs_df.copy()
        df["data"] = df["data"].astype(float).map(
            lambda sigma: math.exp(-areal_density * sigma)
        )
        df["trn_areal_density"] = areal_density
        frames.append(df)

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def lib_da_data_query(ids):
    stmt = select(endf_angle_data).where(endf_angle_data.c.reaction_id.in_(ids))
    with engines["endftables"].connect() as conn:
        df = pd.DataFrame(
            conn.execute(stmt).fetchall(), columns=stmt.selected_columns.keys()
        )
    return df


def lib_da_distinct_query(ids):
    """Return distinct (reaction_id, en_inc, angle) without data values.
    Used to populate slicer dropdowns without loading the full dataset."""
    stmt = (
        select(
            endf_angle_data.c.reaction_id,
            endf_angle_data.c.en_inc,
            endf_angle_data.c.angle,
        )
        .where(endf_angle_data.c.reaction_id.in_(ids))
        .distinct()
    )
    with engines["endftables"].connect() as conn:
        df = pd.DataFrame(
            conn.execute(stmt).fetchall(),
            columns=["reaction_id", "en_inc", "angle"],
        )
    return df


def lib_da_data_query_at_energy(ids, en_target):
    """Return all angle rows exactly at the selected incident energy."""
    if en_target is None:
        return pd.DataFrame()
    t = float(en_target)
    tol = max(abs(t) * 1e-10, 1e-12)
    stmt = select(endf_angle_data).where(
        and_(
            endf_angle_data.c.reaction_id.in_(ids),
            endf_angle_data.c.en_inc.between(t - tol, t + tol),
        )
    )
    with engines["endftables"].connect() as conn:
        df = pd.DataFrame(
            conn.execute(stmt).fetchall(), columns=stmt.selected_columns.keys()
        )
    return df


def lib_da_data_query_at_angle(ids, angle_target):
    """Return all energy rows exactly at the selected angle."""
    if angle_target is None:
        return pd.DataFrame()
    t = float(angle_target)
    tol = max(abs(t) * 1e-10, 1e-12)
    stmt = select(endf_angle_data).where(
        and_(
            endf_angle_data.c.reaction_id.in_(ids),
            endf_angle_data.c.angle.between(t - tol, t + tol),
        )
    )
    with engines["endftables"].connect() as conn:
        df = pd.DataFrame(
            conn.execute(stmt).fetchall(), columns=stmt.selected_columns.keys()
        )
    return df


def lib_ddx_distinct_query(ids):
    """Return distinct (reaction_id, en_inc, angle) without data values.
    Used to populate the DDX slicer dropdowns without loading the full dataset."""
    stmt = (
        select(
            endf_ddx_data.c.reaction_id,
            endf_ddx_data.c.en_inc,
            endf_ddx_data.c.angle,
        )
        .where(endf_ddx_data.c.reaction_id.in_(ids))
        .distinct()
    )
    with engines["endftables"].connect() as conn:
        df = pd.DataFrame(
            conn.execute(stmt).fetchall(),
            columns=["reaction_id", "en_inc", "angle"],
        )
    return df


def lib_ddx_data_query(ids, en_target=None, angle_target=None):
    """Return d²σ/dΩdE rows for the given reaction ids at the selected
    incident-energy slice, optionally narrowed to one emission angle."""
    queries = [endf_ddx_data.c.reaction_id.in_(ids)]
    if en_target is not None:
        t = float(en_target)
        tol = max(abs(t) * 1e-10, 1e-12)
        queries.append(endf_ddx_data.c.en_inc.between(t - tol, t + tol))
    if angle_target is not None:
        t = float(angle_target)
        tol = max(abs(t) * 1e-10, 1e-12)
        queries.append(endf_ddx_data.c.angle.between(t - tol, t + tol))

    stmt = (
        select(
            endf_ddx_data.c.id,
            endf_ddx_data.c.reaction_id,
            endf_ddx_data.c.en_inc,
            endf_ddx_data.c.en_out,
            endf_ddx_data.c.data,
            endf_ddx_data.c.zap,
            endf_ddx_data.c.outgoing_particle,
            endf_ddx_data.c.frame,
            endf_ddx_data.c.angle,
            endf_reactions.c.evaluation,
            endf_reactions.c.process,
            endf_reactions.c.mt,
            endf_reactions.c.residual,
        )
        .select_from(
            endf_ddx_data.join(
                endf_reactions,
                endf_ddx_data.c.reaction_id == endf_reactions.c.reaction_id,
            )
        )
        .where(and_(*queries))
    )
    with engines["endftables"].connect() as conn:
        df = pd.DataFrame(
            conn.execute(stmt).fetchall(), columns=stmt.selected_columns.keys()
        )
    return df


def lib_residual_data_query(inc_pt, ids):
    table = endf_n_residual_data if inc_pt.lower() == "n" else endf_residual_data
    stmt = select(table).where(table.c.reaction_id.in_(ids))
    with engines["endftables"].connect() as conn:
        df = pd.DataFrame(
            conn.execute(stmt).fetchall(), columns=stmt.selected_columns.keys()
        )
    return df


def lib_fy_data_query(ids):
    stmt = select(endf_fy_data).where(endf_fy_data.c.reaction_id.in_(ids))
    with engines["endftables"].connect() as conn:
        df = pd.DataFrame(
            conn.execute(stmt).fetchall(), columns=stmt.selected_columns.keys()
        )
    return df


def lib_resonance_data_query(ids):
    stmt = select(
        resonancetable_data.c.reaction_id,
        resonancetable_data.c.value,
        resonancetable_data.c.dvalue,
    ).where(resonancetable_data.c.reaction_id.in_(ids))
    with engines["endftables"].connect() as conn:
        df = pd.DataFrame(
            conn.execute(stmt).fetchall(), columns=stmt.selected_columns.keys()
        )
    return df




######## -------------------------------------- ########
#    Queries for multiple ENDF-6 file access
######## -------------------------------------- ########


def get_unique_target():
    stmt = select(distinct(endf_reactions.c.target)).order_by(endf_reactions.c.target)
    with engines["endftables"].connect() as conn:
        result = conn.execute(stmt)
        targets = [row[0] for row in result.fetchall()]
    return targets


def get_unique_proces():
    stmt = select(distinct(endf_reactions.c.process)).order_by(endf_reactions.c.process)
    with engines["endftables"].connect() as conn:
        result = conn.execute(stmt)
        targets = [row[0] for row in result.fetchall()]
    return targets


def get_unique_xs_mt():
    stmt = (
        select(distinct(endf_reactions.c.mt))
        .order_by(endf_reactions.c.mt)
        .where(endf_reactions.c.mf == 3)
    )
    with engines["endftables"].connect() as conn:
        result = conn.execute(stmt)
        targets = [row[0] for row in result.fetchall()]
    return targets


def get_all_endf_reaction():
    stmt = select(endf_reactions)
    with engines["endftables"].connect() as conn:
        result = conn.execute(stmt)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())

    return df


def get_reaction_list(input_store):

    columns = [
        endf_reactions.c.reaction_id,
        endf_reactions.c.obs_type,
        endf_reactions.c.evaluation,
        endf_reactions.c.target,
        endf_reactions.c.projectile,
        endf_reactions.c.process,
        endf_reactions.c.mt,
    ]

    obs_type = input_store.get("obs_type")

    if obs_type == "RP":
        obs_type = "residual"

    elif obs_type == "DA":
        obs_type = "angle"
        columns.append(endf_reactions.c.en_inc,)

    elif obs_type == "DE":
        obs_type = "energy"

    else:
        obs_type = "xs"



    if not input_store:
        return pd.DataFrame(columns=[col.name for col in columns])

    conditions = []

    conditions.append(endf_reactions.c.obs_type == obs_type)

    evaluations = input_store.get("evaluation")
    if evaluations:
        conditions.append(endf_reactions.c.evaluation.in_(evaluations))

    projectiles = input_store.get("projectile")
    if projectiles:
        conditions.append(endf_reactions.c.projectile.in_(projectiles))

    targets = input_store.get("target")
    if targets:
        conditions.append(endf_reactions.c.target.in_(targets))

    mts = input_store.get("mt")
    if mts:
        conditions.append(endf_reactions.c.mt.in_(mts))

    # reactions = input_store.get("reactions")
    # if reactions:
    #     conditions.append(endf_reactions.c.reactions.in_(reactions))

    if not conditions:
        return pd.DataFrame(columns=[col.name for col in columns])

    if conditions:
        stmt = (
            select(*columns)
            .where(*conditions)
            .order_by(
                endf_reactions.c.evaluation,
                endf_reactions.c.target,
                endf_reactions.c.mt,
                endf_reactions.c.en_inc,
            )
        )

    with engines["endftables"].connect() as conn:
        df = pd.read_sql(stmt, conn)

    return df


######## -------------------------------------- ########
#    Queries for resonancetable_data
#    (MACS, thermal XS, resonance parameters from
#     RIPL-3, Mughabghab, NDLs, etc.)
#
#    Schema after redesign
#    ---------------------
#    endf_reactions.obs_type now carries the full observable identity:
#      "macs"           — Maxwellian-averaged (n,g)
#      "thermal"        — thermal XS; process column holds channel ("g","f","el","a","p","tot")
#                         residual column distinguishes -g/-m variants
#      "D0","D1","D2"   — level spacing
#      "S0","S1"        — strength function
#      "integral"       — resonance integral; process="g" (Ig) or "f" (If)
#      "R"              — scattering radius
#      "gamgam0","gamgam1" — averaged gamma width
#
#    resonancetable_data has NO quantity column.
#
#    Comparable experimental EXFOR data are pre-extracted as text files in:
#      /Users/okumuras/Documents/nucleardata/EXFOR/thermaldata/
#      /Users/okumuras/Documents/nucleardata/EXFOR/resonance_data/
######## -------------------------------------- ########

# Resonance parameter obs_types. If/Ig stored as "integral" with process f/g.
_RESONANCE_PARAM_OBS = [
    "D0", "D1", "D2", "integral", "R", "S0", "S1", "gamgam0", "gamgam1",
]

# Reusable join fragment
_rt_join = resonancetable_data.join(
    endf_reactions,
    resonancetable_data.c.reaction_id == endf_reactions.c.reaction_id,
)


def _to_obs_type(data_type: str, quantity: str) -> str:
    """
    Convert legacy (data_type, quantity) pair to obs_type stored in endf_reactions.
    resonance_param + D0  →  "D0"
    resonance_param + If  →  "integral"
    resonance_param + Ig  →  "integral"
    thermal + ng          →  "thermal"
    macs + ng             →  "macs"
    """
    if data_type == "resonance_param":
        return "integral" if quantity in ("If", "Ig") else quantity
    return data_type


def resonancetable_index_query(input_store: dict) -> pd.DataFrame:
    """
    Return all sources for a given nuclide + observable, ordered with
    'selected' first then alphabetically.

    input_store keys
    ----------------
    target_elem : str      element symbol, e.g. 'U'
    target_mass : int/str  mass number, e.g. 235
    obs_type    : str      obs_type value, e.g. 'thermal', 'macs', 'D0', 'Ig'
                           — OR provide data_type + quantity for backward compat
    data_type   : str      'macs' | 'thermal' | 'resonance_param'  (legacy)
    quantity    : str      e.g. 'ng', 'D0', 'Ig'                   (legacy)
    process     : str      optional channel filter for thermal, e.g. 'g', 'f'

    Returns
    -------
    DataFrame: source, year, process, residual, value, dvalue, n_exper,
               rel_dev_comp, rel_dev_ndl, rel_dev_exfor, rel_dev_all, spectrum
    """
    elem      = input_store.get("target_elem", "")
    mass      = str(input_store.get("target_mass", 0))
    target    = libstyle_nuclide_expression(elem, mass)
    obs_type  = input_store.get("obs_type") or _to_obs_type(
        input_store.get("data_type", ""), input_store.get("quantity", "")
    )
    process   = input_store.get("process")

    conditions = [
        endf_reactions.c.target   == target,
        endf_reactions.c.obs_type == obs_type,
    ]
    if process is not None:
        conditions.append(endf_reactions.c.process == process)

    stmt = (
        select(
            endf_reactions.c.evaluation.label("source"),
            endf_reactions.c.year,
            endf_reactions.c.process,
            endf_reactions.c.residual,
            resonancetable_data.c.value,
            resonancetable_data.c.dvalue,
            resonancetable_data.c.n_exper,
            resonancetable_data.c.rel_dev_comp,
            resonancetable_data.c.rel_dev_ndl,
            resonancetable_data.c.rel_dev_exfor,
            resonancetable_data.c.rel_dev_all,
            resonancetable_data.c.spectrum,
        )
        .select_from(_rt_join)
        .where(and_(*conditions))
        .order_by(
            (endf_reactions.c.evaluation != "selected").asc(),
            endf_reactions.c.evaluation.asc(),
        )
    )

    with engines["endftables"].connect() as conn:
        df = pd.read_sql(stmt, conn)

    return df


def resonancetable_selected_query(input_store: dict) -> pd.Series:
    """
    Return the single 'selected' (recommended) value for a nuclide + observable.
    Returns a pandas Series, or an empty Series if not found.
    """
    df = resonancetable_index_query(input_store)
    sel = df[df["source"] == "selected"]
    return sel.iloc[0] if not sel.empty else pd.Series(dtype=float)


def resonancetable_nuclide_list(obs_type: str, source: str = "selected") -> pd.DataFrame:
    """
    Return all nuclides with a value for the given obs_type, ordered by target.
    Useful for populating dropdowns and overview plots.

    Parameters
    ----------
    obs_type : e.g. 'macs', 'thermal', 'D0', 'Ig'
    source   : filter to one source (default 'selected'); None = all sources

    Returns
    -------
    DataFrame: target, source, year, process, residual, value, dvalue
    """
    conditions = [endf_reactions.c.obs_type == obs_type]
    if source is not None:
        conditions.append(endf_reactions.c.evaluation == source)

    stmt = (
        select(
            endf_reactions.c.target,
            endf_reactions.c.evaluation.label("source"),
            endf_reactions.c.year,
            endf_reactions.c.process,
            endf_reactions.c.residual,
            resonancetable_data.c.value,
            resonancetable_data.c.dvalue,
        )
        .select_from(_rt_join)
        .where(and_(*conditions))
        .order_by(endf_reactions.c.target)
    )

    with engines["endftables"].connect() as conn:
        df = pd.read_sql(stmt, conn)

    return df


def resonancetable_source_list(obs_type: str) -> list[str]:
    """Return distinct sources for a given obs_type, with 'selected' first."""
    stmt = (
        select(distinct(endf_reactions.c.evaluation))
        .select_from(_rt_join)
        .where(endf_reactions.c.obs_type == obs_type)
        .order_by(endf_reactions.c.evaluation)
    )

    with engines["endftables"].connect() as conn:
        rows = conn.execute(stmt).fetchall()

    sources = [r[0] for r in rows]
    if "selected" in sources:
        sources.insert(0, sources.pop(sources.index("selected")))
    return sources


def resonancetable_obs_type_list(data_type: str) -> list[str]:
    """
    Return the distinct obs_type values stored for a given data category.

    data_type 'resonance_param' returns the individual param names
    (D0, S0, Ig, …) since those are stored directly as obs_type.
    'thermal' and 'macs' return ['thermal'] / ['macs'] plus the distinct
    process values available.
    """
    if data_type == "resonance_param":
        stmt = (
            select(distinct(endf_reactions.c.obs_type))
            .select_from(_rt_join)
            .where(endf_reactions.c.obs_type.in_(_RESONANCE_PARAM_OBS))
            .order_by(endf_reactions.c.obs_type)
        )
    else:
        stmt = (
            select(distinct(endf_reactions.c.obs_type))
            .select_from(_rt_join)
            .where(endf_reactions.c.obs_type == data_type)
            .order_by(endf_reactions.c.obs_type)
        )
    with engines["endftables"].connect() as conn:
        rows = conn.execute(stmt).fetchall()
    return [r[0] for r in rows]


########  -------------------------------------- ##########
##         Index creation
########  -------------------------------------- ##########
