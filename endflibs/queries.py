import sys
import importlib
import pandas as pd
from sqlalchemy import select, and_, or_, distinct

try:
    from config import engines
except:
    module_name = sys.modules[__name__].split(".")[0]
    config = importlib.import_module(f"{module_name}.config")
    from config import engines
from endftables_sql.scripts.models_core import (
    endf_reactions,
    endf_xs_data,
    endf_angle_data,
    endf_residual_data,
    endf_n_residual_data,
    endf_fy_data,
)
from ..utilities.util import libstyle_nuclide_expression
from ..utilities.reaction import MT_BRANCH_LIST_FY

connection = engines["endftables"].connect()


######## -------------------------------------- ########
#    Queries for endftables
######## -------------------------------------- ########


def lib_index_query(input_store):
    obs_type = input_store.get("obs_type").upper()
    elem = input_store.get("target_elem")
    mass = input_store.get("target_mass")
    reaction = input_store.get("reaction")

    target = libstyle_nuclide_expression(elem, mass)

    queries = [
        endf_reactions.c.target == target,
        endf_reactions.c.projectile == reaction.split(",")[0].lower(),
    ]

    if obs_type in ("XS", "DA", "FY", "TH"):
        if input_store.get("mt"):
            mt = input_store.get("mt")
            queries.append(endf_reactions.c.mt == int(mt))

    if obs_type == "TH":
        obs_type = "xs"

    if obs_type == "RP":
        obs_type = "residual"
        rp_elem = input_store.get("rp_elem")
        rp_mass = input_store.get("rp_mass")
        residual = libstyle_nuclide_expression(rp_elem, rp_mass)
        queries.append(endf_reactions.c.residual == residual)

    if obs_type == "DA":
        obs_type = "angle"
        # queries.append(endf_reactions.c.process == reaction.split(",")[1].upper())

    if obs_type == "DE":
        obs_type = "energy"
        # queries.append(endf_reactions.c.process == reaction.split(",")[1].upper())

    queries.append(endf_reactions.c.obs_type == obs_type.lower())

    stmt = select(endf_reactions.c.reaction_id, endf_reactions.c.evaluation).where(
        and_(*queries)
    )

    with engines["endftables"].connect() as conn:
        results = conn.execute(stmt).fetchall()

    return {row.reaction_id: row.evaluation for row in results}


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


def lib_data_query(input_store, ids):
    obs_type = input_store["obs_type"].upper()
    if obs_type == "XS":
        return lib_xs_data_query(ids, thermal=False)
    elif obs_type == "TH":
        return lib_xs_data_query(ids, thermal=True)
    elif obs_type == "FY":
        return lib_fy_data_query(ids)
    elif obs_type == "DA":
        return lib_da_data_query(ids)
    elif obs_type == "RP":
        inc_pt = input_store["reaction"].split(",")[0].lower()
        return lib_residual_data_query(inc_pt, ids)


def lib_xs_data_query(ids, thermal):
    queries = [endf_xs_data.c.reaction_id.in_(ids)]
    if thermal:
        queries.append(endf_xs_data.c.en_inc == 2.53e-8)
    stmt = select(endf_xs_data).where(and_(*queries))
    with engines["endftables"].connect() as conn:
        df = pd.DataFrame(
            conn.execute(stmt).fetchall(), columns=stmt.selected_columns.keys()
        )
    return df


def lib_da_data_query(ids):
    stmt = select(endf_angle_data).where(endf_angle_data.c.reaction_id.in_(ids))
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
    stmt = select(distinct(endf_reactions.c.process)).order_by(endf_reactions.c.target)
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


def get_xs_reaction_list(input_store):

    columns = [
        endf_reactions.c.reaction_id,
        endf_reactions.c.evaluation,
        endf_reactions.c.target,
        endf_reactions.c.projectile,
        endf_reactions.c.process,
        endf_reactions.c.mt,
    ]

    if not input_store:
        return pd.DataFrame(columns=[col.name for col in columns])

    conditions = []
    conditions.append(endf_reactions.c.obs_type == "xs")

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
            )
        )

    with engines["endftables"].connect() as conn:
        df = pd.read_sql(stmt, conn)

    return df
