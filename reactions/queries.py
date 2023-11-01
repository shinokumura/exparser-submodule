import pandas as pd

from config import session_lib, engines
from endftables_sql.models import (
    Endf_Reactions,
    Endf_XS_Data,
    Endf_ANGLE_Data,
    Endf_Residual_Data,
    Endf_N_Residual_Data,
    Endf_FY_Data,
)
from submodules.utilities.util import libstyle_nuclide_expression

connection = engines["endftables"].connect()

######## -------------------------------------- ########
#    Queries for endftables
######## -------------------------------------- ########
def lib_query(input_store):
    # print(input_store)
    type = input_store.get("type").upper()
    elem = input_store.get("target_elem")
    mass = input_store.get("target_mass")
    reaction = input_store.get("reaction")
    
    target = libstyle_nuclide_expression(elem, mass)
    queries = [
        Endf_Reactions.target == target,
        Endf_Reactions.projectile == reaction.split(",")[0].lower(),
    ]

    if type == "XS" or type == "DA" or type == "FY":
        mt = input_store.get("mt")
        queries.append(
            Endf_Reactions.mt == mt.zfill(3)
        )  # if mt is not None else Endf_Reactions.mt is not None)


    elif type == "RP":
        type = "residual"
        rp_elem = input_store.get("rp_elem")
        rp_mass = input_store.get("rp_mass")
        residual = libstyle_nuclide_expression(rp_elem, rp_mass)
        queries.append(Endf_Reactions.residual == residual)


    elif type == "DA":
        type == "angle"
        queries.append(Endf_Reactions.process == reaction.split(",")[1].upper())


    elif type == "DE":
        type == "energy"


    queries.append(Endf_Reactions.type == type.lower())
    ## Establish session to the endftable database
    reac = session_lib().query(Endf_Reactions).filter(*queries).all()

    libs = {}
    for r in reac:
        # print(r.reaction_id, r.evaluation, r.target, r.projectile, r.process, r.residual, r.mt)
        libs[r.reaction_id] = r.evaluation

    print(libs)
    return libs


def lib_residual_nuclide_list(elem, mass, inc_pt):
    # print(input_store)
    target = libstyle_nuclide_expression(elem, mass)

    data = (
        session_lib()
        .query(Endf_Reactions.residual)
        .filter(Endf_Reactions.type == "residual", 
                Endf_Reactions.projectile == inc_pt.lower(),
                Endf_Reactions.target == target
                )
            ).all()
    
    if data:
        return sorted( [d[0] for d in data] )




def lib_data_query(input_store, ids):
    type = input_store["type"].upper()

    if type == "XS":
        lib_xs_data_query(ids)
    elif type == "FY":
        lib_fy_data_query(ids)
    elif type == "DA":
        lib_da_data_query(ids)
    elif type == "RP":
        lib_residual_data_query(input_store["reaction"].split(",")[0].lower(), ids)



def lib_xs_data_query(ids):
    # connection = engines["endftables"].connect()
    data = (
        session_lib()
        .query(Endf_XS_Data)
        .filter(Endf_XS_Data.reaction_id.in_(tuple(ids)))
    )

    df = pd.read_sql(
        sql=data.statement,
        con=connection,
    )

    return df


def lib_da_data_query(ids):
    # connection = engines["endftables"].connect()
    data = (
        session_lib()
        .query(Endf_ANGLE_Data)
        .filter(Endf_ANGLE_Data.reaction_id.in_(tuple(ids)))
    )

    df = pd.read_sql(
        sql=data.statement,
        con=connection,
    )

    return df



def lib_residual_data_query(inc_pt, ids):
    # connection = engines["endftables"].connect()

    if inc_pt.lower() == "n":
        data = (
            session_lib()
            .query(Endf_N_Residual_Data)
            .filter(Endf_N_Residual_Data.reaction_id.in_(tuple(ids)))
        )
    else:
        data = (
            session_lib()
            .query(Endf_Residual_Data)
            .filter(Endf_Residual_Data.reaction_id.in_(tuple(ids)))
        )

    df = pd.read_sql(
        sql=data.statement,
        con=connection,
    )

    return df


def lib_fy_data_query(ids):
    # connection = engines["endftables"].connect()
    data = (
        session_lib()
        .query(Endf_FY_Data)
        .filter(
            Endf_FY_Data.reaction_id.in_(tuple(ids)),
        )
    )

    df = pd.read_sql(
        sql=data.statement,
        con=connection,
    )

    return df
