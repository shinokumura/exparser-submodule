"""Shared point selection and normalisation for EXFOR distributions."""

import numpy as np
import pandas as pd


SLICER_EN_TOL_PCT = 15
SLICER_ANGLE_TOL_DEG = 5
EXFOR_DDX_UNIT = "B/SR/EV"
DISPLAY_DDX_UNIT = "MB/SR/MEV"
EXFOR_TO_DISPLAY = 1.0e9


def filter_by_nearest_entry_value(df, column, target, tolerance):
    """Keep each EXFOR dataset at its nearest accepted slicer value."""
    if df.empty or target is None:
        return pd.DataFrame()

    target = float(target)
    parts = []
    for _, subset in df.groupby("entry_id", sort=False):
        nearest = subset[column].iloc[
            (subset[column] - target).abs().argsort().iloc[:1]
        ].values[0]
        if abs(nearest - target) <= tolerance:
            parts.append(subset[subset[column] == nearest])
    return pd.concat(parts) if parts else pd.DataFrame()


def slice_exfor_da(df, x_axis, en_target=None, angle_target=None):
    """Apply the DA page's per-dataset nearest-value slice."""
    if df.empty:
        return df

    if x_axis == "angle":
        if en_target is None:
            return pd.DataFrame()
        target = float(en_target)
        return filter_by_nearest_entry_value(
            df,
            "en_inc",
            target,
            abs(target) * SLICER_EN_TOL_PCT / 100,
        )
    if x_axis == "energy":
        if angle_target is None:
            return pd.DataFrame()
        return filter_by_nearest_entry_value(
            df,
            "angle",
            float(angle_target),
            SLICER_ANGLE_TOL_DEG,
        )
    raise ValueError(f"Unsupported DA x axis: {x_axis}")


def normalise_exfor_ddx(exfor_df: pd.DataFrame) -> pd.DataFrame:
    """Convert canonical EXFOR DDX rows to the display/API convention."""
    if exfor_df.empty:
        return exfor_df

    df = exfor_df.copy()
    for column in ("e_out", "de_out", "e_out_min", "e_out_max"):
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce") / 1.0e6

    if {"e_out_min", "e_out_max"}.issubset(df.columns):
        has_bin = df["e_out_min"].notna() & df["e_out_max"].notna()
        midpoint = (df["e_out_min"] + df["e_out_max"]) / 2.0
        df.loc[has_bin, "e_out"] = midpoint.loc[has_bin]
        df["e_out_error_minus"] = np.where(
            has_bin,
            df["e_out"] - df["e_out_min"],
            df.get("de_out", np.nan),
        )
        df["e_out_error_plus"] = np.where(
            has_bin,
            df["e_out_max"] - df["e_out"],
            df.get("de_out", np.nan),
        )
    else:
        df["e_out_error_minus"] = df.get("de_out", np.nan)
        df["e_out_error_plus"] = df.get("de_out", np.nan)

    if "y_unit" not in df.columns:
        df["y_unit"] = EXFOR_DDX_UNIT
    canonical_unit = df["y_unit"].fillna(EXFOR_DDX_UNIT).astype(str).str.upper()
    compatible = canonical_unit == EXFOR_DDX_UNIT
    for column in ("data", "ddata"):
        if column in df.columns:
            values = pd.to_numeric(df[column], errors="coerce")
            df.loc[compatible, column] = values.loc[compatible] * EXFOR_TO_DISPLAY
    df.loc[compatible, "y_unit"] = DISPLAY_DDX_UNIT
    df["ddx_plot_compatible"] = compatible

    for column in ("en_inc_frame", "e_out_frame", "angle_frame", "data_frame"):
        if column not in df.columns:
            df[column] = "LAB"
        else:
            df[column] = df[column].fillna("LAB")
    return df


def slice_exfor_ddx(df, en_target=None, angle_target=None):
    """Apply the DDX page's EXFOR energy and angle tolerances."""
    if df.empty:
        return df

    result = df
    if "ddx_plot_compatible" in result.columns:
        result = result[result["ddx_plot_compatible"].fillna(False)]
    if en_target is not None:
        target = float(en_target)
        tolerance = abs(target) * SLICER_EN_TOL_PCT / 100
        result = result[
            result["en_inc"].between(
                target - tolerance,
                target + tolerance,
                inclusive="both",
            )
        ]
    if angle_target is not None:
        target = float(angle_target)
        result = result[
            result["angle"].between(
                target - SLICER_ANGLE_TOL_DEG,
                target + SLICER_ANGLE_TOL_DEG,
                inclusive="both",
            )
        ]
    return result
