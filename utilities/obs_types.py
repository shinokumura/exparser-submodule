####################################################################
#
# This file is part of libraries-2021 dataexplorer.
# Copyright (C) 2022 International Atomic Energy Agency (IAEA)
#
# Contact:    nds.contact-point@iaea.org
#
####################################################################
"""
Canonical observable-type sets shared across exfor, endflibs, and UI layers.

SF6 descriptions come from exfor_dictionary diction 32 via D.get_sf6(code).
Short labels for charts are derived from MAPPING["SF6"]["description"].
"""

# Scalar observables that use the "Year" x-axis (not energy).
# Includes both aggregate grouping keys (TH, RI, MACS, GG, D, S)
# and per-wave-number page params (D0, D1, D2, S0, S1, GG0, GG1, IF, RAD, STF).
SCALAR_OBS: frozenset = frozenset({
    "TH", "RI", "MACS",
    "GG", "D", "S",
    "D0", "D1", "D2",
    "S0", "S1",
    "GG0", "GG1",
    "IF", "RAD", "STF",
})

# Total photon-production cross section.  EXFOR identifies the inclusive
# emitted photon in SF4.  The evaluated curve is constructed from MF=6/12/13
# and indexed with the conventional production MT=202 identifier.
# Keep these identifiers together so the UI and database adapters cannot drift.
# GAMMA_PRODUCTION_OBS_TYPE = "GPROD"
GAMMA_PRODUCTION_SF4 = "0-G-0"

# Mapping from per-L obs_type to expected Momentum-L integer value.
# Used to post-filter EXFOR data fetched for D0/D1/D2 and S0/S1.
L_WAVE_OBS: dict = {
    "D0": 0, "D1": 1, "D2": 2,
    "S0": 0, "S1": 1,
    "GG0":0, "GG1": 1,
}


# ---------------------------------------------------------------------------
# SF6 → ENDF MF number
# ---------------------------------------------------------------------------

sf6_to_mf: dict[str, str] = {
    "NU":    "1",
    "WID":   "2",
    "ARE":   "2",
    "D":     "2",
    "EN":    "2",
    "J":     "2",
    "SIG":   "3",
    "DA":    "4",
    "DE":    "5",
    "FY":    "8",
    "DA/DE": "6",
}


# ---------------------------------------------------------------------------
# SF6 → data-directory name
# ---------------------------------------------------------------------------

sf6_to_dir: dict[str, str] = {
    "SIG":   "xs",
    "GPROD": "xs",
    "DA":    "angle",
    "DE":    "energy",
    "DA/DE": "energy/angle",
    "NU":    "neutrons",
    "DL":    "neutrons",
    "NU/DE": "neutrons/energy",
    "FY":    "fission/yield",
    "FY/DE": "fission/energy",
    "KE":    "kinetic_energy",
    "AKE":   "kinetic_energy/average",
    "TRN":   "transmission",
}


# Pages whose downloadable source files currently come only from
# EXFORTABLES_py.  Keep the UI and path generation on the same definition.
EXFOR_ONLY_FILE_DOWNLOADS: frozenset = frozenset({"TRN"})


# SF6 codes that represent resonance parameters
resonance_parameter_sf6: list[str] = ["WID", "WID/STR", "WID/RED", "ARE", "ETA", "ALF"]


# ---------------------------------------------------------------------------
# Incident-particle (projectile) display labels for UI and charts.
# Keys are the EXFOR projectile codes stored in the database.
# ---------------------------------------------------------------------------
PROJ_LABELS: dict[str, str] = {
    "N":   "Neutron (n)",
    "P":   "Proton (p)",
    "D":   "Deuteron (d)",
    "T":   "Triton (t)",
    "A":   "Alpha (α)",
    "G":   "Gamma (γ)",
    "HE3": "Helium-3 (³He)",
    "E":   "Electron (e⁻)",
}
SPONTANEOUS = {"0":   "Spontaneous"}

# ---------------------------------------------------------------------------
# Observable top-category grouping and SF6 per-code metadata.
# Used for UI dropdowns and filtering; top_category groups SF6 codes into
# human-readable categories shown in the search panel.
# ---------------------------------------------------------------------------
MAPPING = {
    "top_category": {
        "SIG": "Cross Section (SIG)",
        "DA": "Angular Distribution (DA)",
        "DE": "Energy Distribution (DE)",
        "FY": "Fission Yield (FY, AP, ZP)",
        "DDX": "Double Differential Cross Section (DA/DE)",
        "KE": "Kinetic Energy (KE)",
        "RES": "Resonance Parameters (WID, ARE, EN, D, RI, J, L, PHS, AMP)",
        "NPR":"Nuclear Properties (LD, LDP, PTY, SCO, RAD, SWG, STR, STF)",
        "POL": "Polarization (POL, FM, TYA)",
        "NU": "Neutron (NU, PN)",
        "TTY": "Tick Target Yield (TTY, PY, RYL)",
        "TRN": "Transmission (TRN)",
        "FND": "Fundamental Quantities (ALF, ETA, RAT)",
        "Others": "Others",

    },
    "SF6": {
        "AG":    {"description": "Symmetry coefficient",                                   "top_category": "POL"},
        "AH":    {"description": "Asymmetry coefficient",                                  "top_category": "POL"},
        "AKE":   {"description": "Average kinetic energy",                                 "top_category": "KE"},
        "ALF":   {"description": "Alpha = capture/fission cross-section ratio",            "top_category": "FND"},
        "AMP":   {"description": "Scattering length",                                      "top_category": "RES"},
        "AP":    {"description": "Most probable mass of fission-fragments",                "top_category": "FY"},
        "ARE":   {"description": "Resonance-area",                                         "top_category": "RES"},
        "D":     {"description": "Average level-spacing",                                  "top_category": "RES"},
        "DA":    {"description": "Angular Distribution",                                   "top_category": "DA"},
        "DA2":   {"description": "Double-diff. by angle (quadruple-diff cs only)",         "top_category": "DDX"},
        "DA/DE": {"description": "Double-diff. cross section",                             "top_category": "DDX"},
        "DE":    {"description": "Energy Distribution",                                    "top_category": "DE"},
        "DE2":   {"description": "Double-diff. by energy (quadruple-diff.cs only)",        "top_category": "DDX"},
        "DEN":   {"description": "Differential with incident energy",                      "top_category": "DE"},
        "DP":    {"description": "Differential with lin.momentum of outgoing particles",   "top_category": "Others"},
        "DT":    {"description": "Diff. with 4-momentum transfer squared",                 "top_category": "Others"},
        "EN":    {"description": "Resonance-energy",                                       "top_category": "RES"},
        "ETA":   {"description": "Average neutron yield per nonelastic event",             "top_category": "FND"},
        "FM":    {"description": "Product of polarization and cross section",              "top_category": "POL"},
        "FY":    {"description": "Fission Yield",                                          "top_category": "FY"},
        "INT":   {"description": "Cross-section integral over incident energy",            "top_category": "SIG"},
        "IPA":   {"description": "Cross section (partial angular range)",                  "top_category": "SIG"},
        "IPP":   {"description": "Cross section (partial momentum range)",                 "top_category": "Others"},
        "J":     {"description": "Spin J",                                                 "top_category": "RES"},
        "KE":    {"description": "Kinetic Energy",                                         "top_category": "KE"},
        "KEM":   {"description": "Temperature of Maxwellian distr. of outgoing particles", "top_category": "KE"},
        "KEP":   {"description": "Most probable kinetic energy of outgoing particle",      "top_category": "KE"},
        "KER":   {"description": "Kerma factor",                                           "top_category": "Others"},
        "L":     {"description": "Angular momentum L",                                     "top_category": "RES"},
        "LD":    {"description": "Level-density",                                          "top_category": "NPR"},
        "LDP":   {"description": "Level-density parameter",                                "top_category": "NPR"},
        "MLT":   {"description": "Multiplicity (particle yield per event)",                "top_category": "FY"},
        "NU":    {"description": "Fission-neutron yield, nu-bar",                          "top_category": "NU"},
        "PHS":   {"description": "Reich-Moore phase",                                      "top_category": "RES"},
        "PN":    {"description": "Delayed neutron emission probability",                   "top_category": "NU"},
        "POL":   {"description": "Polarization",                                           "top_category": "POL"},
        "PTY":   {"description": "Parity",                                                 "top_category": "NPR"},
        "PY":    {"description": "Product yield",                                          "top_category": "TTY"},
        "RAD":   {"description": "Scattering radius",                                      "top_category": "NPR"},
        "RAT":   {"description": "Ratio",                                                  "top_category": "FND"},
        "RED":   {"description": "Reduced",                                                "top_category": "Others"},
        "RI":    {"description": "Resonance integral",                                     "top_category": "RES"},
        "RYL":   {"description": "Reaction yield",                                         "top_category": "TTY"},
        "SCO":   {"description": "Spin cut-off factor",                                    "top_category": "NPR"},
        "SGV":   {"description": "Thermonuclear reaction rate",                            "top_category": "SIG"},
        "SIF":   {"description": "Self-indication function",                               "top_category": "Others"},
        "SIG":   {"description": "Cross Section",                                          "top_category": "SIG"},
        "SPC":   {"description": "Intensity of discrete gamma-lines",                      "top_category": "Others"},
        "STF":   {"description": "Strength function",                                      "top_category": "NPR"},
        "STR":   {"description": "Strength",                                               "top_category": "NPR"},
        "SUM":   {"description": "Sum",                                                    "top_category": "Others"},
        "SWG":   {"description": "Statistical weight g",                                   "top_category": "NPR"},
        "TEM":   {"description": "Nuclear temperature",                                    "top_category": "KE"},
        "TKE":   {"description": "Total kinetic energy",                                   "top_category": "KE"},
        "TMP":   {"description": "Temperature-dependent quantity",                         "top_category": "Others"},
        "TRN":   {"description": "Transmission",                                           "top_category": "TRN"},
        "TTT":   {"description": "Thick-target yield per unit time",                       "top_category": "TTY"},
        "TTY":   {"description": "Thick-target yield",                                     "top_category": "TTY"},
        "TYA":   {"description": "Differential with respect to Treiman-Yang angle",        "top_category": "POL"},
        "WID":   {"description": "Resonance width",                                        "top_category": "RES"},
        "ZP":    {"description": "Most probable charge of fission fragments",              "top_category": "FY"},
    },
}
