import calendar
import datetime
from numbers import Number

import pandas as pd
from omero.gateway import BlitzObjectWrapper

SLP = "COPY of Human Tissue Repository Project"
CCP = "KTB - LUNG"

# NA_VALUES = [
#     "Unknown",
#     "Unspecified",
#     "Not specified",
#     "obo:NCIT_C43234   - Not specified",
#     "unknown",
#     "uu",
#     "Not Done",
#     "UK",
#     "UU",
#     "Uu",
#     "Don't know",
#     "Other (Specify)",
#     "U",
#     "Nk",
#     "u",
#     "nk",
# ]
LAB_ID = [
    "lab_id_main",
    "slice_lab_id",
    "lab_id_block",
    "lab_id_slide",
    "steyn_lab_id",
    "lab_id3",
    "record_id",
]
RENAME_MAPPER = {
    "scan_participant_id": "pid",
    "select_specimen_type___1": "specimen_type_tissue",
    "select_specimen_type___2": "specimen_type_block",
    "select_specimen_type___4": "specimen_type_appraisal",
    "envexp___1": "envexp_dust",
    "envexp___2": "envexp_fumes",
    "envexp___3": "envexp_smoke",
    "envexp___4": "envexp_vapors",
    "envexp___5": "envexp_gases",
    "envexp___6": "envexp_mists",
    "envexp___7": "envexp_none",
    "envfactlive___1": "envfactlive_woodfire",
    "envfactlive___2": "envfactlive_coalfire",
    "envfactlive___3": "envfactlive_paraffin",
    "envfactlive___4": "envfactlive_factory",
    "envfactlive___5": "envfactlive_other",
    "envfactlive___6": "envfactlive_none",
    "drgssmkd___1": "drgssmkd_marijuana",
    "drgssmkd___2": "drgssmkd_woonga",
    "drgssmkd___3": "drgssmkd_cocaine",
    "drgssmkd___4": "drgssmkd_other",
    "diagmthd___1": "diagmthd_cxr",
    "diagmthd___2": "diagmthd_ultrasound",
    "diagmthd___3": "diagmthd_afb",
    "diagmthd___4": "diagmthd_culture",
    "diagmthd___5": "diagmthd_biopsy",
    "diagmthd___6": "diagmthd_xpert",
    "diagmthd___7": "diagmthd_clinical",
    "xrayabn___1": "xrayabn_indeterminate",
    "xrayabn___2": "xrayabn_unilateral_infiltrate",
    "xrayabn___3": "xrayabn_adenopathy",
    "xrayabn___4": "xrayabn_cavitary_lesion",
    "xrayabn___5": "xrayabn_pericardial_effusion",
    "xrayabn___6": "xrayabn_bilateral_infiltrate",
    "xrayabn___7": "xrayabn_miliary_pattern",
    "xrayabn___8": "xrayabn_pleural_effusion",
    "xrayabn___9": "xrayabn_not_known",
    "xrayabn___10": "xrayabn_other",
    "eptbtype___1": "eptbtype_abdomen",
    "eptbtype___2": "eptbtype_pericarditis",
    "eptbtype___3": "eptbtype_disseminated",
    "eptbtype___4": "eptbtype_pleural_effusion",
    "eptbtype___5": "eptbtype_adenitis",
    "eptbtype___6": "eptbtype_skin",
    "eptbtype___7": "eptbtype_meningitis",
    "eptbtype___8": "eptbtype_spine",
    "eptbtype___9": "eptbtype_other",
    "rectbsite___1": "rectbsite_pulmonary",
    "rectbsite___2": "rectbsite_extrapulmonary",
    "currsymp___1": "currsymp_wheezing",
    "currsymp___2": "currsymp_diff_breathing",
    "currsymp___3": "currsymp_coughing",
    "currsymp___4": "currsymp_coughing_blood",
    "currsymp___5": "currsymp_none",
    "currsymp___6": "currsymp_other",
    "arm___1": "case",
    "arm___2": "control",
    "lungspecol___1": "lungspecol_lung",
    "lungspecol___2": "lungspecol_lymph",
    "lungcoll___1": "lungcoll_left",
    "lungcoll___2": "lungcoll_right",
    "llunglobes___1": "llunglobes_upper",
    "llunglobes___2": "llunglobes_lower",
    "rlunglobes___1": "rlunglobes_upper",
    "rlunglobes___2": "rlungglobes_middle",
    "rlungglobes___3": "rlunglobes_lower",
    "lymphcoll___1": "lymphcoll_left_lung",
    "lymphcoll___2": "lymphcoll_right_lung",
    "llunglymph___1": "llunglymph_upper",
    "llunglymph___2": "llunglymph_lower",
    "rlunglymph___1": "rlunglymph_upper",
    "rlunglymph___2": "rlunglymph_middle",
    "rlunglymph___3": "rlunglymph_lower",
    "comorbidity___1": "comorbid_hiv",
    "comorbidity___2": "comorbid_asthma",
    "comorbidity___3": "comorbid_copd",
    "comorbidity___4": "comorbid_bronchiectasis",
    "comorbidity___5": "comorbid_diabetes",
    "comorbidity___6": "comorbid_cancer",
    "comorbidity___7": "comorbid_epilepsy",
    "comorbidity___8": "comorbid_hypertension",
    "comorbidity___9": "comorbid_hyperlipidemia",
    "comorbidity___10": "comorbid_other",
}
months = (
    {calendar.month_name[i].casefold(): i for i in range(1, 13)}
    | {calendar.month_abbr[i].casefold(): i for i in range(1, 13)}
    | {"jily": 7, "d": pd.NA, "0ct": 10, "n0v": 11}
)


COL_OVERLAPS = {
    "gender": "gender_main",
    "race": "race_main",
    "age_main": "age",
    "treatment": "rectbregimen",
    "hiv": "comorbidity___1",
    "asthma": "comorbidity___2",
}
DURATION_MY = {
    "yrssmkecigs": "mnthssmkecigs",
    "drgsyrs": "drgsmnth",
    "expyears": "expmnths",
}
# DT_COLS = {
#     "COPY of Human Tissue Repository Project": [
#         "courier_date",
#         "date_captured",
#         "date_last_modified2",
#         "date_last_modified2_org",
#         "date_last_modified2_orgv2",
#         "date_last_modified3",
#         "date_last_modified4",
#         "date_scanned",
#         "expired_date",
#         "microct_scan_date",
#         "microtome_date",
#         "puthru_date",
#         "return_date",
#         "sample_collection_date",
#         "stain_date",
#         "tissue_proc_date",
#         "waxed_date",
#     ],
#     "KTB - LUNG": [
#         "vdate",
#         "dob",
#         "tbtxstartdate",
#         "proptbdse",
#         "compdatedem",
#         "lungtssuedate",
#         "compdatelung",
#         "m_dob",
#         "m_vldate",
#         "cov_hadcovid_date",
#         "conmed_medstopdate",
#         "smear_specdate",
#         "pcr_specdate",
#         "dst_specdate",
#     ],
#     "KTB - Lung Study Source": [],
# }
CURRENT_YEAR = datetime.datetime.now().year
# CCP_DAYS = {
#     "0": 1,
#     "00": 1,
#     "05-11-2021": 5,
#     "!8": 8,
#     "Nov ": 11,
#     "52": 22,
#     "2024": 24,
#     "D": pd.NA,
#     "A": pd.NA,
#     "ongoing ": pd.NA,
# }
MULTIINDICES = {
    "ConMeds": ["pid", "conmed_medication"],
    "ARV Medications": ["pid", "arvmed_medication"],
    "CD4 Counts": ["pid", "cd4count_datecd4"],
    "TB Medications": ["pid", "tbmed_medication"],
    "Viral Loads": ["pid", "vlres_datevl"],
    "Smear Microscopy": ["pid", "smear_specdate"],
    "DST Results": ["pid", "dst_specdate"],
    "Probe Assay Results": ["pid", "pcr_specdate"],
    "Baseline": "pid",
    "Organ Slices Form": ["sl", "slice_id"],
    "Histopathological Appraisal": "sl",
    "Participants Entry Form": "sl",
    "Blocks Entry Form": ["sl", "block_id"],
    "MicroCT Form": "sl",
    "Slides Entry Form": ["sl", "slide_id"],
    "Organs Entry Form": ["sl", "organ_collected"],
}


def get_ymd(s, *, year="year", month="mnth", day="day", date="date"):
    return [s.replace(date, ymd) for ymd in (year, month, day) if ymd is not None]


DATE_TRIPLET_COLS = (
    {
        "xraydate": get_ymd("xraydate", year="ear"),
        "datebron": ["yrbro", "mnthbron"],
        "dateothr1": ["yrothr1", "mnthotr1"],
        "dateothr2": ["yrothr2", "mnthotr2"],
    }
    | {
        col: get_ymd(col, month="month")
        for col in [
            "datetoldtb",
            "datetbtxotcme",
            "conmed_datestartmed",
            "tbmed_medstartdate",
            "tbmed_medstopdate",
            "cd4count_datecd4",
            "arvmed_medstartdate",
            "arvmed_medstopdate",
            "vlres_datevl",
        ]
    }
    | {
        col + "date": get_ymd(col + "date")
        for col in ["rectbdiag", "m_artstart", "pohiv", "neghiv", "artstart", "p_pohiv"]
    }
    | {
        "date" + col: get_ymd("date" + col, year="yr", day=None)
        for col in ["ild", "ast", "copd", "diab", "cncr"]
    }
)


def format_year(y):
    try:
        y = int(y)
    except (ValueError, TypeError):
        return pd.NA
    if 1000 <= y <= CURRENT_YEAR:
        return y
    y = y % 100
    return y + 2000 if (y <= CURRENT_YEAR - 2000) else y + 1900


def format_month(m):
    if pd.isna(m):
        return pd.NA
    try:
        mo_num = int(m)
    except (ValueError, TypeError):
        m = m.casefold().strip()
        if m in months:
            return months[m]
        s = {mo_num for mo_name, mo_num in months.items() if mo_name.startswith(m)}
        if len(s) == 1:
            mo_num = s.pop()
            months[m] = mo_num
            return mo_num
        return pd.NA
    return mo_num if mo_num > 0 else 1


def get_date_triplet(df, na_month=7, na_day=1):
    ymd = df.columns
    arg = df.rename(columns=dict(zip(ymd, ["year", "month", "day"])))
    arg["year"] = arg["year"].map(format_year)
    arg["month"] = arg["month"].map(format_month).astype("Int64").fillna(na_month)
    if len(ymd) == 3:
        arg["day"] = pd.to_numeric(arg["day"], "coerce").fillna(na_day)
    else:
        arg["day"] = na_day
    return pd.to_datetime(arg.astype("Int64").astype(str), "coerce")


# Functions
def sl_int(s) -> int:
    if pd.isna(s):
        return pd.NA
    if isinstance(s, Number) and int(s) == s:
        return int(s)
    if isinstance(s, str):
        return int(s.removeprefix("SL"))
    if isinstance(s, BlitzObjectWrapper):
        return int(s.getName().removeprefix("SL"))
    raise ValueError(f"Invalid literal for sl_int(): {s} of type {type(s)}")


BOOL_MAPS = [
    {"Unchecked": False, "Checked": True},
    {"No": False, "Yes": True, "Unknown": pd.NA},
    {
        "No": False,
        r"Yes  {select_the_projects_select}{select_the_projects_text}": True,
    },
    {"Surgical resection": False, "Unspecified": pd.NA, "Autopsy": True},
    {"Negative": False, "Positive": True, "Unknown": pd.NA},
]
REPLACE_DATES = {
    "16/SEPT/1997": "1997-09-16",
    "15-05-2015": "2025-05-15",
    "27-01-2016": "2016-01-27",
}
RACE_MAP = {
    "obo:NCIT_C42331   - African": "Black",
    "obo:NCIT_C156583 - Colored": "Coloured",
    "obo:NCIT_C41261   - White": "White",
    "obo:NCIT_C41260   - Asian": "Indian",
    "African": "Black",
}


def fix_monthsontbtx(s: str):
    if pd.isna(s):
        return s
    try:
        return int(s.strip())
    except ValueError:
        s = s.lower()
        if "month" in s:
            return int(s.split("month")[0].strip())
        elif "y" in s:
            return int(s.split("y")[0].strip()) * 12
        elif "week" in s:
            return int(s.split("week")[0].strip()) * 0.23
        else:
            raise ValueError(f"Cannot parse months on TB treatment from value: {s}")


MONTHSONTBTX_UNITS = {"Y": 12, "month": 1, "months": 1, "weeks": 0.25, "yrs": 12}
REPLACEMENTS = {
    "compdatelung": {"16 March 2023 ": "2023-03-16"},
    "pohivyear": {"202p": 2020},
    "yrssmkecigs": {2006: 17},
    "xrayday": {"05-11-2021": 5},
    "conmed_daystartmed": {"Nov ": 11},
    "tbmed_medstartday": {"!8": 18},
    "tbmed_medstopday": {"ongoing ": pd.NA},
}


def standardise_treatment(t: str) -> str:
    if pd.isna(t):
        return pd.NA
    t = t.lower()
    if t.startswith("mdr"):
        return "MDR-regimen"
    if t.startswith("xdr"):
        return "XDR-regimen"
    if "rifafour" in t or "rifinah" in t:
        return "Rifafour/Rifinah"
    return pd.NA


def idxmax(row: pd.Series):
    return row.idxmax() if (row.sum() == 1) else pd.NA


def get_transform(s: pd.Series) -> pd.Series:
    if (
        "date" in s.name or "dob" in s.name or s.name == "proptbdse"
    ) and "knwn" not in s.name:
        try:
            return pd.to_datetime(s, format="%Y-%m-%d", exact=False)
        except ValueError as e:
            raise ValueError(f"Error parsing dates in column {s.name}") from e
    unique_vals = set(s.dropna().unique())
    for d in BOOL_MAPS:
        if unique_vals.issubset(d.keys()):
            return s.map(d).astype("boolean")
    if hasattr(s, "str") and s.str.contains("\n", regex=False).any():
        return s.str.replace(r"((\.|\r)\s*\n)+", ". ", regex=True)
    try:
        return pd.to_numeric(s)
    except ValueError:
        return s


def merge(
    kdf1: tuple[str, pd.DataFrame], kdf2: tuple[str, pd.DataFrame], idx: str
) -> tuple[str, pd.DataFrame]:
    key1, df1 = kdf1
    key2, df2 = kdf2
    suffixes = [f"_{key.replace(' ', '_').lower()}" for key in (key1, key2)]
    merged = df1.merge(df2, how="left", on=idx, suffixes=suffixes)
    return (f"{key1} + {key2}", merged)


def fillna_same(s: pd.Series, tol=None, aggfunc="mean"):
    if s.notna().any():
        if s.nunique(dropna=True) == 1:
            return s.get(s.first_valid_index())
        elif tol is not None and abs(s.max() - s.min()) <= tol:
            return s.agg(aggfunc)
    return pd.NA
