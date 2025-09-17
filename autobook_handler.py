# autobook_handler.py
import re
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

from db import execute_sql
from aggregation_handler import detect_aggregation, build_aggregation_query
from mapper_utils import (
    extract_direct_column_filters,
    extract_comparative_filters,
    parse_date_range_from_prompt,
)

# -------------------------------
# AUTOBOOK “virtual table” schema
# -------------------------------
AUTOBOOK_TABLE = "AUTOBOOK_VIEW"

AUTOBOOK_COLUMNS_META: Dict[str, Dict[str, str]] = {
    "APPL_NB":         {"desc": "Application number",                         "type": "string"},
    "LOAN_VERF_BK_DT": {"desc": "Loan verification booking date",             "type": "date"},
    "DIR_LOC_CD":      {"desc": "Direct location code",                       "type": "string"},
    "PROD_TYPE_NM":    {"desc": "Product type",                               "type": "string"},
    "CHNL_CD":         {"desc": "Channel code",                               "type": "string"},
    "ORGN_CHNL_NM":    {"desc": "Origination channel name",                   "type": "string"},
    "BOOKED_USER":     {"desc": "User who booked the application",            "type": "string"},
    "EMAIL":           {"desc": "Email extracted from activity comments",     "type": "string"},
    "DR_USERS":        {"desc": "Users involved in DR activities (CSV)",      "type": "string"},
    "RE_USERS":        {"desc": "Users involved in RE activities (CSV)",      "type": "string"},
    "FX_USERS":        {"desc": "Users involved in FX activities (CSV)",      "type": "string"},
}

# ----------------------------------------
# Hard-coded business rule (= CAFVASC)
# ----------------------------------------
BUSINESS_RULES: List[Tuple[re.Pattern, str]] = [
    # autobook
    (re.compile(r"\bauto[- ]?book\b|\bcfsin\b", re.I), "BOOKED_USER = 'CAFVASC'"),
    # manual
    (re.compile(r"\bmanual\b|\bagent booked\b|\bhuman\b", re.I), "BOOKED_USER <> 'CAFVASC'"),
]

# Optional normalizers so “loan/retail/subaru/land rover” convert to exact equals
NORMALIZE_EQ = {
    "PROD_TYPE_NM": {
        r"\bloan(s)?\b": "LOAN",
        r"\blease(s)?\b": "LEASE",
    },
    "ORGN_CHNL_NM": {
        r"\bretail\b": "RETAIL",
        r"\bsubaru\b": "SUBARU",
        r"\bland\s*rover\b": "LAND ROVER",
    },
}

DEFAULT_WINDOW_DAYS = 30


def _infer_dates(prompt: str) -> Tuple[str, str]:
    dr = parse_date_range_from_prompt(prompt)
    if dr and dr.get("from") and dr.get("to"):
        return dr["from"], dr["to"]
    # fallback: current month window (or last 30 days)
    today = datetime.utcnow().date()
    start = today.replace(day=1)
    return start.isoformat(), today.isoformat()


def _business_clauses(prompt: str) -> List[str]:
    clauses = []
    for rx, clause in BUSINESS_RULES:
        if rx.search(prompt):
            clauses.append(clause)
    return clauses


def _normalized_equals_from_prompt(prompt: str) -> List[str]:
    eqs = []
    for col, rx_map in NORMALIZE_EQ.items():
        for rx, val in rx_map.items():
            if re.search(rx, prompt, re.I):
                eqs.append(f"{col} = '{val}'")
    return eqs


# -------------------------
# DNA base query (cleaned)
# -------------------------
BASE_CTE = """
WITH First_DR_Transaction AS (
  SELECT
    dsi.APPL_NB,
    MIN(orgn.ACTV_TS) AS FIRST_DR_ACTV_TS
  FROM PROD_110575_ICDW_DB.AUTO_V.AFNC_DSI_ORGN_ACCT_DY dsi
  JOIN PROD_110575_ICDW_DB.AUTO_V.AFNC_DSI_ORGN_ACTV_DY_SUM orgn
    ON orgn.AUTO_FNCE_ORGN_DIM_ID = dsi.AUTO_FNCE_ORGN_DIM_ID
  WHERE orgn.ACTV_TYPE_CD = 'DR'
    AND dsi.LOAN_VERF_BK_DT BETWEEN '{FROM}' AND '{TO}'
    AND dsi.SNPST_DT = (
      SELECT MAX(SNPST_DT)
      FROM PROD_110575_ICDW_DB.AUTO_V.AFNC_DSI_ORGN_ACCT_DY
    )
    AND dsi.APPL_EXCL_IN = 1
    AND dsi.BK_IN = 1
  GROUP BY dsi.APPL_NB
),
Booked_User_CTE AS (
  SELECT
    dsi.APPL_NB,
    MAX(CASE WHEN orgn.ACTV_TYPE_CD IN ('BK','PM') THEN orgn.ADJC_DCSN_USR_ID END) AS BOOKED_USER
  FROM PROD_110575_ICDW_DB.AUTO_V.AFNC_DSI_ORGN_ACCT_DY dsi
  JOIN PROD_110575_ICDW_DB.AUTO_V.AFNC_DSI_ORGN_ACTV_DY_SUM orgn
    ON orgn.AUTO_FNCE_ORGN_DIM_ID = dsi.AUTO_FNCE_ORGN_DIM_ID
  WHERE dsi.LOAN_VERF_BK_DT BETWEEN '{FROM}' AND '{TO}'
    AND dsi.SNPST_DT = (
      SELECT MAX(SNPST_DT)
      FROM PROD_110575_ICDW_DB.AUTO_V.AFNC_DSI_ORGN_ACCT_DY
    )
    AND dsi.APPL_EXCL_IN = 1
    AND dsi.BK_IN = 1
  GROUP BY dsi.APPL_NB
)
"""

BASE_SELECT = """
SELECT
  dsi.APPL_NB,
  MAX(dsi.LOAN_VERF_BK_DT) AS LOAN_VERF_BK_DT,
  MAX(dsi.DIR_LOC_CD)      AS DIR_LOC_CD,
  MAX(dsi.PROD_TYPE_NM)    AS PROD_TYPE_NM,
  MAX(dsi.CHNL_CD)         AS CHNL_CD,
  MAX(dsi.ORGN_CHNL_NM)    AS ORGN_CHNL_NM,
  bu.BOOKED_USER           AS BOOKED_USER,
  MAX(CASE
        WHEN bu.BOOKED_USER IN ('CAFVASC','CAFECON')
         AND orgn.ADJC_DCSN_USR_ID IN ('CAFVASC','CAFECON')
         AND orgn.ACTV_ADDL_CMNT_TX ILIKE '%@%'
      THEN orgn.ACTV_ADDL_CMNT_TX
  END) AS EMAIL,
  LISTAGG(CASE WHEN orgn.ACTV_TYPE_CD = 'DR' THEN orgn.ADJC_DCSN_USR_ID END, ', ')
    WITHIN GROUP (ORDER BY orgn.ADJC_DCSN_USR_ID) AS DR_USERS,
  LISTAGG(CASE WHEN orgn.ACTV_TYPE_CD = 'RC' THEN orgn.ADJC_DCSN_USR_ID END, ', ')
    WITHIN GROUP (ORDER BY orgn.ADJC_DCSN_USR_ID) AS RE_USERS,
  LISTAGG(CASE WHEN orgn.ACTV_TYPE_CD = 'FX' THEN orgn.ADJC_DCSN_USR_ID END, ' ')
    WITHIN GROUP (ORDER BY orgn.ADJC_DCSN_USR_ID) AS FX_USERS
FROM PROD_110575_ICDW_DB.AUTO_V.AFNC_DSI_ORGN_ACCT_DY dsi
JOIN First_DR_Transaction fdr
  ON dsi.APPL_NB = fdr.APPL_NB
JOIN PROD_110575_ICDW_DB.AUTO_V.AFNC_DSI_ORGN_ACTV_DY_SUM orgn
  ON orgn.AUTO_FNCE_ORGN_DIM_ID = dsi.AUTO_FNCE_ORGN_DIM_ID
LEFT JOIN PROD_110575_ICDW_DB.AUTO_V.AFNC_BF_ORGN_CNTRCT_EXCP_DY excpt
  ON dsi.APPL_NB = excpt.APPL_NB
JOIN Booked_User_CTE bu
  ON dsi.APPL_NB = bu.APPL_NB
WHERE dsi.LOAN_VERF_BK_DT BETWEEN '{FROM}' AND '{TO}'
  AND dsi.SNPST_DT = (
    SELECT MAX(SNPST_DT)
    FROM PROD_110575_ICDW_DB.AUTO_V.AFNC_DSI_ORGN_ACCT_DY
  )
  AND dsi.APPL_EXCL_IN = 1
  AND dsi.BK_IN = 1
  AND orgn.ACTV_TS >= fdr.FIRST_DR_ACTV_TS
GROUP BY dsi.APPL_NB, bu.BOOKED_USER
"""

def _materialize_view(from_date: str, to_date: str) -> str:
    return (
        BASE_CTE.format(FROM=from_date, TO=to_date)
        + "\n, "
        + AUTOBOOK_TABLE
        + " AS (\n"
        + BASE_SELECT.format(FROM=from_date, TO=to_date)
        + "\n)"
    )


def build_autobook_sql(prompt: str, limit_hint: int | None = 200) -> str:
    # 1) dates
    from_dt, to_dt = _infer_dates(prompt)

    # 2) materialize virtual table
    with_view = _materialize_view(from_dt, to_dt)

    # 3) build WHERE parts
    where_clauses: List[str] = []
    where_clauses += _business_clauses(prompt)

    # strict equals from your direct-filter extractor (authorized columns)
    comparative_filters, filtered_cols = extract_comparative_filters(prompt, AUTOBOOK_COLUMNS_META)
    direct_filters = extract_direct_column_filters(prompt, AUTOBOOK_COLUMNS_META, filtered_columns=filtered_cols)

    # ensure normalization like “loan/retail/subaru” → equals
    norm_equality = _normalized_equals_from_prompt(prompt)

    # Only allow filters on declared columns
    all_filters = list(dict.fromkeys(list(comparative_filters) + list(direct_filters) + norm_equality))

    # 4) aggregation?
    agg_func, agg_col = detect_aggregation(prompt, AUTOBOOK_COLUMNS_META)

    if agg_func:
        # percentage & others handled exactly like mapper flow
        percentage_condition = None
        percentage_denominator_condition = None

        # Let build_aggregation_query craft SELECT ... FROM AUTOBOOK_VIEW ...
        query = build_aggregation_query(
            agg_func,
            agg_col,
            AUTOBOOK_TABLE,
            prompt,
            AUTOBOOK_COLUMNS_META,
            percentage_condition,
            percentage_denominator_condition
        )

        # Inject WHERE (same approach you use in mapper)
        if where_clauses or all_filters:
            where_sql = " WHERE " + " AND ".join(where_clauses + all_filters)
            import re as _re
            query = _re.sub(r"\bWHERE\b.*?(LIMIT|$)", where_sql, query, flags=_re.IGNORECASE) or query + where_sql

        # Apply limit if your builder didn’t
        if "LIMIT" not in query.upper() and limit_hint:
            query += f" LIMIT {int(limit_hint)}"

        return f"{with_view}\n{query}"

    # 5) non-aggregation SELECT
    select_cols = ", ".join(AUTOBOOK_COLUMNS_META.keys())
    outer = [f"SELECT {select_cols} FROM {AUTOBOOK_TABLE}"]
    if where_clauses or all_filters:
        outer.append("WHERE " + " AND ".join(where_clauses + all_filters))
    if limit_hint:
        outer.append(f"LIMIT {int(limit_hint)}")

    return f"{with_view}\n" + "\n".join(outer)


def run_autobook(prompt: str, limit: int | None = 200):
    sql = build_autobook_sql(prompt, limit_hint=limit)
    rows = execute_sql(sql)
    return rows, sql
