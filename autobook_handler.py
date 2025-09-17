# # autobook_handler.py
# import re
# from datetime import datetime, timedelta
# from typing import Dict, List, Tuple

# from db import execute_sql
# from aggregation_handler import detect_aggregation, build_aggregation_query
# from mapper_utils import (
#     extract_direct_column_filters,
#     extract_comparative_filters,
#     parse_date_range_from_prompt,
# )

# # -------------------------------
# # AUTOBOOK “virtual table” schema
# # -------------------------------
# AUTOBOOK_TABLE = "AUTOBOOK_VIEW"

# AUTOBOOK_COLUMNS_META: Dict[str, Dict[str, str]] = {
#     "APPL_NB":         {"desc": "Application number",                         "type": "string"},
#     "LOAN_VERF_BK_DT": {"desc": "Loan verification booking date",             "type": "date"},
#     "DIR_LOC_CD":      {"desc": "Direct location code",                       "type": "string"},
#     "PROD_TYPE_NM":    {"desc": "Product type",                               "type": "string"},
#     "CHNL_CD":         {"desc": "Channel code",                               "type": "string"},
#     "ORGN_CHNL_NM":    {"desc": "Origination channel name",                   "type": "string"},
#     "BOOKED_USER":     {"desc": "User who booked the application",            "type": "string"},
#     "EMAIL":           {"desc": "Email extracted from activity comments",     "type": "string"},
#     "DR_USERS":        {"desc": "Users involved in DR activities (CSV)",      "type": "string"},
#     "RE_USERS":        {"desc": "Users involved in RE activities (CSV)",      "type": "string"},
#     "FX_USERS":        {"desc": "Users involved in FX activities (CSV)",      "type": "string"},
# }

# # ----------------------------------------
# # Hard-coded business rule (= CAFVASC)
# # ----------------------------------------
# BUSINESS_RULES: List[Tuple[re.Pattern, str]] = [
#     # autobook
#     (re.compile(r"\bauto[- ]?book\b|\bcfsin\b", re.I), "BOOKED_USER = 'CAFVASC'"),
#     # manual
#     (re.compile(r"\bmanual\b|\bagent booked\b|\bhuman\b", re.I), "BOOKED_USER <> 'CAFVASC'"),
# ]

# # Optional normalizers so “loan/retail/subaru/land rover” convert to exact equals
# NORMALIZE_EQ = {
#     "PROD_TYPE_NM": {
#         r"\bloan(s)?\b": "LOAN",
#         r"\blease(s)?\b": "LEASE",
#     },
#     "ORGN_CHNL_NM": {
#         r"\bretail\b": "RETAIL",
#         r"\bsubaru\b": "SUBARU",
#         r"\bland\s*rover\b": "LAND ROVER",
#     },
# }

# DEFAULT_WINDOW_DAYS = 30


# def _infer_dates(prompt: str) -> Tuple[str, str]:
#     dr = parse_date_range_from_prompt(prompt)
#     if dr and dr.get("from") and dr.get("to"):
#         return dr["from"], dr["to"]
#     # fallback: current month window (or last 30 days)
#     today = datetime.utcnow().date()
#     start = today.replace(day=1)
#     return start.isoformat(), today.isoformat()


# def _business_clauses(prompt: str) -> List[str]:
#     clauses = []
#     for rx, clause in BUSINESS_RULES:
#         if rx.search(prompt):
#             clauses.append(clause)
#     return clauses


# def _normalized_equals_from_prompt(prompt: str) -> List[str]:
#     eqs = []
#     for col, rx_map in NORMALIZE_EQ.items():
#         for rx, val in rx_map.items():
#             if re.search(rx, prompt, re.I):
#                 eqs.append(f"{col} = '{val}'")
#     return eqs


# # -------------------------
# # DNA base query (cleaned)
# # -------------------------
# BASE_CTE = """
# WITH First_DR_Transaction AS (
#   SELECT
#     dsi.APPL_NB,
#     MIN(orgn.ACTV_TS) AS FIRST_DR_ACTV_TS
#   FROM PROD_110575_ICDW_DB.AUTO_V.AFNC_DSI_ORGN_ACCT_DY dsi
#   JOIN PROD_110575_ICDW_DB.AUTO_V.AFNC_DSI_ORGN_ACTV_DY_SUM orgn
#     ON orgn.AUTO_FNCE_ORGN_DIM_ID = dsi.AUTO_FNCE_ORGN_DIM_ID
#   WHERE orgn.ACTV_TYPE_CD = 'DR'
#     AND dsi.LOAN_VERF_BK_DT BETWEEN '{FROM}' AND '{TO}'
#     AND dsi.SNPST_DT = (
#       SELECT MAX(SNPST_DT)
#       FROM PROD_110575_ICDW_DB.AUTO_V.AFNC_DSI_ORGN_ACCT_DY
#     )
#     AND dsi.APPL_EXCL_IN = 1
#     AND dsi.BK_IN = 1
#   GROUP BY dsi.APPL_NB
# ),
# Booked_User_CTE AS (
#   SELECT
#     dsi.APPL_NB,
#     MAX(CASE WHEN orgn.ACTV_TYPE_CD IN ('BK','PM') THEN orgn.ADJC_DCSN_USR_ID END) AS BOOKED_USER
#   FROM PROD_110575_ICDW_DB.AUTO_V.AFNC_DSI_ORGN_ACCT_DY dsi
#   JOIN PROD_110575_ICDW_DB.AUTO_V.AFNC_DSI_ORGN_ACTV_DY_SUM orgn
#     ON orgn.AUTO_FNCE_ORGN_DIM_ID = dsi.AUTO_FNCE_ORGN_DIM_ID
#   WHERE dsi.LOAN_VERF_BK_DT BETWEEN '{FROM}' AND '{TO}'
#     AND dsi.SNPST_DT = (
#       SELECT MAX(SNPST_DT)
#       FROM PROD_110575_ICDW_DB.AUTO_V.AFNC_DSI_ORGN_ACCT_DY
#     )
#     AND dsi.APPL_EXCL_IN = 1
#     AND dsi.BK_IN = 1
#   GROUP BY dsi.APPL_NB
# )
# """

# BASE_SELECT = """
# SELECT
#   dsi.APPL_NB,
#   MAX(dsi.LOAN_VERF_BK_DT) AS LOAN_VERF_BK_DT,
#   MAX(dsi.DIR_LOC_CD)      AS DIR_LOC_CD,
#   MAX(dsi.PROD_TYPE_NM)    AS PROD_TYPE_NM,
#   MAX(dsi.CHNL_CD)         AS CHNL_CD,
#   MAX(dsi.ORGN_CHNL_NM)    AS ORGN_CHNL_NM,
#   bu.BOOKED_USER           AS BOOKED_USER,
#   MAX(CASE
#         WHEN bu.BOOKED_USER IN ('CAFVASC','CAFECON')
#          AND orgn.ADJC_DCSN_USR_ID IN ('CAFVASC','CAFECON')
#          AND orgn.ACTV_ADDL_CMNT_TX ILIKE '%@%'
#       THEN orgn.ACTV_ADDL_CMNT_TX
#   END) AS EMAIL,
#   LISTAGG(CASE WHEN orgn.ACTV_TYPE_CD = 'DR' THEN orgn.ADJC_DCSN_USR_ID END, ', ')
#     WITHIN GROUP (ORDER BY orgn.ADJC_DCSN_USR_ID) AS DR_USERS,
#   LISTAGG(CASE WHEN orgn.ACTV_TYPE_CD = 'RC' THEN orgn.ADJC_DCSN_USR_ID END, ', ')
#     WITHIN GROUP (ORDER BY orgn.ADJC_DCSN_USR_ID) AS RE_USERS,
#   LISTAGG(CASE WHEN orgn.ACTV_TYPE_CD = 'FX' THEN orgn.ADJC_DCSN_USR_ID END, ' ')
#     WITHIN GROUP (ORDER BY orgn.ADJC_DCSN_USR_ID) AS FX_USERS
# FROM PROD_110575_ICDW_DB.AUTO_V.AFNC_DSI_ORGN_ACCT_DY dsi
# JOIN First_DR_Transaction fdr
#   ON dsi.APPL_NB = fdr.APPL_NB
# JOIN PROD_110575_ICDW_DB.AUTO_V.AFNC_DSI_ORGN_ACTV_DY_SUM orgn
#   ON orgn.AUTO_FNCE_ORGN_DIM_ID = dsi.AUTO_FNCE_ORGN_DIM_ID
# LEFT JOIN PROD_110575_ICDW_DB.AUTO_V.AFNC_BF_ORGN_CNTRCT_EXCP_DY excpt
#   ON dsi.APPL_NB = excpt.APPL_NB
# JOIN Booked_User_CTE bu
#   ON dsi.APPL_NB = bu.APPL_NB
# WHERE dsi.LOAN_VERF_BK_DT BETWEEN '{FROM}' AND '{TO}'
#   AND dsi.SNPST_DT = (
#     SELECT MAX(SNPST_DT)
#     FROM PROD_110575_ICDW_DB.AUTO_V.AFNC_DSI_ORGN_ACCT_DY
#   )
#   AND dsi.APPL_EXCL_IN = 1
#   AND dsi.BK_IN = 1
#   AND orgn.ACTV_TS >= fdr.FIRST_DR_ACTV_TS
# GROUP BY dsi.APPL_NB, bu.BOOKED_USER
# """

# def _materialize_view(from_date: str, to_date: str) -> str:
#     return (
#         BASE_CTE.format(FROM=from_date, TO=to_date)
#         + "\n, "
#         + AUTOBOOK_TABLE
#         + " AS (\n"
#         + BASE_SELECT.format(FROM=from_date, TO=to_date)
#         + "\n)"
#     )


# def build_autobook_sql(prompt: str, limit_hint: int | None = 200) -> str:
#     # 1) dates
#     from_dt, to_dt = _infer_dates(prompt)

#     # 2) materialize virtual table
#     with_view = _materialize_view(from_dt, to_dt)

#     # 3) build WHERE parts
#     where_clauses: List[str] = []
#     where_clauses += _business_clauses(prompt)

#     # strict equals from your direct-filter extractor (authorized columns)
#     comparative_filters, filtered_cols = extract_comparative_filters(prompt, AUTOBOOK_COLUMNS_META)
#     direct_filters = extract_direct_column_filters(prompt, AUTOBOOK_COLUMNS_META, filtered_columns=filtered_cols)

#     # ensure normalization like “loan/retail/subaru” → equals
#     norm_equality = _normalized_equals_from_prompt(prompt)

#     # Only allow filters on declared columns
#     all_filters = list(dict.fromkeys(list(comparative_filters) + list(direct_filters) + norm_equality))

#     # 4) aggregation?
#     agg_func, agg_col = detect_aggregation(prompt, AUTOBOOK_COLUMNS_META)

#     if agg_func:
#         # percentage & others handled exactly like mapper flow
#         percentage_condition = None
#         percentage_denominator_condition = None

#         # Let build_aggregation_query craft SELECT ... FROM AUTOBOOK_VIEW ...
#         query = build_aggregation_query(
#             agg_func,
#             agg_col,
#             AUTOBOOK_TABLE,
#             prompt,
#             AUTOBOOK_COLUMNS_META,
#             percentage_condition,
#             percentage_denominator_condition
#         )

#         # Inject WHERE (same approach you use in mapper)
#         if where_clauses or all_filters:
#             where_sql = " WHERE " + " AND ".join(where_clauses + all_filters)
#             import re as _re
#             query = _re.sub(r"\bWHERE\b.*?(LIMIT|$)", where_sql, query, flags=_re.IGNORECASE) or query + where_sql

#         # Apply limit if your builder didn’t
#         if "LIMIT" not in query.upper() and limit_hint:
#             query += f" LIMIT {int(limit_hint)}"

#         return f"{with_view}\n{query}"

#     # 5) non-aggregation SELECT
#     select_cols = ", ".join(AUTOBOOK_COLUMNS_META.keys())
#     outer = [f"SELECT {select_cols} FROM {AUTOBOOK_TABLE}"]
#     if where_clauses or all_filters:
#         outer.append("WHERE " + " AND ".join(where_clauses + all_filters))
#     if limit_hint:
#         outer.append(f"LIMIT {int(limit_hint)}")

#     return f"{with_view}\n" + "\n".join(outer)


# def run_autobook(prompt: str, limit: int | None = 200):
#     sql = build_autobook_sql(prompt, limit_hint=limit)
#     rows = execute_sql(sql)
#     return rows, sql






# autobook_handler.py
import re
from datetime import datetime
from typing import Dict, List, Tuple, Optional

# --- Reuse the same project utilities used by mapper ---
from db import execute_sql
from mapper_utils import (
    extract_direct_column_filters,
    extract_comparative_filters,
    parse_date_range_from_prompt,
)
from prompt_utils import extract_limit_from_prompt
from aggregation_handler import detect_aggregation, build_aggregation_query

# -------------------------------------------------------
# Virtual table name + column metadata (UPPERCASE columns)
# -------------------------------------------------------
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

# -------------------------------------------------------
# Hard-coded business mapping (as requested)
#   - "autobook"  -> BOOKED_USER = 'CAFVASC'
#   - "manual"    -> BOOKED_USER <> 'CAFVASC'
# -------------------------------------------------------
_BUSINESS_RULES: List[Tuple[re.Pattern, str]] = [
    (re.compile(r"\bauto[- ]?book\b|\bcfsin\b", re.I), "BOOKED_USER = 'CAFVASC'"),
    (re.compile(r"\bmanual\b|\bagent booked\b|\bhuman\b", re.I), "BOOKED_USER <> 'CAFVASC'"),
]

# Optional normalizers so free-text maps to strict equals for the two direct-filter columns
_NORMALIZE_EQ: Dict[str, Dict[str, str]] = {
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

# -------------------------------------
# DNA base query (cleaned + parameterized)
# We inline FROM/TO as ISO dates (Snowflake/ANSI style).
# -------------------------------------
_BASE_CTE = """
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

_BASE_SELECT = """
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

# ---------------------------
# Helpers
# ---------------------------
def _infer_date_window(prompt: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Use the same parser as mapper. Returns (from_iso, to_iso) or (None, None) if no range.
    """
    dr = parse_date_range_from_prompt(prompt)
    if dr and dr.get("from") and dr.get("to"):
        return dr["from"], dr["to"]
    return None, None  # keep None so LIMIT fallback matches mapper behavior

def _business_where(prompt: str) -> List[str]:
    where: List[str] = []
    for rx, clause in _BUSINESS_RULES:
        if rx.search(prompt):
            where.append(clause)
    return where

def _normalized_equals_from_prompt(prompt: str) -> List[str]:
    """
    Safety net so phrases like 'product type loan' / 'channel retail'
    become strict equals even if direct filter extractor misses them.
    """
    eqs: List[str] = []
    for col, rx_map in _NORMALIZE_EQ.items():
        for rx, val in rx_map.items():
            if re.search(rx, prompt, re.I):
                eqs.append(f"{col} = '{val}'")
    return eqs

def _materialize_view(from_date: str, to_date: str) -> str:
    return (
        _BASE_CTE.format(FROM=from_date, TO=to_date)
        + "\n, "
        + AUTOBOOK_TABLE
        + " AS (\n"
        + _BASE_SELECT.format(FROM=from_date, TO=to_date)
        + "\n)"
    )

# ---------------------------
# Public API
# ---------------------------
def build_autobook_sql(prompt: str, limit_hint: Optional[int] = None) -> str:
    """
    Build final SQL for the /autobook route, reusing the same mechanics as mapper:
    - date parsing (mapper_utils.parse_date_range_from_prompt)
    - business terms mapping (hard-coded CAFVASC rule here)
    - direct/comparative filters (mapper_utils)
    - aggregation (aggregation_handler)
    - prompt-based LIMIT (prompt_utils.extract_limit_from_prompt)
    """
    prompt_lower = prompt.lower()

    # 1) Dates
    from_dt, to_dt = _infer_date_window(prompt)
    # If no dates in prompt, use FIRST-OF-CURRENT-MONTH..TODAY inside the materialized view
    # (keeps typical month window while still honoring mapper's default-limit behavior outside)
    if not (from_dt and to_dt):
        today = datetime.utcnow().date()
        from_dt = today.replace(day=1).isoformat()
        to_dt = today.isoformat()

    # 2) Build the virtual table
    with_view = _materialize_view(from_dt, to_dt)

    # 3) WHERE clauses from business rules + filters
    where_clauses: List[str] = []
    where_clauses += _business_where(prompt)

    # Allow only declared columns (same pattern mapper uses)
    comparative_filters, filtered_cols = extract_comparative_filters(prompt, AUTOBOOK_COLUMNS_META)
    direct_filters = extract_direct_column_filters(
        prompt,
        AUTOBOOK_COLUMNS_META,
        filtered_columns=filtered_cols
    )
    normalized_equals = _normalized_equals_from_prompt(prompt)

    # Collapse duplicates while keeping order
    all_filters = list(dict.fromkeys(list(comparative_filters) + list(direct_filters) + normalized_equals))

    # 4) Aggregation detection (same API as mapper)
    try:
        agg_func, agg_col = detect_aggregation(prompt, AUTOBOOK_COLUMNS_META)
    except Exception:
        agg_func, agg_col = None, None

    # Prompt-based LIMIT (same behavior as mapper)
    extracted_limit = extract_limit_from_prompt(prompt)  # default in your util is 50
    # If the caller explicitly passes a limit (e.g., via JSON body), that wins
    if limit_hint is not None:
        extracted_limit = int(limit_hint)

    # 5) Aggregation path
    if agg_func:
        # percentage args are kept None here; your build_aggregation_query already handles them
        percentage_condition = None
        percentage_denominator_condition = None

        query = build_aggregation_query(
            agg_func,
            agg_col,
            AUTOBOOK_TABLE,
            prompt,
            AUTOBOOK_COLUMNS_META,
            percentage_condition,
            percentage_denominator_condition
        )

        # Inject WHERE
        if where_clauses or all_filters:
            where_sql = " WHERE " + " AND ".join(where_clauses + all_filters)
            # Try to replace an existing WHERE; otherwise append
            m = re.search(r"\bWHERE\b", query, flags=re.IGNORECASE)
            if m:
                query = re.sub(r"\bWHERE\b.*?(LIMIT|$)", where_sql + r" \1", query, flags=re.IGNORECASE)
            else:
                query += where_sql

        # LIMIT: if user specified a limit (different from default 50) OR there was no date range in the prompt,
        # mirror mapper’s behavior.
        if "LIMIT" not in query.upper():
            if extracted_limit != 50:
                query += f" LIMIT {extracted_limit}"
            elif not parse_date_range_from_prompt(prompt):  # only apply default when no explicit date range in prompt
                query += " LIMIT 50"

        return f"{with_view}\n{query}"

    # 6) Non-aggregation SELECT
    selected_cols = ", ".join(AUTOBOOK_COLUMNS_META.keys())
    outer = [f"SELECT {selected_cols} FROM {AUTOBOOK_TABLE}"]
    if where_clauses or all_filters:
        outer.append("WHERE " + " AND ".join(where_clauses + all_filters))

    # LIMIT (same logic as aggregation branch)
    if extracted_limit != 50:
        outer.append(f"LIMIT {extracted_limit}")
    elif not parse_date_range_from_prompt(prompt):
        outer.append("LIMIT 50")

    return f"{with_view}\n" + "\n".join(outer)

def run_autobook(prompt: str, limit: Optional[int] = None):
    """
    Execute the AutoBook query and return (rows, sql).
    `limit` (if provided) overrides the prompt-derived limit.
    """
    sql = build_autobook_sql(prompt, limit_hint=limit)
    rows = execute_sql(sql)
    return rows, sql
