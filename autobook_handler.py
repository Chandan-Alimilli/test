# autobook_handler.py
"""
AutoBook handler — filters & aggregates applied directly on the DNA base query.

- Injects extra AND ... into the OUTER WHERE of the default query (raw columns).
- "Autobook" → bu.BOOKED_USER IN AUTBOOK_USER_CODES
- "Manual"   → COALESCE(bu.BOOKED_USER,'') NOT IN AUTBOOK_USER_CODES
- Other filters bind to dsi.PROD_TYPE_NM, dsi.ORGN_CHNL_NM, dsi.CHNL_CD, dsi.DIR_LOC_CD.
- Date phrases (this month, previous month, yesterday, from..to, on <date>, last N days) handled here.
- Aggregations wrap the SAME base SQL (never a custom table).
- Prints & logs the exact SQL executed.
"""

import re
import calendar
import logging
from datetime import datetime, timedelta
from typing import Optional, Tuple, List

# --- configure logging ---
logger = logging.getLogger("autobook")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

# --- your DB executor ---
from db import execute_sql

# =========================
# CONFIG: Autobook user codes
# =========================
# Adjust this list to whatever the DNA/business team certify as "autobook" booking users.
AUTBOOK_USER_CODES = ("CAFVASC", "CEIFS", "CFSIN")

# =========================
# DNA TEAM DEFAULT QUERY
# =========================
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
-- ##EXTRA_WHERE##   -- extra AND ... will be injected here
GROUP BY dsi.APPL_NB, bu.BOOKED_USER
"""

# =========================
# Date parsing (self-contained)
# =========================
_MONTH_WORDS = {
    "january": 1, "jan": 1, "february": 2, "feb": 2, "march": 3, "mar": 3,
    "april": 4, "apr": 4, "may": 5, "june": 6, "jun": 6, "july": 7, "jul": 7,
    "august": 8, "aug": 8, "september": 9, "sep": 9, "sept": 9,
    "october": 10, "oct": 10, "november": 11, "nov": 11, "december": 12, "dec": 12,
}

def _month_bounds(year: int, month: int) -> Tuple[str, str]:
    start = datetime(year, month, 1).date()
    last_day = calendar.monthrange(year, month)[1]
    end = datetime(year, month, last_day).date()
    return start.isoformat(), end.isoformat()

def _coerce_date(token: str) -> Optional[str]:
    try:
        if re.match(r"^\d{4}-\d{2}-\d{2}$", token):
            return datetime.strptime(token, "%Y-%m-%d").date().isoformat()
        if re.match(r"^\d{1,2}/\d{1,2}/\d{2,4}$", token):
            fmt = "%d/%m/%Y" if len(token.split("/")[-1]) == 4 else "%d/%m/%y"
            return datetime.strptime(token, fmt).date().isoformat()
        if re.match(r"^\d{1,2}-\d{1,2}-\d{2,4}$", token):
            fmt = "%d-%m-%Y" if len(token.split("-")[-1]) == 4 else "%d-%m-%y"
            return datetime.strptime(token, fmt).date().isoformat()
    except Exception:
        return None
    return None

def parse_date_range_from_prompt(prompt: str) -> Tuple[str, str]:
    txt = " ".join((prompt or "").lower().split())
    today = datetime.utcnow().date()

    date_token = r"(?:\d{4}-\d{2}-\d{2}|\d{1,2}[/-]\d{1,2}[/-]\d{2,4})"

    m = re.search(rf"\bbetween\s+({date_token})\s+and\s+({date_token})\b", txt)
    if m:
        a, b = _coerce_date(m.group(1)), _coerce_date(m.group(2))
        if a and b:
            return (a, b) if a <= b else (b, a)

    m = re.search(rf"\bfrom\s+({date_token})\s+(?:to|through|-)\s+({date_token})\b", txt)
    if m:
        a, b = _coerce_date(m.group(1)), _coerce_date(m.group(2))
        if a and b:
            return (a, b) if a <= b else (b, a)

    m = re.search(rf"\bon\s+({date_token})\b", txt)
    if m:
        d = _coerce_date(m.group(1))
        if d: return d, d

    if "yesterday" in txt:
        y = (today - timedelta(days=1)).isoformat()
        return y, y
    if "today" in txt:
        t = today.isoformat()
        return t, t

    m = re.search(r"\b(last|past)\s+(\d{1,3})\s+day(s)?\b", txt)
    if m:
        n = max(1, int(m.group(2)))
        start = (today - timedelta(days=n - 1)).isoformat()
        return start, today.isoformat()

    if "this week" in txt:
        start = today - timedelta(days=today.weekday())
        end = start + timedelta(days=6)
        return start.isoformat(), end.isoformat()
    if "last week" in txt or "previous week" in txt:
        end = today - timedelta(days=today.weekday() + 1)
        start = end - timedelta(days=6)
        return start.isoformat(), end.isoformat()

    if "this month" in txt or "current month" in txt:
        return _month_bounds(today.year, today.month)
    if "previous month" in txt or "last month" in txt:
        y, mth = today.year, today.month - 1
        if mth == 0: y, mth = y - 1, 12
        return _month_bounds(y, mth)

    for w, mnum in _MONTH_WORDS.items():
        if re.search(rf"\b{w}\b", txt):
            year = today.year
            ym = re.search(rf"{w}\s+(\d{{4}})", txt)
            if ym: year = int(ym.group(1))
            return _month_bounds(year, mnum)

    # default safe window: current month
    return _month_bounds(today.year, today.month)

# =========================
# Prompt filters & limit
# =========================

def extract_limit_from_prompt(prompt: str) -> int:
    txt = " ".join((prompt or "").lower().split())
    m = (re.search(r"\blimit\s+(\d+)\b", txt) or
         re.search(r"\btop\s+(\d+)\b", txt) or
         re.search(r"\bfirst\s+(\d+)\b", txt) or
         re.search(r"\bhead\s+(\d+)\b", txt))
    if m:
        try:
            n = int(m.group(1))
            return max(1, min(n, 10000))
        except Exception:
            pass
    return 50

def _sanitize_literal(value: str) -> str:
    """Uppercase and allow alnum, space, -_/& only; collapse spaces."""
    v = re.sub(r"[^A-Za-z0-9 _\-\/&]", "", value or "").upper().strip()
    v = re.sub(r"\s+", " ", v)
    return v

def parse_filters_to_raw_columns(prompt: str) -> List[str]:
    """
    Convert English to SQL conditions AGAINST RAW COLUMNS in the base query:
      - bu.BOOKED_USER (autobook/manual)
      - dsi.ORGN_CHNL_NM, dsi.PROD_TYPE_NM, dsi.CHNL_CD, dsi.DIR_LOC_CD
    Returns list of SQL expressions WITHOUT leading AND.
    """
    txt = " ".join((prompt or "").lower().split())
    conds: List[str] = []

    # ---- autobook / manual
    if re.search(r"\bauto[- ]?book", txt) or re.search(r"\bcfsin\b", txt) or re.search(r"\bceifs\b", txt):
        in_list = ", ".join(f"'{c}'" for c in AUTBOOK_USER_CODES)
        conds.append(f"bu.BOOKED_USER IN ({in_list})")
    if re.search(r"\bmanual\b|\bagent booked\b|\bhuman\b", txt):
        in_list = ", ".join(f"'{c}'" for c in AUTBOOK_USER_CODES)
        conds.append(f"COALESCE(bu.BOOKED_USER,'') NOT IN ({in_list})")

    # ---- product type (loan/lease or explicit)
    m = re.search(r"\b(product\s*type|prod\s*type|type)\s*(is|=)?\s*([a-z ]+)\b", txt)
    if m:
        val = _sanitize_literal(m.group(3))
        if "LOAN" in val: conds.append("dsi.PROD_TYPE_NM = 'LOAN'")
        elif "LEASE" in val: conds.append("dsi.PROD_TYPE_NM = 'LEASE'")
        else: conds.append(f"dsi.PROD_TYPE_NM = '{val}'")
    else:
        if re.search(r"\bloan(s)?\b", txt) and "dsi.PROD_TYPE_NM = 'LOAN'" not in conds:
            conds.append("dsi.PROD_TYPE_NM = 'LOAN'")
        if re.search(r"\blease(s)?\b", txt) and "dsi.PROD_TYPE_NM = 'LEASE'" not in conds:
            conds.append("dsi.PROD_TYPE_NM = 'LEASE'")

    # ---- origination channel name (aka original channel name)
    m = re.search(r"\b(origination|original)\s*channel\s*(name)?\s*(is|=)?\s*([a-z &/-]+)\b", txt)
    if m:
        val = _sanitize_literal(m.group(4))
        # common synonyms
        if "RETAIL" in val: val = "RETAIL"
        if "SUBARU" in val: val = "SUBARU"
        conds.append(f"dsi.ORGN_CHNL_NM = '{val}'")
    else:
        if re.search(r"\bchannel\b.*\bretail\b", txt) and "dsi.ORGN_CHNL_NM = 'RETAIL'" not in conds:
            conds.append("dsi.ORGN_CHNL_NM = 'RETAIL'")

    # ---- channel code (CHNL_CD)
    m = re.search(r"\bchannel\s*code\s*(is|=)?\s*([a-z0-9]+)\b", txt)
    if m:
        val = _sanitize_literal(m.group(2))
        conds.append(f"dsi.CHNL_CD = '{val}'")

    # ---- direct location code (DIR_LOC_CD)
    m = re.search(r"\b(dir(?:ect)?\s*loc(?:ation)?\s*code|dir[_ ]?loc[_ ]?cd)\s*(is|=)?\s*([a-z0-9]+)\b", txt)
    if m:
        val = _sanitize_literal(m.group(3))
        conds.append(f"dsi.DIR_LOC_CD = '{val}'")

    # de-duplicate (preserve order)
    seen = set()
    out = []
    for c in conds:
        if c not in seen:
            out.append(c); seen.add(c)
    return out

# =========================
# Aggregation helpers
# =========================
_GROUPABLE = {
    "channel": "ORGN_CHNL_NM",
    "product": "PROD_TYPE_NM",
    "product type": "PROD_TYPE_NM",
    "booked user": "BOOKED_USER",
    "dir loc": "DIR_LOC_CD",
    "dir loc code": "DIR_LOC_CD",
    "dir location": "DIR_LOC_CD",
}

def wants_count(prompt: str) -> bool:
    t = " ".join((prompt or "").lower().split())
    return bool(re.search(r"\bhow many\b|\bcount\b|\bnumber of\b|\bno\.?\s+of\b", t))

def group_by_column(prompt: str) -> Optional[str]:
    t = " ".join((prompt or "").lower().split())
    m = re.search(r"\bby\s+([a-z _]+)\b", t)
    if not m:
        return None
    key = m.group(1).strip()
    for k, col in _GROUPABLE.items():
        if k in key:
            return col
    if key.upper() in ("ORGN_CHNL_NM", "PROD_TYPE_NM", "BOOKED_USER", "DIR_LOC_CD"):
        return key.upper()
    return None

# =========================
# SQL assembly
# =========================
def _inject_extra_where(base_select_sql: str, extra_conditions: List[str]) -> str:
    """
    Insert extra AND ... into the existing WHERE block (before GROUP BY).
    The base SELECT has a marker, but we also fall back to regex if needed.
    """
    if not extra_conditions:
        return base_select_sql
    extra = "  AND " + " AND ".join(extra_conditions) + "\n"
    if "-- ##EXTRA_WHERE##" in base_select_sql:
        return base_select_sql.replace("-- ##EXTRA_WHERE##", extra.strip("\n"))
    return re.sub(r"(WHERE\b[\s\S]*?)(\bGROUP BY\b)", r"\1" + extra + r"\2",
                  base_select_sql, flags=re.IGNORECASE)

def build_autobook_sql(prompt: str, limit_hint: Optional[int] = None) -> str:
    # 1) Date window
    frm, to = parse_date_range_from_prompt(prompt)
    logger.info(f"AUTOBOOK date window: {frm} -> {to}")

    # 2) Parse filters → RAW columns
    extra_where = parse_filters_to_raw_columns(prompt)

    # 3) Assemble base SQL with date substitutions and filter injection
    cte = _BASE_CTE.format(FROM=frm, TO=to)
    select_with_filters = _inject_extra_where(_BASE_SELECT.format(FROM=frm, TO=to), extra_where)

    # 4) Limit
    limit_val = limit_hint if limit_hint is not None else extract_limit_from_prompt(prompt)

    # 5) Aggregation?
    if wants_count(prompt):
        grp = group_by_column(prompt)
        if grp:
            final = f"""
{cte}
, AUTBOOK_BASE AS (
{select_with_filters}
)
SELECT {grp}, COUNT(*) AS CNT
FROM AUTBOOK_BASE
GROUP BY {grp}
ORDER BY CNT DESC
LIMIT {limit_val}
""".strip()
        else:
            final = f"""
{cte}
SELECT COUNT(*) AS CNT
FROM (
{select_with_filters}
) t
""".strip()
    else:
        final = (cte + "\n" + select_with_filters).strip()
        if limit_val:
            final += f"\nLIMIT {int(limit_val)}"

    print("\n[AutoBook Generated SQL]\n", final)
    logger.info("Generated SQL:\n%s", final)
    return final

def run_autobook(prompt: str, limit: Optional[int] = None):
    sql = build_autobook_sql(prompt, limit_hint=limit)
    rows = execute_sql(sql)
    return rows, sql
