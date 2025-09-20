# # import requests, json

# # BASE = "https://www.courtlistener.com/api/rest/v3/"
# # HEADERS = {"Authorization": f"Token {"6e823e2831234dc8de47c6113ac7eb631b5eec13"}"}



# # test.py
# # Fetch full opinion text (not just URLs) and return JSON with:
# # - metadata (case_name, court, date, links)
# # - inputs_text  (facts/narrative for RAG)
# # - judgment_text (disposition/holding for scoring)

# import json, re, html, requests

# BASE = "https://www.courtlistener.com/api/rest/v3/"
# API_KEY = "6e823e2831234dc8de47c6113ac7eb631b5eec13"  # <-- paste your key here

# HEADERS = {
#     "Authorization": f"Token {API_KEY}",
#     "User-Agent": "ai-law-platform-local-test/0.2"
# }
# TIMEOUT = 30

# # ---------- HTML → text helpers ----------
# def html_to_text(s: str) -> str:
#     if not s: return ""
#     # Normalize common breaks to newlines
#     s = re.sub(r"(?i)<\s*br\s*/?>", "\n", s)
#     s = re.sub(r"(?i)</\s*p\s*>", "\n", s)
#     # Strip all tags
#     s = re.sub(r"<[^>]+>", "", s)
#     # Collapse multiple blank lines
#     s = re.sub(r"\n{3,}", "\n\n", s)
#     return html.unescape(s).strip()

# # ---------- API wrappers ----------
# def get(url, params=None):
#     r = requests.get(url, headers=HEADERS, params=params, timeout=TIMEOUT)
#     r.raise_for_status()
#     return r

# def j(url, params=None):
#     return get(url, params).json()

# def search_opinions(query, start="2020-01-01", end="2025-12-31", limit=10):
#     data = j(
#         BASE + "search/",
#         params={
#             "q": query,
#             "type": "o",
#             "date_filed_after": start,
#             "date_filed_before": end,
#             "order_by": "score desc",
#             "page_size": limit,
#         },
#     )
#     return data.get("results", [])

# def fetch_opinion(opinion_id):
#     return j(BASE + f"opinions/{opinion_id}/")

# def fetch_cluster(cluster_id):
#     return j(BASE + f"clusters/{cluster_id}/")

# def fetch_url(url):
#     return j(url)

# # ---------- Full-body fetch with fallbacks ----------
# def fetch_opinion_body(op_json: dict) -> str:
#     # 1) prefer plain_text
#     txt = op_json.get("plain_text")
#     if txt: return txt

#     # 2) html_with_citations
#     html_body = op_json.get("html_with_citations")
#     if html_body: return html_to_text(html_body)

#     # 3) download_url (NY reporter pages, etc.)
#     dl = op_json.get("download_url")
#     if dl:
#         r = requests.get(dl, timeout=TIMEOUT)
#         r.raise_for_status()
#         return html_to_text(r.text)

#     # 4) absolute_url (CourtListener page)
#     absu = op_json.get("absolute_url")
#     if absu:
#         url = "https://www.courtlistener.com" + absu if absu.startswith("/") else absu
#         r = requests.get(url, timeout=TIMEOUT)
#         r.raise_for_status()
#         return html_to_text(r.text)

#     return ""

# # ---------- Inputs vs Judgment extractor (simple heuristics) ----------
# INPUT_KEYS = [
#     "rear-end", "collision", "accident", "vehicle", "towing", "tow",
#     "mph", "miles per hour", "stopped", "traffic", "lane", "expressway",
#     "bronx", "queens", "brooklyn", "manhattan", "major deegan", "date", "on ",
#     "plaintiff", "defendant", "according to"
# ]
# JUDGMENT_KEYS = [
#     "accordingly", "it is ordered", "order, supreme court", "reversed",
#     "affirmed", "vacated", "modified", "motion granted", "summary judgment",
#     "liability", "this constitutes the decision and order", "dismissed",
#     "granted", "denied"
# ]

# def extract_inputs_and_judgment(text: str) -> dict:
#     lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
#     inputs, judgment = [], []
#     for L in lines:
#         low = L.lower()
#         if any(k in low for k in JUDGMENT_KEYS):
#             judgment.append(L)
#         elif any(k in low for k in INPUT_KEYS):
#             inputs.append(L)
#     # Fallbacks: if nothing matched, take first and last paragraphs
#     if not inputs and lines:
#         inputs = lines[:6]
#     if not judgment and lines:
#         judgment = [p for p in lines if "ordered" in p.lower() or "this constitutes" in p.lower()]
#         if not judgment:
#             judgment = lines[-4:]
#     return {
#         "inputs_text": "\n".join(inputs[:12]).strip(),
#         "judgment_text": "\n".join(judgment[:12]).strip()
#     }

# # ---------- Build per-case JSON ----------
# def normalize_opinion_hit(hit: dict) -> dict:
#     """Resolve a search hit to an opinion JSON (handles id vs cluster)."""
#     # Most hits have 'id' (opinion id). Use that first.
#     op_id = hit.get("id")
#     if op_id:
#         return fetch_opinion(op_id)

#     # Fallback: pull cluster and take first sub opinion
#     cluster_id = hit.get("cluster_id")
#     if cluster_id:
#         cl = fetch_cluster(cluster_id)
#         subs = cl.get("sub_opinions", []) or cl.get("opinions", [])
#         if subs:
#             first = subs[0]
#             if isinstance(first, str):
#                 return fetch_url(first)
#             elif isinstance(first, int):
#                 return fetch_opinion(first)
#             elif isinstance(first, dict) and "id" in first:
#                 return fetch_opinion(first["id"])
#     raise RuntimeError("Unable to resolve opinion from search hit.")

# def build_case_json(op_json: dict) -> dict:
#     body = fetch_opinion_body(op_json)
#     split = extract_inputs_and_judgment(body)
#     abs_url = op_json.get("absolute_url")
#     abs_url = ("https://www.courtlistener.com" + abs_url) if isinstance(abs_url, str) and abs_url.startswith("/") else abs_url

#     return {
#         "id": op_json.get("id"),
#         "case_name": op_json.get("caseName") or op_json.get("case_name"),
#         "court": op_json.get("court") or op_json.get("court_citation_string"),
#         "date_filed": op_json.get("dateFiled") or op_json.get("date_filed"),
#         "citation": op_json.get("citation") or op_json.get("citations") or [],
#         "absolute_url": abs_url,
#         "download_url": op_json.get("download_url"),
#         "inputs_text": split["inputs_text"],
#         "judgment_text": split["judgment_text"],
#         # Optional: include an excerpt of the full body for debugging
#         "body_excerpt": (body[:1200] + ("... [truncated]" if len(body) > 1200 else "")),
#     }

# # ---------- MAIN ----------
# if __name__ == "__main__":
#     if not API_KEY or "YOUR_SINGLE_COURTLISTENER_API_KEY" in API_KEY:
#         raise SystemExit("❌ Paste your CourtListener API key into API_KEY.")

#     QUERY = '"rear-end collision" AND ("New York" OR Bronx)'
#     START, END = "2020-01-01", "2025-12-31"
#     LIMIT = 5  # how many cases to return

#     try:
#         hits = search_opinions(QUERY, start=START, end=END, limit=LIMIT)
#         if not hits:
#             print(json.dumps({"results": [], "message": "No opinions found."}, indent=2))
#         else:
#             results = []
#             for hit in hits:
#                 try:
#                     op_json = normalize_opinion_hit(hit)
#                     case_json = build_case_json(op_json)
#                     results.append(case_json)
#                 except Exception as e:
#                     # Skip bad records; include reason for debugging
#                     results.append({"error": str(e), "hit_absolute_url": hit.get("absolute_url")})

#             print(json.dumps({"query": QUERY, "count": len(results), "results": results}, indent=2))
#     except requests.HTTPError as e:
#         print(json.dumps({"error": f"HTTPError: {e}", "query": QUERY}, indent=2))














# test.py — intake-driven matcher with compact queries + robust retries
# - Builds small, meaningful queries from the intake (no huge OR lists)
# - Requires: state match; and (if present) traffic + speed limit must appear in text
# - Robust requests.Session with Retry/backoff; short page_size to reduce server load
# - Falls back to simpler queries automatically on ConnectTimeout

import json, re, html, time, os, requests
from typing import Any, Dict, List, Optional, Union
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ========= CONFIG =========
BASE = "https://www.courtlistener.com/api/rest/v3/"
API_KEY = "6e823e2831234dc8de47c6113ac7eb631b5eec13"   # <-- paste your key here
USER_AGENT = "ai-law-platform-intake-matcher/1.1"
CONNECT_TIMEOUT = 10
READ_TIMEOUT = 45
TIMEOUT = (CONNECT_TIMEOUT, READ_TIMEOUT)
PAGE_SIZE = 5            # smaller pages to keep responses fast
PER_QUERY = 5
TOP_K = 15
DATE_START, DATE_END = "2015-01-01", "2025-12-31"
STRICT = True           # enforce state + traffic + speed limit locally
INCLUDE_CITY_IN_QUERY = False  # per your note: state must match, city optional

# ========= SAMPLE INTAKE (replace with your request) =========
CLIENT_REQUEST = {
  "incident": {
    "type": "rear_end_collision",
    "datetime_iso": "2025-06-04T08:10:00-04:00",
    "location": {"address": "Major Deegan Expy N near Exit 11", "city": "Bronx", "state": "NY", "coordinates": None},
    "conditions": {"weather": "clear", "road_surface": "dry", "traffic": "heavy", "speed_limit_mph": 50},
    "vehicle_movement": {"client_status": "stopped", "stopped_duration_seconds": 10, "signal_used": False},
    "diagram_or_media": ["dashcam_front.mp4", "dashcam_rear.mp4", "scene_photo_1.jpg"],
    "cameras_present": ["NYC DOT traffic cam - Exit 11"]
  },
  "vehicles": [
    {"role": "client", "year": 2021, "make": "Honda", "model": "Accord", "plate": "ABC-1234", "vin_last4": "7890", "damage_summary": "rear bumper/trunk/sensors"},
    {"role": "defendant", "type": "pickup_tow", "owner": "Municipal fleet", "employer": "City agency", "damage_summary": "front bumper"}
  ],
  "flags": {"municipal_defendant": True}
}

STATE_ABBR_TO_NAME = {
  "NY":"New York","NJ":"New Jersey","CT":"Connecticut","PA":"Pennsylvania","MA":"Massachusetts","VT":"Vermont",
  "NH":"New Hampshire","ME":"Maine","RI":"Rhode Island","DE":"Delaware","MD":"Maryland","VA":"Virginia","DC":"District of Columbia",
  # (add more if needed)
}

# ========= SESSION WITH RETRIES =========
def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"Authorization": f"Token {API_KEY}", "User-Agent": USER_AGENT, "Accept": "application/json"})
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=1.5,
        status_forcelist=[429, 502, 503, 504],
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://", HTTPAdapter(max_retries=retry))
    # If your network uses a proxy, uncomment and set:
    # s.proxies.update({"https": "http://proxyhost:port", "http": "http://proxyhost:port"})
    return s

SESSION = None

def _get(url: str, params=None) -> requests.Response:
    global SESSION
    if SESSION is None:
        SESSION = make_session()
    try:
        r = SESSION.get(url, params=params, timeout=TIMEOUT)
        r.raise_for_status()
        return r
    except requests.exceptions.ConnectTimeout:
        # Fallback: try once more with no params or a simpler query
        if url.endswith("/search/") and params and "q" in params:
            simp = simplify_query(params["q"])
            r = SESSION.get(url, params={**params, "q": simp, "page_size": PAGE_SIZE}, timeout=TIMEOUT)
            r.raise_for_status()
            return r
        raise

def _json(url: str, params=None) -> Dict[str, Any]:
    return _get(url, params=params).json()

# ========= HTML → TEXT =========
def html_to_text(s: str) -> str:
    if not s: return ""
    s = re.sub(r"(?i)<\s*br\s*/?>", "\n", s)
    s = re.sub(r"(?i)</\s*p\s*>", "\n", s)
    s = re.sub(r"<[^>]+>", "", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return html.unescape(s).strip()

def full_abs_url(path_or_url: Optional[str]) -> Optional[str]:
    if not path_or_url: return None
    return ("https://www.courtlistener.com" + path_or_url) if path_or_url.startswith("/") else path_or_url

# ========= COURTLISTENER WRAPPERS =========
def search_opinions(q: str, page_size=PAGE_SIZE, limit=PER_QUERY):
    data = _json(
        BASE + "search/",
        params={"q": q, "type": "o", "date_filed_after": DATE_START, "date_filed_before": DATE_END,
                "order_by": "score desc", "page_size": page_size}
    )
    return data.get("results", [])[:limit]

def fetch_opinion(op_id: Union[int,str]) -> Optional[Dict[str,Any]]:
    try:
        return _json(BASE + f"opinions/{op_id}/")
    except requests.HTTPError:
        return None

def fetch_cluster(cl_id: Union[int,str]) -> Optional[Dict[str,Any]]:
    try:
        return _json(BASE + f"clusters/{cl_id}/")
    except requests.HTTPError:
        return None

def find_download_url_in_page(html_str: str) -> Optional[str]:
    for pat in [
        r'href=["\'](https?://[^"\']*nycourts\.gov/reporter[^"\']+)["\']',
        r'href=["\'](https?://[^"\']*courts\.state\.ny\.us/reporter[^"\']+)["\']',
        r'href=["\'](https?://[^"\']*3dseries/20\d{2}/\d{4}_\d+\.htm)["\']',
    ]:
        m = re.search(pat, html_str, flags=re.I)
        if m: return m.group(1)
    return None

def fetch_body_from_abs_page(abs_url: str) -> Dict[str, Any]:
    r = _get(abs_url)
    page_html = r.text
    dl = find_download_url_in_page(page_html)
    if dl:
        rr = _get(dl)
        return {"body_text": html_to_text(rr.text), "download_url": dl}
    return {"body_text": html_to_text(page_html), "download_url": None}

def load_body(hit_or_op: Dict[str, Any]) -> Dict[str, Any]:
    op = hit_or_op if "plain_text" in hit_or_op or "html_with_citations" in hit_or_op else None
    if not op and "id" in hit_or_op:
        op = fetch_opinion(hit_or_op["id"])
    if not op and "cluster_id" in hit_or_op:
        cl = fetch_cluster(hit_or_op["cluster_id"])
        subs = (cl or {}).get("sub_opinions", []) or (cl or {}).get("opinions", [])
        for item in subs:
            try:
                op = _json(item) if isinstance(item, str) else fetch_opinion(item if isinstance(item, int) else item["id"])
            except Exception:
                op = None
            if op: break

    txt, dl = "", None
    absu = full_abs_url((op or hit_or_op).get("absolute_url"))
    if op:
        txt = op.get("plain_text") or ""
        if not txt:
            html_body = op.get("html_with_citations")
            if html_body: txt = html_to_text(html_body)
        dl = op.get("download_url")
    if not txt and absu:
        fb = fetch_body_from_abs_page(absu)
        txt, dl = fb["body_text"], fb["download_url"]

    meta = {
        "case_name": (op or hit_or_op).get("caseName") or (op or hit_or_op).get("case_name"),
        "court": (op or hit_or_op).get("court") or (op or hit_or_op).get("court_citation_string"),
        "date_filed": (op or hit_or_op).get("dateFiled") or (op or hit_or_op).get("date_filed"),
        "citation": (op or hit_or_op).get("citation") or (op or hit_or_op).get("citations") or [],
        "absolute_url": absu, "download_url": dl
    }
    return {"body_text": txt, "meta": meta}

# ========= INTAKE → QUERIES (compact) =========
def phrase_variants(s: str) -> List[str]:
    s = (s or "").strip()
    if not s: return []
    base = s.replace("_"," ").strip()
    if base.lower() == "rear end collision":  # common special-case
        return ['"rear-end collision"', '"rear end collision"']
    return [f'"{base}"']

def simplify_query(q: str) -> str:
    # remove excessive ORs and keep only the first few clauses
    parts = re.split(r"\s+AND\s+", q)
    keep = []
    for p in parts:
        if len(" AND ".join(keep + [p])) > 400:
            break
        keep.append(p)
    return " AND ".join(keep)

def build_queries(req: Dict[str, Any]) -> List[str]:
    loc = req.get("incident", {}).get("location", {}) or {}
    state_abbr = (loc.get("state") or "").upper()
    state_name = STATE_ABBR_TO_NAME.get(state_abbr, state_abbr)
    city = loc.get("city")
    addr = loc.get("address","")

    itype = req.get("incident", {}).get("type") or ""
    q_type = " OR ".join(phrase_variants(itype)) or '"motor vehicle"'

    must = [q_type]
    if state_name:
        must.append(f'("{state_name}")')

    opt = []

    # address / roadway focus (compact)
    m = re.search(r"(major\s+deegan)", addr, flags=re.I)
    if m:
        opt.append('"Major Deegan"')

    # municipal flag
    if req.get("flags", {}).get("municipal_defendant"):
        opt.append('("NYPD" OR "City of New York" OR "Transit Authority")')

    # towing hint
    if any("tow" in (v.get("type","") + " " + v.get("owner","") + " " + v.get("employer","")).lower()
           for v in (req.get("vehicles") or [])):
        opt.append('("tow" OR "towing" OR "vehicle in tow")')

    # VTL §1103 (only if municipal)
    if req.get("flags", {}).get("municipal_defendant"):
        opt.append('("Vehicle and Traffic Law § 1103" OR "VTL 1103")')

    queries = []
    # Base, minimal (type + state)
    queries.append(" AND ".join(must))
    # Add combos with each optional bit, but keep them separate (avoid giant strings)
    for o in opt:
        queries.append(" AND ".join(must + [o]))
    # If city is allowed, add one blended query
    if INCLUDE_CITY_IN_QUERY and city:
        queries.append(" AND ".join(must + [f'("{city}")']))

    # Dedup + length cap
    uniq = []
    for q in queries:
        q2 = simplify_query(q)
        if q2 not in uniq:
            uniq.append(q2)
    return uniq[:6]  # keep it small

# ========= CONSTRAINTS (strict, post-retrieval) =========
def norm(s: Optional[str]) -> str:
    return re.sub(r"\s+", " ", (s or "").lower()).strip()

def has_state(text: str, req: Dict[str, Any]) -> bool:
    abbr = (req.get("incident", {}).get("location", {}) or {}).get("state")
    if not abbr: return True
    full = STATE_ABBR_TO_NAME.get(abbr.upper(), "")
    t = norm(text)
    return (full.lower() in t) or (f" {abbr.lower()} " in f" {t} ") or (" n.y." in t if abbr=="NY" else False)

def has_traffic(text: str, req: Dict[str, Any]) -> bool:
    val = (req.get("incident", {}).get("conditions", {}) or {}).get("traffic")
    if not val: return True
    return (f"{val} traffic".lower() in norm(text))

def has_speed_limit(text: str, req: Dict[str, Any]) -> bool:
    sl = (req.get("incident", {}).get("conditions", {}) or {}).get("speed_limit_mph")
    if not sl: return True
    t = norm(text)
    if f"{int(sl)} mph" in t: return True
    m = re.search(r"(speed\s+limit[^0-9]{0,10})(\d{2,3})", t)
    return bool(m and abs(int(m.group(2)) - int(sl)) <= 2)

def meets_constraints(text: str, req: Dict[str, Any]) -> bool:
    if not STRICT:
        return has_state(text, req)
    return has_state(text, req) and has_traffic(text, req) and has_speed_limit(text, req)

# ========= EXTRACTION shaped like your intake + judgment =========
def extract_client_like_input(raw_text: str, req: Dict[str, Any]) -> Dict[str, Any]:
    t = norm(raw_text)
    itype = req.get("incident", {}).get("type")
    state = req.get("incident", {}).get("location", {}).get("state")
    city = req.get("incident", {}).get("location", {}).get("city")
    traffic = (req.get("incident", {}).get("conditions", {}) or {}).get("traffic")
    speed = (req.get("incident", {}).get("conditions", {}) or {}).get("speed_limit_mph")
    client_status = req.get("incident", {}).get("vehicle_movement", {}).get("client_status")

    towing = bool(re.search(r"\btow(ing|ed| truck)?\b", t))
    municipal = any(x in t for x in ["city of new york","nypd","transit authority"])

    return {
      "incident": {
        "type": itype,
        "datetime_iso": None,
        "location": {"address": None, "city": city if city else None, "state": state, "coordinates": None},
        "conditions": {"weather": None, "road_surface": None, "traffic": traffic, "speed_limit_mph": speed},
        "vehicle_movement": {"client_status": client_status, "stopped_duration_seconds": None, "signal_used": None},
        "diagram_or_media": [], "cameras_present": []
      },
      "vehicles": [
        {"role": "client", "year": None, "make": None, "model": None, "plate": None, "vin_last4": None, "damage_summary": None},
        {"role": "defendant", "type": "tow" if towing else None, "owner": "municipal" if municipal else None, "employer": None, "damage_summary": None}
      ],
      "flags": {"municipal_defendant": municipal}
    }

def extract_judgment(raw_text: str) -> Dict[str, Any]:
    lines = [ln.strip() for ln in raw_text.split("\n") if ln.strip()]
    dispo = next((L for L in lines if re.search(r"\b(reversed|affirmed|vacated|modified|dismissed|granted|denied)\b", L, re.I)), None)
    order = next((L for L in lines if re.search(r"(THIS CONSTITUTES THE DECISION AND ORDER|IT IS ORDERED)", L, re.I)), None)
    holding = next((L for L in lines if re.search(r"(summary judgment|liability|negligence|reckless|prima facie)", L, re.I)), None)
    return {"disposition": dispo, "order": order, "holding": holding}

# ========= MAIN PIPELINE =========
def find_similar_cases(req: Dict[str, Any]) -> Dict[str, Any]:
    queries = build_queries(req)
    results = []
    seen = set()

    for q in queries:
        hits = []
        try:
            hits = search_opinions(q, page_size=PAGE_SIZE, limit=PER_QUERY)
        except Exception as e:
            # Already retried inside _get; skip this query if still failing
            continue

        for hit in hits:
            key = hit.get("absolute_url")
            if not key or key in seen: 
                continue
            seen.add(key)
            try:
                bundle = load_body(hit)
                body = bundle["body_text"]
                if not body:
                    continue
                if not meets_constraints(body, req):
                    continue

                client_like = extract_client_like_input(body, req)
                judgment = extract_judgment(body)
                src = {
                    "case_name": bundle["meta"]["case_name"],
                    "court": bundle["meta"]["court"],
                    "date_filed": bundle["meta"]["date_filed"],
                    "citation": bundle["meta"]["citation"],
                    "absolute_url": bundle["meta"]["absolute_url"],
                    "download_url": bundle["meta"]["download_url"],
                }
                results.append({"client_like_input": client_like, "judgment": judgment, "source": src})
                if len(results) >= TOP_K:
                    break
            except Exception:
                continue
        if len(results) >= TOP_K:
            break

    return {"query_plan": queries, "returned": len(results), "results": results}

if __name__ == "__main__":
    if not API_KEY or "YOUR_SINGLE_COURTLISTENER_API_KEY" in API_KEY:
        raise SystemExit("❌ Paste your CourtListener API key into API_KEY.")
    out = find_similar_cases(CLIENT_REQUEST)
    print(json.dumps(out, indent=2))
