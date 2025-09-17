import re
import json
import spacy
import dateparser
from datetime import datetime, timedelta
from dateutil import parser
from difflib import SequenceMatcher
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load SpaCy model
nlp = spacy.load("en_core_web_sm")

# Load business mappings
try:
    with open("business_mapping.json", "r") as f:
        BUSINESS_TERMS = json.load(f)
except Exception as e:
    logger.error(f"Failed to load business_mapping.json: {e}")
    raise

# Month aliases
MONTHS_MAP = {
    "january": 1, "jan": 1,
    "february": 2, "feb": 2,
    "march": 3, "mar": 3,
    "april": 4, "apr": 4,
    "may": 5,
    "june": 6, "jun": 6,
    "july": 7, "jul": 7,
    "august": 8, "aug": 8,
    "september": 9, "sep": 9, "sept": 9,
    "october": 10, "oct": 10,
    "november": 11, "nov": 11,
    "december": 12, "dec": 12,
}

# Quarter mappings
QUARTER_MAP = {
    "q1": (1, 3),  # Jan-Mar
    "q2": (4, 6),  # Apr-Jun
    "q3": (7, 9),  # Jul-Sep
    "q4": (10, 12) # Oct-Dec
}

# Normalize text using SpaCy lemmatization
def normalize_text(text: str) -> str:
    """Normalize text by lemmatizing and removing stopwords/punctuation."""
    doc = nlp(text.lower())
    return " ".join([token.lemma_ for token in doc if not token.is_stop and not token.is_punct])

# Fuzzy matching for column/description
def fuzzy_match(a: str, b: str, threshold: float = 0.8) -> bool:
    """Perform fuzzy matching between two strings."""
    return SequenceMatcher(None, a.lower(), b.lower()).ratio() >= threshold

# Extract noun phrases
def extract_entities(prompt: str) -> list:
    """Extract noun phrases from the prompt using SpaCy."""
    try:
        doc = nlp(prompt)
        entities = [chunk.text.lower() for chunk in doc.noun_chunks]
        logger.debug(f"Extracted entities: {entities}")
        return entities
    except Exception as e:
        logger.error(f"Error extracting entities: {e}")
        return []

# Date range parsing
def parse_date_range_from_prompt(prompt: str) -> tuple:
    """Parse date ranges from prompt, handling relative phrases, exact dates, and quarters."""
    prompt = prompt.lower().strip()
    from_dt, to_dt = None, None
    today = datetime.now()

    try:
        # Exact date match (yyyy-mm-dd)
        date_matches = re.findall(r"\d{4}-\d{2}-\d{1,2}", prompt)
        if len(date_matches) == 1:
            from_dt = to_dt = parser.parse(date_matches[0]).strftime("%Y-%m-%d")
            logger.debug(f"Parsed exact date: {from_dt}")
        elif len(date_matches) >= 2:
            from_dt = parser.parse(date_matches[0]).strftime("%Y-%m-%d")
            to_dt = parser.parse(date_matches[1]).strftime("%Y-%m-%d")
            logger.debug(f"Parsed date range: {from_dt} to {to_dt}")

        # Handle "from month to month" (e.g., "from April to June")
        month_range_match = re.search(r"from\s+(\w+)\s+to\s+(\w+)", prompt)
        if month_range_match:
            start_month, end_month = month_range_match.groups()
            start_month_num = MONTHS_MAP.get(start_month.lower())
            end_month_num = MONTHS_MAP.get(end_month.lower())
            if start_month_num and end_month_num:
                year = today.year
                from_dt = datetime(year, start_month_num, 1).strftime("%Y-%m-%d")
                to_dt = (datetime(year, end_month_num + 1, 1) - timedelta(days=1)).strftime("%Y-%m-%d")
                logger.debug(f"Parsed month range: {from_dt} to {to_dt}")
                return from_dt, to_dt

        # Quarter detection (e.g., "Q1", "last Q4", "Q2 of 2024")
        quarter_match = re.search(r"(?:last\s+)?q([1-4])(?:\s+of\s+(\d{4}))?", prompt)
        if quarter_match:
            is_last = "last" in quarter_match.group(0)
            quarter_num = quarter_match.group(1)
            year_str = quarter_match.group(2)
            year = today.year - 1 if is_last else today.year
            if year_str:
                year = int(year_str)
            start_month, end_month = QUARTER_MAP.get(f"q{quarter_num}")
            from_dt = datetime(year, start_month, 1).strftime("%Y-%m-%d")
            to_dt = (datetime(year, end_month + 1, 1) - timedelta(days=1)).strftime("%Y-%m-%d")
            logger.debug(f"Parsed quarter '{quarter_match.group(0)}': {from_dt} to {to_dt}")
            return from_dt, to_dt

        # Relative date phrases
        relative_phrases = {
            "this month": (
                today.replace(day=1),
                (today.replace(day=1) + timedelta(days=31)).replace(day=1) - timedelta(days=1)
            ),
            "last month": (
                (today.replace(day=1) - timedelta(days=1)).replace(day=1),
                today.replace(day=1) - timedelta(days=1)
            ),
            "this week": (
                today - timedelta(days=today.weekday()),
                today + timedelta(days=6 - today.weekday())
            ),
            "last week": (
                today - timedelta(days=today.weekday() + 7),
                today - timedelta(days=today.weekday() + 1)
            ),
            "today": (today, today),
            "yesterday": (
                today - timedelta(days=1),
                today - timedelta(days=1)
            ),
            "this year": (
                today.replace(month=1, day=1),
                today.replace(month=12, day=31)
            ),
            "last year": (
                today.replace(year=today.year - 1, month=1, day=1),
                today.replace(year=today.year - 1, month=12, day=31)
            )
        }

        for phrase, (start, end) in relative_phrases.items():
            if phrase in prompt:
                from_dt = start.strftime("%Y-%m-%d")
                to_dt = end.strftime("%Y-%m-%d")
                logger.debug(f"Parsed relative date phrase '{phrase}': {from_dt} to {to_dt}")
                break

        # Month names (e.g., "in May", "during Jan")
        if not from_dt and not to_dt:
            for word in prompt.split():
                word = word.strip(",.")
                if word in MONTHS_MAP:
                    year = today.year
                    month = MONTHS_MAP[word]
                    from_dt = f"{year}-{month:02d}-01"
                    to_dt = (datetime(year, month, 1) + timedelta(days=31)).replace(day=1) - timedelta(days=1)
                    to_dt = to_dt.strftime("%Y-%m-%d")
                    logger.debug(f"Parsed month name '{word}': {from_dt} to {to_dt}")
                    break

        # Fallback with dateparser for other relative expressions
        if not from_dt and not to_dt:
            parsed = dateparser.parse(prompt, settings={"PREFER_DATES_FROM": "past", "STRICT_PARSING": False})
            if parsed:
                from_dt = to_dt = parsed.strftime("%Y-%m-%d")
                logger.debug(f"Dateparser fallback: {from_dt}")

        if from_dt and to_dt:
            logger.info(f"Final date range: {from_dt} to {to_dt}")
        else:
            logger.warning("No valid date range parsed from prompt")
        return from_dt, to_dt

    except Exception as e:
        logger.error(f"Error parsing date range: {e}")
        return None, None

# Extract comparative filters
def extract_comparative_filters(prompt: str, metadata_columns: dict) -> tuple:
    """Extract SQL comparative filters (e.g., 'AMOUNT > 5000', 'AMOUNT BETWEEN 60000 AND 80000') from prompt and return filters with affected columns."""
    filters = []
    filtered_columns = set()  # Track columns with comparative filters
    prompt_lower = prompt.lower()
    doc = nlp(prompt)

    comparison_ops = {
        "less than or equal to": "<=",
        "greater than or equal to": ">=",
        "less than": "<",
        "more than": ">",
        "greater than": ">",
        "equal to": "=",
        "equals": "=",
        "=": "=",
        ">": ">",
        "<": "<",
        ">=": ">=",
        "<=": "<="
    }

    try:
        # Dependency parsing for comparative structures
        for token in doc:
            if token.dep_ in ("attr", "dobj", "pobj") and token.head.lemma_ in ("be", "have"):
                for child in token.head.children:
                    phrase = " ".join([t.text.lower() for t in child.subtree if not t.is_punct])
                    # Check for between range
                    between_match = re.search(r"(between|in between)\s+([\d,.]+(?:k)?)\s+and\s+([\d,.]+(?:k)?)", phrase)
                    if between_match:
                        op = "BETWEEN"
                        col_text = token.text
                        low_val = between_match.group(2).replace(',', '').replace('k', '000').strip()
                        high_val = between_match.group(3).replace(',', '').replace('k', '000').strip()
                        if not low_val.isdigit() or not high_val.isdigit():
                            logger.warning(f"Invalid numeric range values: {low_val}, {high_val}")
                            continue
                        low_val, high_val = min(int(low_val), int(high_val)), max(int(low_val), int(high_val))
                        for col, desc in metadata_columns.items():
                            if isinstance(desc, dict) and desc.get("type") == "numeric":
                                col_lower = col.lower()
                                desc_text = desc["desc"].lower()
                                if fuzzy_match(col_text, col_lower) or fuzzy_match(col_text, desc_text):
                                    filters.append(f"{col.upper()} BETWEEN {low_val} AND {high_val}")
                                    filtered_columns.add(col.upper())
                                    logger.debug(f"Parsed between filter: {col.upper()} BETWEEN {low_val} AND {high_val}")
                                    break
                        continue

                    # Check for other comparison operators
                    for op_phrase, symbol in comparison_ops.items():
                        if op_phrase in phrase:
                            value_token = None
                            for t in child.subtree:
                                if t.pos_ in ("NUM", "NOUN") and t.text.replace('.', '').isdigit():
                                    value_token = t.text
                                    break
                            if value_token:
                                for col, desc in metadata_columns.items():
                                    if isinstance(desc, dict) and desc.get("type") == "numeric":
                                        col_lower = col.lower()
                                        desc_text = desc["desc"].lower()
                                        if fuzzy_match(token.text, col_lower) or fuzzy_match(token.text, desc_text):
                                            value = f"'{value_token}'" if not value_token.replace('.', '').isdigit() else value_token
                                            filters.append(f"{col.upper()} {symbol} {value}")
                                            filtered_columns.add(col.upper())
                                            logger.debug(f"Parsed comparative filter: {col.upper()} {symbol} {value}")
                                            break
                            break

        # Regex fallback for explicit patterns
        for col, desc in metadata_columns.items():
            if isinstance(desc, dict) and desc.get("type") == "numeric":
                col_lower = col.lower()
                desc_text = desc["desc"].lower()
                # Between range regex
                between_pattern = rf"(?:{desc_text}|{col_lower})\s+(?:is\s+)?(?:between|in between)\s+([\d,.]+(?:k)?)\s+and\s+([\d,.]+(?:k)?)"
                match = re.search(between_pattern, prompt_lower)
                if match:
                    low_val = match.group(1).replace(',', '').replace('k', '000').strip()
                    high_val = match.group(2).replace(',', '').replace('k', '000').strip()
                    if not low_val.isdigit() or not high_val.isdigit():
                        logger.warning(f"Invalid numeric range values: {low_val}, {high_val}")
                        continue
                    low_val, high_val = min(int(low_val), int(high_val)), max(int(low_val), int(high_val))
                    filters.append(f"{col.upper()} BETWEEN {low_val} AND {high_val}")
                    filtered_columns.add(col.upper())
                    logger.debug(f"Regex parsed between filter: {col.upper()} BETWEEN {low_val} AND {high_val}")
                    continue

                # Other comparison operators
                for phrase, symbol in comparison_ops.items():
                    pattern = rf"(?:{desc_text}|{col_lower})\s+(?:is\s+)?{phrase}\s+([0-9]+(?:\.[0-9]+)?)"
                    match = re.search(pattern, prompt_lower)
                    if match:
                        value = match.group(1)
                        filters.append(f"{col.upper()} {symbol} {value}")
                        filtered_columns.add(col.upper())
                        logger.debug(f"Regex parsed comparative filter: {col.upper()} {symbol} {value}")
                        break

        # Remove duplicates while preserving order
        filters = list(dict.fromkeys(filters))
        logger.info(f"Extracted comparative filters: {filters}, affected columns: {filtered_columns}")
        return filters, filtered_columns

    except Exception as e:
        logger.error(f"Error extracting comparative filters: {e}")
        return [], set()

# Direct filters (e.g., "state code NY")
def extract_direct_column_filters(prompt: str, metadata_columns: dict, filtered_columns: set = None) -> list:
    """Extract direct SQL filters (e.g., "STATE_CODE = 'NY'") from prompt, avoiding columns with comparative filters."""
    filters = []
    filtered_columns = filtered_columns or set()  # Default to empty set if not provided
    ignore_values = {"in", "on", "at", "of", "to", "for", "from", "by", "with", "is", "and"}
    column_values = {}  # Track values per column for IN clause

    try:
        # Extract quoted phrases first
        quoted_phrases = re.findall(r"['\"]([^'\"]+)['\"]", prompt)
        prompt_lower = prompt.lower()
        for phrase in quoted_phrases:
            for col, desc in metadata_columns.items():
                if isinstance(desc, dict) and desc.get("type") != "numeric" and col.upper() not in filtered_columns:
                    col_lower = col.lower()
                    desc_lower = desc["desc"].lower() if isinstance(desc, dict) else desc.lower()
                    # Check if column or description appears before the quoted phrase
                    pattern = rf"(?:{re.escape(desc_lower)}|{re.escape(col_lower)})\s*(?:is\s+|from\s+|with\s+|of\s+)?['\"]{re.escape(phrase)}['\"]"
                    if re.search(pattern, prompt_lower):
                        # Add to column_values for potential IN clause
                        column_values.setdefault(col.upper(), []).append(phrase)
                        logger.debug(f"Parsed quoted direct filter: {col.upper()} = '{phrase}'")

        # Dependency parsing for non-quoted multi-word or single-word filters
        doc = nlp(prompt)
        for token in doc:
            if token.dep_ in ("attr", "dobj", "pobj"):
                for col, desc in metadata_columns.items():
                    if isinstance(desc, dict) and desc.get("type") != "numeric" and col.upper() not in filtered_columns:
                        col_lower = col.lower()
                        desc_lower = desc["desc"].lower()
                        if fuzzy_match(token.text, col_lower) or fuzzy_match(token.text, desc_lower):
                            # Collect tokens until a condition boundary or end
                            value_tokens = []
                            current_token = token.head
                            while current_token and current_token.text.lower() not in ignore_values:
                                for child in current_token.children:
                                    if child.pos_ in ("NOUN", "PROPN", "NUM") and child.text.lower() not in ignore_values:
                                        value_tokens.append(child.text)
                                    elif child.text in (",", "and"):
                                        # Handle multiple values for the same column
                                        value = " ".join(value_tokens).strip()
                                        if value and not any(phrase in value for phrase in quoted_phrases):
                                            column_values.setdefault(col.upper(), []).append(value)
                                            logger.debug(f"Parsed direct filter value: {col.upper()} = '{value}'")
                                        value_tokens = []
                                current_token = next((c for c in current_token.children if c.dep_ in ("conj", "appos")), None)
                                if not current_token or current_token.text.lower() in (",", "and"):
                                    break
                            # Add remaining value if any
                            if value_tokens:
                                value = " ".join(value_tokens).strip()
                                if value and not any(phrase in value for phrase in quoted_phrases):
                                    column_values.setdefault(col.upper(), []).append(value)
                                    logger.debug(f"Parsed direct filter value: {col.upper()} = '{value}'")

        # Regex fallback for non-quoted multi-word or single-word filters
        for col, desc in metadata_columns.items():
            if isinstance(desc, dict) and desc.get("type") != "numeric" and col.upper() not in filtered_columns:
                col_lower = col.lower()
                desc_lower = desc["desc"].lower()
                # Match multi-word or single-word values, handling comma or 'and' separators
                pattern = rf"(?:{re.escape(desc_lower)}|{re.escape(col_lower)})\s*(?:is\s+|from\s+|with\s+|of\s+)?([a-zA-Z0-9\s\-'.]+?)(?=\s*(?:,|and\s|with\s|whose\s|from\s|between\s|$))"
                matches = re.findall(pattern, prompt)
                for match in matches:
                    # Split on commas or 'and' to capture multiple values
                    values = re.split(r'\s*,\s*|\s+and\s+', match.strip())
                    for value in values:
                        value = value.strip()
                        if value.lower() in ignore_values or any(phrase in value for phrase in quoted_phrases):
                            continue
                        if value:
                            column_values.setdefault(col.upper(), []).append(value)
                            logger.debug(f"Regex parsed direct filter value: {col.upper()} = '{value}'")

        # Build filters from column_values
        for col, values in column_values.items():
            # Remove duplicates while preserving order
            unique_values = list(dict.fromkeys(values))
            if len(unique_values) > 1:
                # Use IN clause for multiple values
                formatted_values = [f"'{v}'" if not v.replace('.', '').isdigit() else v for v in unique_values]
                filters.append(f"{col} IN ({', '.join(formatted_values)})")
                logger.debug(f"Generated IN clause: {col} IN ({', '.join(formatted_values)})")
            elif unique_values:
                # Use = for single value
                value = unique_values[0]
                value = f"'{value}'" if not value.replace('.', '').isdigit() else value
                filters.append(f"{col} = {value}")
                logger.debug(f"Generated single value filter: {col} = {value}")

        logger.info(f"Extracted direct filters: {filters}")
        return filters

    except Exception as e:
        logger.error(f"Error extracting direct filters: {e}")
        return []