


import json
from difflib import SequenceMatcher
import os
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

schema_path = "schema_metadata.json"
business_terms_path = "business_mapping.json"

# Validate schema file
if not os.path.exists(schema_path):
    logger.error(f"Schema file not found: {schema_path}")
    schema_metadata = {}
else:
    try:
        with open(schema_path, "r") as f:
            schema_content = f.read().strip()
            if not schema_content:
                logger.error("schema_metadata.json is empty")
                schema_metadata = {}
            else:
                schema_metadata = json.loads(schema_content)
    except Exception as e:
        logger.error(f"Error loading schema_metadata.json: {e}")
        schema_metadata = {}

# Validate business terms file
if not os.path.exists(business_terms_path):
    logger.error(f"Business mapping file not found: {business_terms_path}")
    business_terms = {}
else:
    try:
        with open(business_terms_path, "r") as f:
            business_content = f.read().strip()
            if not business_content:
                logger.error("business_mapping.json is empty")
                business_terms = {}
            else:
                business_terms = json.loads(business_content)
    except Exception as e:
        logger.error(f"Error loading business_mapping.json: {e}")
        business_terms = {}

# Generic terms to penalize if used alone
GENERIC_TERMS = {"request", "record", "entry", "data"}

def similarity(a, b):
    """Calculate similarity between two strings."""
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()

def retrieve_relevant_table(prompt: str, memory_context=None):
    """Retrieve the most relevant table based on prompt and context."""
    from followup_handler import is_follow_up_prompt
    prompt_lower = prompt.lower()
    prompt_norm = prompt_lower
    logger.debug(f"Processing prompt for table retrieval: {prompt_lower}")

    # Skip table retrieval for follow-up prompts
    if is_follow_up_prompt(prompt) and memory_context:
        logger.debug("Follow-up prompt detected, skipping table retrieval")
        return {}

    table_scores = {}

    # Business Term Override (high confidence)
    for keyword, mapping in business_terms.items():
        if keyword in prompt_lower:
            matched_table = mapping["table"].upper()
            if matched_table in schema_metadata:
                logger.debug(f"Matched table {matched_table} via business term: {keyword}")
                return {matched_table: schema_metadata[matched_table]}
            else:
                logger.warning(f"Business term table {matched_table} not in schema_metadata")

    # Compute table scores
    for table, meta in schema_metadata.items():
        score = 0
        table_desc = meta.get("description", "").lower()

        # Boost for matching table name or description
        score += max(
            similarity(prompt_norm, table.lower()),
            similarity(prompt_norm, table_desc)
        ) * 10

        # Boost for matching column names/descriptions
        for col, desc_obj in meta.get("columns", {}).items():
            col_lower = col.lower()
            if isinstance(desc_obj, dict):
                desc_lower = desc_obj.get("desc", "").lower()
            else:
                desc_lower = str(desc_obj).lower()
            if col_lower in prompt_lower or desc_lower in prompt_lower:
                score += 5
            else:
                score += similarity(prompt_lower, col_lower) + similarity(prompt_lower, desc_lower)

        # Penalize generic terms if no strong keyword match
        if any(generic in prompt_lower for generic in GENERIC_TERMS) and score < 10:
            score -= 5
        table_scores[table] = score
        logger.debug(f"Table {table} score: {score}")

    # Return highest scored table
    if table_scores:
        best_table = max(table_scores.items(), key=lambda x: x[1])[0]
        if table_scores[best_table] > 5:
            logger.debug(f"Selected table: {best_table} with score {table_scores[best_table]}")
            return {best_table: schema_metadata[best_table]}
        else:
            logger.warning(f"No table matched with sufficient score for prompt: {prompt}")
            return {}
    logger.warning(f"No table matched for prompt: {prompt}")
    return {}