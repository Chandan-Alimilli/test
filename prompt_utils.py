

import re

def extract_limit_from_prompt(prompt, default_limit=50, max_limit=500):
    prompt = prompt.lower()

    # ✅ Match only when limit context is clearly mentioned
    match = re.search(r"(?:limit(?:\s+of)?|return|fetch|show)\s*(\d{1,4})\s*(?:records|rows)?", prompt)
    
    if match:
        try:
            val = int(match.group(1))
            return min(val, max_limit)
        except:
            return default_limit

    return default_limit
