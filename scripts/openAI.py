import requests
from .creds import API_KEYS
import json

# openAI company name fuzzy match
# Input:: String: Target Company, String: Found Company Name
# Return:: Boolean
def fuzzy_match_company(target_company, current_company_name):
 
    API_KEY = API_KEYS["OPENAI_API_KEY"]

    # The prompt to send to the openAI API.
    prompt = f"""You are a highly accurate system for matching company names. Your task is to determine if a candidate company name refers to the same entity as a target company name.

        Target Company: "{target_company}"
        Candidate Company: "{current_company_name}"

        **Instructions:**
        Focus on the core identity of the company, ignoring legal suffixes, punctuation, and common abbreviations. Consider parent companies and known acronyms.

        **Examples of Matches:**
        * "L3Harris Technologies" vs "L3Harris"
        * "Google LLC" vs "Google"
        * "The Coca-Cola Company" vs "Coca-Cola"
        * "International Business Machines" vs "IBM"
        * "General Electric" vs "GE"
        * "The Boeing Company" vs "Boeing"
        * "JPMorgan Chase & Co." vs "JPMorgan Chase"
        * "Amazon Web Services" vs "Amazon"
        * "Unilever N.V." vs "Unilever"
        * "Apple Inc." vs "Apple"
        * "Microsoft Corporation" vs "Microsoft"
        * "Google LLC" vs "Google"
        * "Amazon.com" vs "Amazon"
        
        Respond with ONLY a JSON object:
        json
        {{"match": true/false}}
    """
    # Make the API call.
    result, usage = openai_request(API_KEY, prompt, model="gpt-4.1-nano")

    if not result:
        return False, usage

    try:
        parsed = json.loads(result)
        return parsed.get("match", False), usage
    except (json.JSONDecodeError, TypeError):
        print("Error parsing OpenAI response:", result)
        return False, usage

# openAI job title fuzzy match
# Input:: List[String]: Target Job Titles, String: Found Job Title
# Return:: Boolean
def fuzzy_match_job_title(target_job_titles, current_job_title):
    API_KEY = API_KEYS["OPENAI_API_KEY"]  # Make sure this dict exists

    prompt = f"""
        You are comparing a candidate's current job title to a list of target job titles to determine if they represent the same job function or a closely related role.

        Target Job Titles: {target_job_titles}
        Candidate Job Title: "{current_job_title}"

        **Core Principle:** Focus on the job's **function and field**, not on literal keywords. Roles in the same domain, even with different seniority or exact titles, should be considered a match.

        **Match Examples:**
        * **Executive Leadership:** "CEO", "Founder", "President", "Managing Member", "Chief Executive Officer"
        * **Sales & Marketing:** "VP of Sales", "Sales Director", "Head of Marketing", "Marketing Lead"
        * **Product/Design/Development:** "Product Manager", "Product Owner", "Product Strategist", "Product Development Engineer", "Product Designer", "Innovation Manager", "Design Engineer"
        * **Packaging:** "Packaging Engineer", "Structural Packaging Engineer", "Packaging Specialist", "Packaging Design Engineer"
        * **Software:** "Software Engineer", "Developer", "Programmer", "Full Stack Developer", "Backend Engineer"

        **Key Rules:**
        1.  **Seniority:** A "Senior" role can match a "Lead" or "Director" role (e.g., "Senior Design Engineer" to "Director of Product Development").
        2.  **Domain:** The terms "Design," "Development," and "Product" are often used interchangeably for similar functions (e.g., a "Design Engineer" may work in "Product Development").
        3.  **Multiple Roles:** If the job title contains multiple roles (e.g., "President / Mechanical Engineer"), evaluate each part separately. A match on any single part is a full match.

        Respond with ONLY a JSON object:
        json
        {{"match": true/false}}
    """

    # Make the API Call
    result, usage = openai_request(API_KEY, prompt, model="gpt-4.1-nano")

    if not result:
        return False, usage

    try:
        parsed = json.loads(result)
        return parsed.get("match", False), usage
    except (json.JSONDecodeError, TypeError):
        print("Error parsing OpenAI response:", result)
        return False, usage


def openai_request(api_key, prompt, model):
    api_url = "https://api.openai.com/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    data = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0,
        "stream": False  # Non-streaming so we get token usage
    }

    response = requests.post(api_url, headers=headers, json=data)
    if response.status_code != 200:
        print(f"OpenAI API request failed: {response.status_code} - {response.text}")
        return None, None

    response_json = response.json()

    # Extract the text response
    content = response_json["choices"][0]["message"]["content"].strip()
    # Extract token usage (may be None if the API doesn't return it)
    usage = response_json.get("usage").get("total_tokens", 0)

    return content, usage