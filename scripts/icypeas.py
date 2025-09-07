import requests
from .creds import API_KEYS

# Icypeas API Request to find necessary information
# Input: query, profile_url, tracker
# Return: Tuple (query, response_data) or (query, None) on failure
def icypeas_request(query, profile_url, tracker):
    alt_icypeas_url = f"https://app.icypeas.com/api/scrape/profile?url={profile_url}"

    headers = {
        "Content-Type": "application/json",
        "Authorization": API_KEYS["ICYPEAS_API_KEY"]
    }
    
    try:
        response = requests.get(alt_icypeas_url, headers=headers)
        response.raise_for_status()
        response_data = response.json()
        return (query, response_data)
    except requests.exceptions.RequestException as e:
        print(f"Error during API call for {query} ({profile_url}): {e}")
        tracker.log(query, f"Icypeas API Error: Request failed {e}")
        return (query, None)
    except Exception as e:
        print(f"Unexpected error for {query} ({profile_url}): {e}")
        tracker.log(query, f"Icypeas API Error: Unexpected error {e}")
        return (query, None)

# Enrich profile data using Icypeas API
# Input: query (str), url (str), tracker (QueryTracker)
# Output: Dict {query: {"URL": url, "Icypeas result": response_data}}
def enrich_profile(query, url, tracker, logger):
    results = {}
    # Process the single query
    result_query, result_data = icypeas_request(query, url, tracker)
    results[query] = {
        "URL": url,
        "Icypeas result": result_data
    }
    return results