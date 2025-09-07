import aiohttp
import asyncio
import json
from .creds import API_KEYS

# Split up each profile and call findmymail_request asynchronously
# Findmymail API limit = 300 requests concurrently
'''
input example: "ehx.com Marketing Director site:linkedin.com/in": {
    "query": "ehx.com Marketing Director site:linkedin.com/in",
    "URL": "https://www.linkedin.com/in/mike-castellano",
    "domain": "ehx.com",
    "validation_result": {
      "valid": True,
      "firstname": "Michael",
      "lastname": "Castellano",
      "currentCompany": "Electro-Harmonix",
      "currentJobTitle": "Vice President of Marketing",
      "findmymail": ""
    },
    "openAI_usage": "711"
  },
  ...
output example:
"ehx.com Marketing Director site:linkedin.com/in": {
    "query": "ehx.com Marketing Director site:linkedin.com/in",
    "URL": "https://www.linkedin.com/in/mike-castellano",
    "domain": "ehx.com",
    "validation_result": {
      "valid": True,
      "firstname": "Michael",
      "lastname": "Castellano",
      "currentCompany": "Electro-Harmonix",
      "currentJobTitle": "Vice President of Marketing",
      "findmymail": <fill in if found/ /

leave blank if not found>
    },
    "openAI_usage": "711"
  },
  ...
'''
async def findymail(profiles, tracker, logger):
    # Create a copy of input profiles to avoid modifying original
    result = profiles.copy()
    MAX_CONCURRENT_REQUESTS = 300  # Matches FindMyMail API limit
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    async def process_profile(query, profile):
        # Extract necessary information from profile
        validation_result = profile.get("validation_result", {})
        firstname = validation_result.get("firstname")
        lastname = validation_result.get("lastname")
        domain = profile.get("domain")

        # Make FindMyMail request
        async with semaphore:
            email = await findmymail_request(query, firstname, lastname, domain, tracker, logger)
        # Update profile with email result
        updated_profile = profile.copy()
        updated_profile["validation_result"]["findmymail"] = email if email else ""
        return query, updated_profile

    # Process profiles concurrently
    tasks = [
        asyncio.create_task(process_profile(query, profile))
        for query, profile in profiles.items()
    ]

    # Collect results
    for future in await asyncio.gather(*tasks, return_exceptions=True):
        if isinstance(future, tuple):
            query, updated_profile = future
            result[query] = updated_profile
        else:
            query = next(iter(profiles))  # Fallback to first query if error
            tracker.log(query, f"Error processing profile: {str(future)}")

    return result

async def findmymail_request(query, firstName, lastName, domain, tracker, logger):
    """
    Make a single FindMyMail API request.
    
    Args:
        query (str): Query string
        firstName (str): First name
        lastName (str): Last name
        domain (str): Domain to search
        tracker: Tracker object for logging
        logger: Logger object for tracking credits and results
        
    Returns:
        str or None: Found email or None if not found
    """
    FINDMYMAIL_API_KEY = API_KEYS["FINDMYMAIL_API_KEY"]
    findmymail_url = "https://app.findymail.com/api/search/name"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {FINDMYMAIL_API_KEY}"
    }
    name = f"{firstName} {lastName}"
    payload = {
        "name": name,
        "domain": domain,
        "webhook_url": None
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(findmymail_url, headers=headers, json=payload) as response:
                if response.status == 200:
                    response_data = await response.json()
                    contact_data = response_data.get('contact', {})
                    email = contact_data.get('email')
                    if email:
                        tracker.log(query, f"Email found for {firstName} {lastName}")
                        logger.add_findmymail_credit(1)
                        logger.add_found_email(1)
                    else:
                        tracker.log(query, f"Profile validated but no email found")
                    return email
                else:
                    tracker.log(query, f"FindMyMail API request failed with status code: {response.status}")
                    return None
    except aiohttp.ClientError as e:
        tracker.log(query, f"FindMyMail API request failed: {str(e)}")
        return None

# Wrapper to run the async function synchronously
def findymail_sync(profiles, tracker, logger):
    return asyncio.run(findymail(profiles, tracker, logger))