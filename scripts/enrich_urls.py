import aiohttp
import asyncio
import json
import math
from .creds import API_KEYS

# Parallel Process Enrich LinkedIn URLs
# Use bulk search function to do a request to ICYPEAS API and get data and match the query to the response
# icypeas API limit is 20 requests per second (50 Profiles per request)
# Input: Dictionary of URLs in the following format
# {
#     "query1": {"url": "https://www.linkedin.com/in/profile1", "job_titles": [...], "company": <company>, "domain": <domain>},
#     "query2": {"url": "https://www.linkedin.com/in/profile2", "job_titles": [...], "company": <company>, "domain": <domain>},
#     ...
# }
# Output: Dictionary of enriched profiles in the following format
# {
#     "query1": {"URL": "https://www.linkedin.com/in/profile1", "job_titles": [...], "company": <company>, "domain": <domain>, "icypeas_response": {URL: <URL>, status: <status>, firstname: <firstname>, lastname: <lastname>, worksFor: <worksFor>}},
#     "query2": {"URL": "https://www.linkedin.com/in/profile2", "job_titles": [...], "company": <company>, "domain": <domain>, "icypeas_response": {URL: <URL>, status: <status>, firstname: <firstname>, lastname: <lastname>, worksFor: <worksFor>}},
#     ...
# }
# Use async to do multiple requests without going over the 20 requests/sec limit
# Create batches of the input of linkedin_urls with a maximum of 50 but make it modular for testing purposes
async def enrich_urls(linkedin_urls, tracker, logger):
    enriched_profiles = {}
    BATCH_SIZE = 50
    MAX_REQUESTS_PER_SECOND = 20  # Adjustable, matches ICYPEAS API limit

    # Create semaphore to limit concurrent requests
    semaphore = asyncio.Semaphore(MAX_REQUESTS_PER_SECOND)
    
    # Calculate the number of batches needed
    num_profiles = len(linkedin_urls)
    num_batches = math.ceil(num_profiles / BATCH_SIZE)
    batches = []
    keys = list(linkedin_urls.keys())

    # Create batches of URLs
    for i in range(num_batches):
        start_index = i * BATCH_SIZE
        end_index = min(start_index + BATCH_SIZE, num_profiles)
        batch_keys = keys[start_index:end_index]
        batch = {k: linkedin_urls[k] for k in batch_keys}
        batches.append(batch)

    # Process batches asynchronously
    async def process_batch(batch, batch_idx):
        async with semaphore:
            try:
                result = await bulk_search(batch, tracker, logger)
                return result
            except Exception as e:
                tracker.log("bulk_search_error", f"Batch {batch_idx} generated an exception: {e}")
                return {}

    tasks = []
    for batch_idx, batch in enumerate(batches):
        tasks.append(asyncio.create_task(process_batch(batch, batch_idx)))

    # Collect results with rate limiting
    start_time = asyncio.get_event_loop().time()
    delay_per_request = 1.0 / MAX_REQUESTS_PER_SECOND
    for idx, future in enumerate(await asyncio.gather(*tasks, return_exceptions=True)):
        if isinstance(future, dict):
            enriched_profiles.update(future)
        else:
            tracker.log("bulk_search_error", f"Batch {idx} generated an exception: {future}")

        # Rate limiting logic
        time_taken = asyncio.get_event_loop().time() - start_time
        if time_taken < delay_per_request and idx < len(tasks) - 1:
            await asyncio.sleep(delay_per_request - time_taken)
        start_time = asyncio.get_event_loop().time()

    return enriched_profiles

# Bulk search profiles using Icypeas API
# Maximum of 50 profiles per request
# Returns a dictionary with the following format:
# The return order of profiles is the same as the order of the input URLs
async def bulk_search(input_data, tracker, logger):
    bulk_url = "https://app.icypeas.com/api/scrape"
    API_key = API_KEYS["ICYPEAS_API_KEY"]

    body = {
        "type": "profile",
        "data": [v['url'] for v in input_data.values()]
    }

    headers = {
        "Content-Type": "application/json",
        "Authorization": API_key
    }
    result = {}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(bulk_url, json=body, headers=headers) as response:
                response.raise_for_status()
                data = await response.json()
                if not data.get("success", False):
                    for query in input_data.keys():
                        tracker.log(query, f"Bulk search API returned unsuccessful: {data}")
                    return result

                # Use helper function to make sure each query matches the correspondent profile
                for query, input_item, profile_data in zip(input_data.keys(), input_data.values(), data.get("data")):
                    result_data = profile_data.get("result")
                    if profile_data.get("status") == "FOUND":
                        logger.add_icypeas(1)
                        first_name = result_data.get("firstname")
                        last_name = result_data.get("lastname")
                        if first_name and last_name:
                            result_data = profile_data.get("result")
                            worksFor = result_data.get("worksFor")
                            result[query] = {
                                "URL": input_item['url'],
                                "job_titles": input_item['job_titles'],
                                "company": input_item['company'],
                                "domain": input_item['domain'],
                                "icypeas_response": {
                                    "URL": result_data.get("url"),
                                    "status": profile_data.get("status"),
                                    "firstname": first_name,
                                    "lastname": last_name,
                                    "worksFor": worksFor
                                }
                            }
                        else:
                            tracker.log(query, f"Profile not found in ICYPEAS: {input_item['url']}")
                    else:
                        tracker.log(query, f"Profile not found in ICYPEAS: {input_item['url']}")
                
                return result
    except aiohttp.ClientResponseError as e:
        tracker.log(list(input_data.keys())[0], f"Bulk search request error: {e}")
        return {"success": False, "data": []}
    except aiohttp.ClientError as e:
        tracker.log(list(input_data.keys())[0], f"Bulk search request error: {e}")
        return {"success": False, "data": []}
    except json.JSONDecodeError as e:
        tracker.log(list(input_data.keys())[0], f"Bulk search JSON decode error: {e}")
        return {"success": False, "data": []}

# Wrapper to run the async function synchronously
def enrich_urls_sync(linkedin_urls, tracker, logger):
    return asyncio.run(enrich_urls(linkedin_urls, tracker, logger))