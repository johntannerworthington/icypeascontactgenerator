import aiohttp
import asyncio
import re
import json
from aiolimiter import AsyncLimiter
from .creds import API_KEYS

# Use Serper API to get a single LinkedIn profile URL
async def serper_request(data, tracker, logger, semaphore, session, rate_limiter):
    company = data[1]["company"]
    title = data[1]["title"]
    domain = data[1]["domain"]
    serper_api_url = "https://google.serper.dev/search"
    serper_api_key = API_KEYS["SERPER_API_KEY"]
    headers = {
        'X-API-KEY': serper_api_key,
        'Content-Type': 'application/json'
    }
    
    try:
        payload = json.dumps([{"q": data[0]}])
        async with semaphore:
            async with rate_limiter:  # Apply rate limiting here
                async with session.post(serper_api_url, headers=headers, data=payload) as response:
                    response_data = await response.json()
                    organic_results = response_data[0].get('organic', [])
                    if response.status == 200:
                        logger.add_serper(1)
                        if organic_results:
                            link = organic_results[0].get('link', '')
                            if link and bool(re.search(r'linkedin\.com/in/', link)):
                                logger.add_urls_found(1)
                                return {data[0]: {"url": link, "company": company, "title": title, "domain": domain}}
                            tracker.log(data[0], f"No valid LinkedIn URL found. Serper link: {link}")
                            print(f"No valid LinkedIn URL found for query '{data[0]}'. Link: {link}")
                        tracker.log(data[0], f"Empty result from Serper API: {response_data}")
                        return {data[0]: {"url": "", "company": company, "title": title, "domain": domain}}
                    else:
                        tracker.log(data[0], f"Failed API request: {response.status}")
                        return {data[0]: {"url": "", "company": company, "title": title, "domain": domain}}
    except aiohttp.ClientError as e:
        tracker.log(data[0], f"Error during API call: {e}")
        print(f"Error during API call: {e}")
        return {data[0]: {"url": "", "company": company, "title": title, "domain": domain}}
    except KeyError as e:
        tracker.log(data[0], f"Missing API key: {e}")
        print(f"Missing API key: {e}")
        return {data[0]: {"url": "", "company": company, "title": title, "domain": domain}}
    except json.JSONDecodeError as e:
        tracker.log(data[0], f"Error parsing JSON response: {e}")
        print(f"Error parsing JSON response: {e}")
        return {data[0]: {"url": "", "company": company, "title": title, "domain": domain}}

# Find LinkedIn URLs in a dictionary of queries using serper_request function 
async def get_linkedin_urls(query_dict, tracker, logger):
    MAX_CONCURRENT_REQUESTS = 220
    MAX_REQUESTS_PER_SECOND = 220  # Target rate (Serper API limit)
    linkedin_urls = {}
    
    # Create semaphore to limit concurrent requests
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS)
    
    # Create rate limiter: 300 requests per second
    rate_limiter = AsyncLimiter(MAX_REQUESTS_PER_SECOND, 1)  # 300 requests per 1 second

    queries = list(query_dict.items())
    if not queries:
        return linkedin_urls
    
    async with aiohttp.ClientSession(connector=connector) as session: 
        # Create all tasks immediately without rate limiting
        tasks = [
            asyncio.create_task(serper_request(query, tracker, logger, semaphore, session, rate_limiter))
            for query in queries
        ]
        
        # Collect results as they complete
        for completed_task in asyncio.as_completed(tasks):
            try:
                result = await completed_task
                if isinstance(result, dict):
                    linkedin_urls.update(result)
            except Exception as e:
                query_key = queries[tasks.index(completed_task)][0]
                tracker.log(query_key, f"Task exception: {e}")
                print(f"Task exception for query '{query_key}': {e}")
                linkedin_urls[query_key] = {
                    "url": "", 
                    "company": queries[tasks.index(completed_task)][1]["company"], 
                    "title": queries[tasks.index(completed_task)][1]["title"],
                    "domain": queries[tasks.index(completed_task)][1]["domain"]
                }
    
    return linkedin_urls

# Wrapper to run the async function
def get_linkedin_urls_sync(query_dict, tracker, logger):
    return asyncio.run(get_linkedin_urls(query_dict, tracker, logger))