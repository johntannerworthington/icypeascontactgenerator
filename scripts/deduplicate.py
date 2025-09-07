import re

# Deduplicate LinkedIn URLs and keep track of all the job titles that returned that URL
# Log which queries were removed due to duplication
# Input: {<query1>: {url: <found linkedin url or blank>, company: <company_name>, title: <job_title>}...
#         <queryN>: {url: <found linkedin url or blank>, company: <company_name>, title: <job_title>}...}
#       QueryTracker: tracker object
# Return: Dictionary, with {"[query]": { url: <url>, job_titles: [...], company: <company> }}
def deduplicate_linkedin_urls(url_dict, tracker):
    # Define the regex pattern to match a valid LinkedIn profile URL
    linkedin_profile_pattern = re.compile(r'linkedin\.com/in/')
    
    # Dictionary to track seen URLs and their associated queries, job titles, and company
    seen_urls = {}
    
    # Iterate through the dictionary to identify duplicates and collect job titles
    for query, data in url_dict.items():
        try:
            url = data.get('url')
            job_title = data.get('title')
            company = data.get('company')
            domain = data.get('domain')
            if url:
                if linkedin_profile_pattern.search(url):
                    if url in seen_urls:
                        seen_urls[url]['job_titles'].append(job_title)
                        tracker.log(query, f"Duplicate URL found: {url}")
                    else:
                        seen_urls[url] = {
                            'url': url,
                            'job_titles': [job_title],
                            'query': query,
                            'company': company,
                            'domain': domain
                        }
        except Exception as e:
            tracker.log(query, f"Deduplication error: {e}")

    # Format the result as {"[query]": {url: <url>, job_titles: [...], company: <company>, domain: <domain>}}
    result = {
        entry['query']: {
            'url': entry['url'],
            'job_titles': entry['job_titles'],
            'company': entry['company'],
            'domain': entry['domain']
        } for entry in seen_urls.values()
    }
    
    return result