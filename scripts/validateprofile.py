import concurrent.futures
import json
import re
from .openAI import fuzzy_match_company, fuzzy_match_job_title

# Fuzzy match company parameters: target_company, current_company_name
# Output: result (boolean), usage
# Fuzzy match job title: target_job_titles list, current_job_title
# Output: result (boolean), usage

'''
This function will take in a JSON of cleaned enrichments, target company, tracker
and validate each profile against the target company and job titles.

Input_format = {
    "query1": {
        "URL": "https://www.linkedin.com/in/profile1",
        "job_titles": ["[some title]"],  # Note: this is a list of strings
        "company": "[some company]", # Target company
        "icypeas_response": {
            "URL": "<url>",
            "status": "<FOUND/NOT_FOUND>",
            "firstname": "Phil",
            "lastname": "Eichmiller",
            "worksFor": [
                # So in here there could be one, two, etc or none
                {
                    "name": "Company A",
                    "endDate": "[some time]" # '0001-01-01T00:00:00.000Z' or '0000-01-01T00:00:00.000Z' for current job
                    "jobTitle": "[some title]"
                },
            ]
        }
    },
}

Sequential Steps for validation: (They have to be followed in order)

Works for:
Check if "worksFor" field is not empty
then if it is not empty
Go through and check each entry in "worksFor" and get the first one that 
matches the endDate criteria that means it is their current job
if no current job found log using tracker function: log(query, exit reason) with reason "No current job found"
then exit validation
then set current_job into a variable

Company:
Check if "name" field in "worksFor" fuzzy matches the target_company
if no match found log using tracker function: log(query, exit reason) 
with reason "Current Company does not Fuzzy Match target company: {current_company} : {target_company}"
then exit validation
then set current_company into a variable

Job Title:
Check if "jobTitle" field in "worksFor" fuzzy matches any of the target_job_titles
if no match found log using tracker function: log(query, exit reason) 
with reason "Current Job Title does not Fuzzy Match target job titles: {current_job_title} : {target_job_titles}"
then exit validation
then set current_job_title into a variable

Fuzzy Match:
Fuzzy match is done with an openAI request

Output:
result = {
    {
        "query: "[query]",
        "URL": "https://www.linkedin.com/in/profile1",
        "validation_result": {
            "valid": true,
            "firstname": "[some first name]",
            "lastName": "[some last name]",
            "currentCompany": "[some current company]",
            "currentJobTitle": "[some current job title]"
        },
        "openAI_usage": "[total_tokens]"
    }
}

Make sure every query gets logged accordingly
Thread parallelization to do multiple profile validations at a time using helper function
OpenAI limit = 30,0000 queries per minute so shouldn't have an issue

pass into validate_profile helper function one profile in the following format
"query1": {
    "URL": "https://www.linkedin.com/in/profile1",
    "icypeas_response": {
        "firstname": "Phil",
        "lastname": "Eichmiller",
        "worksFor": [
                {
                    "name": "Company A",
                    "endDate": "0001-01-01T00:00:00.000Z",
                    "jobTitle": "Software Engineer"
                }
            ]
        }
    }
}
'''
def validate_profiles(cleaned_enrichments, tracker, logger):
    """
    Processes multiple profiles in parallel using a ThreadPoolExecutor.
    
    Args:
        cleaned_enrichments (dict): The dictionary containing all profiles to be validated.

    Returns:
        dict: A dictionary containing the validation results for each profile.
    """
    results = {}
    
    # Use ThreadPoolExecutor for thread-based parallelization
    workers = 70
    if workers == 0:
        workers = 1
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:

        # Dictionary to hold the futures, mapping each future to its query name
        future_to_query = {
            executor.submit(validate_profile, query_name, data, tracker, logger): query_name
            for query_name, data in cleaned_enrichments.items()
        }
        
        # Iterate over completed futures as they become available
        for future in concurrent.futures.as_completed(future_to_query):
            try:
                # Retrieve the result from the completed future
                result = future.result()
                if result:
                    results.update(result)
            except Exception as exc:
                query_name = future_to_query[future]
                print(f"{query_name} generated an exception: {exc}")
                # Log the error for the specific query
                tracker.log(query_name, f"Validation failed due to exception: {exc}")
    
    return results


# This is a helper function that will process one profile in the following format:
'''
"query1": {
    "URL": "https://www.linkedin.com/in/profile1",
    "icypeas_response": {
        "firstname": "Phil",
        "lastname": "Eichmiller",
        "worksFor": [
                {
                    "name": "Company A",
                    "endDate": "0001-01-01T00:00:00.000Z",
                    "jobTitle": "Software Engineer"
                }
            ]
        }
    }
}
'''
def validate_profile(query, data, tracker, logger):
    """
    Validates a single profile against a target company and job titles.
    
    Args:
        query (str): The original query string.
        data (dict): The profile data to validate.
        tracker (object): A tracker object for logging.
        logger (object): A logger object for tracking OpenAI usage.

    Returns:
        dict: The validation result for the profile.
    """
    target_company = data.get("company")
    job_titles = data.get("job_titles", [])
    
    # Check if Icypeas result is present and has worksFor data
    icypeas_result = data.get("icypeas_response")
    if not icypeas_result:
        tracker.log(query, f"No Icypeas result found: {data.get('URL')}")
        return {}

    worksFor = icypeas_result.get("worksFor")
    if not worksFor:
        tracker.log(query, f"Works for data from Icypeas result is empty {data.get('URL')}")
        return {}
    
    # Sequential Step 1: Find the current job
    current_work_entry = current_work(worksFor)
    if not current_work_entry:
        tracker.log(query, "No current job found")
        return {}

    current_company_name = current_work_entry.get("name")
    current_job_title = current_work_entry.get("jobTitle")

    # Sequential Step 2: Fuzzy match company
    company_usage = 0
    try:
        company_match, usage = fuzzy_match_company(target_company, current_company_name)
        company_usage += usage
    except Exception as e:
        tracker.log(query, f"Error in company fuzzy match: {str(e)}")
        return {}

    if not company_match:
        tracker.log(query, f"Current Company does not Fuzzy Match target company: {current_company_name} : {target_company} url: {data.get('URL')}")
        return {}

    # Sequential Step 3: Fuzzy match job title
    job_title_usage = 0
    try:
        job_title_match, usage = fuzzy_match_job_title(job_titles, current_job_title)
        job_title_usage += usage
    except Exception as e:
        tracker.log(query, f"Error in job title fuzzy match: {str(e)}")
        return {}

    if not job_title_match:
        tracker.log(query, f"Current Job Title does not Fuzzy Match target job titles: {current_job_title} : {job_titles} url: {data.get('URL')}")
        return {}

    # Log OpenAI usage and return validated profile
    logger.add_openai(int(company_usage) + int(job_title_usage))
    
    return {
        query: {
            "query": query,
            "URL": data.get("URL"),
            "domain": data.get("domain"),
            "validation_result": {
                "valid": True,
                "firstname": icypeas_result.get("firstname"),
                "lastname": icypeas_result.get("lastname"),
                "currentCompany": current_company_name,
                "currentJobTitle": current_job_title,
                "findmymail": ''
            },
            "openAI_usage": str(int(company_usage) + int(job_title_usage))
        }
    }

def current_work(worksFor):
    """
    Finds the current job from a list of 'worksFor' entries.
    
    Args:
        worksFor (list): A list of dictionaries representing past and current jobs.

    Returns:
        dict or None: The dictionary for the current job, or None if not found.
    """
    for work in worksFor:
        end_date = work.get("endDate")
        if end_date in ["0001-01-01T00:00:00.000Z", "0000-01-01T00:00:00.000Z"]:
            return work
    return None