from typing import final
import polars as pl
import datetime as dt
import pandas as pd
import time
import requests
import json
import re
from scripts.query_tracker import QueryTracker as QueryTracker
from scripts.queries import gen_queries
from scripts.serper import get_linkedin_urls_sync
from scripts.deduplicate import deduplicate_linkedin_urls as deduplicate_linkedin_urls
from scripts.enrich_urls import enrich_urls_sync
from scripts.validateprofile import validate_profiles as validate_profiles
from scripts.findymail import findymail_sync
from scripts.Logger import Logger as Logger
from scripts.creds import API_KEYS as API_KEYS
import itertools

def main():
    all_start = time.time()
    filepath = "input.csv"
    input_df = pl.read_csv(filepath)
    logger = Logger()
    query_tracker = QueryTracker()

    print(f"processing {len(input_df)} companies")
    logger.total_input_companies = len(input_df)

    # Extract job titles and turn it into a list
    str_job_titles = input_df["job titles"][0]
    job_titles = [title.strip() for title in str_job_titles.split(",")] 
    companies_data = [
        {"company": row["company"], "Root Domain": row["Root Domain"]}
        for row in input_df.iter_rows(named=True)
    ]
    # Generate Queries
    queries = gen_queries(companies_data, job_titles, logger)
    logger.add_queries(len(queries))
    print(f"Generated {len(queries)} queries")

    # Debugging print
    # pretty_queries = json.dumps(queries, indent=2)
    # print(pretty_queries)
    print("Serper starting...")
    serper_start = time.time()
    # Serper Request API Limit 300 / s
    urls = get_linkedin_urls_sync(queries, query_tracker, logger)
    print(len(urls))
    print(f"Serper runtime: {time.time() - serper_start:.2f} seconds")
    print(f"QPS: {len(queries)/(time.time() - serper_start):.2f}")

    # Debugging print
    # pretty_urls = json.dumps(urls, indent=2)
    # print(pretty_urls)
    
    # Deduplicate
    print("Deduplicating URLs...")
    deduplicated_urls = deduplicate_linkedin_urls(urls, query_tracker)
    logger.add_deduplicated(len(deduplicated_urls))

    # Debugging print
    # pretty_deduplicated = json.dumps(deduplicated_urls, indent=2)
    # print(pretty_deduplicated)
    # print(f"Deduplicated: {type(deduplicated_urls)}")
    
    # Icypeas
    print("Enriching URLs with Icypeas...")
    icy_start = time.time()
    icypeas_profiles = enrich_urls_sync(deduplicated_urls, query_tracker, logger)
    print(f"Icy runtime: {time.time() - icy_start:.2f} seconds")

    # Debugging print
    # pretty_enrich = json.dumps(icypeas_profiles, indent=2)
    # print(pretty_enrich)    

    # Validate
    print("Validating profiles...")
    validate_start = time.time()
    validated_profiles = validate_profiles(icypeas_profiles, query_tracker, logger)
    logger.add_matches(len(validated_profiles))
    print(f"Validation runtime: {time.time() - validate_start:.2f} seconds")

    # Debugging print
    # pretty_print = json.dumps(validated_profiles, indent=2)
    # print(pretty_print)
    
    # findymail_start_response = requests.get("https://app.findymail.com/api/credits",
    # headers={
    #   "Authorization": f"Bearer {API_KEYS['FINDMYMAIL_API_KEY']}"
    # })
    # start_data = findymail_start_response.json()
    # start_credits = start_data.get("credits")

    # Findymail
    print("Finding emails with Findymail...")
    findymail_start = time.time()
    emails = findymail_sync(validated_profiles, query_tracker, logger)
    print(f"Findymail runtime: {time.time() - findymail_start:.2f} seconds")

    # findymail_end_response = requests.get("https://app.findymail.com/api/credits",
    # headers={
    #   "Authorization": f"Bearer {API_KEYS['FINDMYMAIL_API_KEY']}"
    # })
    # end_data = findymail_end_response.json()
    # end_credits = end_data.get("credits")
    
    # Debugging print
    # pretty_emails = json.dumps(emails, indent=2)
    # print(pretty_emails)

    # Done
    final_output_df = output_results(emails, input_df)
    final_output_df.write_csv("output.csv")
    
    logger.output()
    
    
    print(f"Total runtime: {time.time() - all_start:.2f} seconds")

# Function to output results into csv file
# Needs to add all the previous columns from raw df input
# and then add the following at the end
# Full Name, First Name, Job title (current job title), linkedIn URL, Company, search_query, email (findmymail)
'''
Input example:
profiles = {
    "Electro-Harmonix New Product Development Specialist site:linkedin.com/in": {
        "query": "Electro-Harmonix New Product Development Specialist site:linkedin.com/in",
        "URL": "https://www.linkedin.com/in/mannysilverstein",
        "domain": "ehx.com",
        "validation_result": {
            "valid": true,
            "firstname": "Manny",
            "lastname": "Silverstein",
            "currentCompany": "Electro-Harmonix / New Sensor Corp.",
            "currentJobTitle": "Senior Design Engineer",
            "findmymail": "manny@ehx.com"
        },
        "openAI_usage": "738"
    }
...
}
'''
# One row in the raw_df means one company but there might be 0 - multiple profiles per company
# match the profile to the company based on the "domain" value on
# the profile dict to the 'Root Domain' column in raw_df
def output_results(profiles, raw_df: pl.DataFrame) -> pl.DataFrame:
    """
    Combines profile data with an existing Polars DataFrame and formats it for output.

    Args:
        profiles (Dict[str, Any]): A dictionary of profile data.
        raw_df (pl.DataFrame): The raw Polars DataFrame containing company information.

    Returns:
        pl.DataFrame: A new Polars DataFrame with the combined data.
    """
    
    # 1. Create a DataFrame from the profiles dictionary
    profile_data = []
    for search_query, profile_info in profiles.items():
        validation = profile_info.get('validation_result', {})
        
        # Check if the profile is valid and has the required information
        if validation.get('valid', False):
            first_name = validation.get('firstname', '')
            last_name = validation.get('lastname', '')
            full_name = f"{first_name} {last_name}".strip()
            
            # Append a dictionary representing a single profile to the list
            profile_data.append({
                'Full Name': full_name,
                'First Name': first_name,
                'Job title': validation.get('currentJobTitle', ''),
                'LinkedIn URL': profile_info.get('URL', ''),
                'Company': validation.get('currentCompany', ''),
                'search_query': search_query,
                'email': validation.get('findmymail', ''),
                'Root Domain': profile_info.get('domain', '')
            })

    # If no valid profiles were found, return the original DataFrame
    if not profile_data:
        return raw_df.with_columns(
            pl.lit(None, pl.Utf8).alias('Full Name'),
            pl.lit(None, pl.Utf8).alias('First Name'),
            pl.lit(None, pl.Utf8).alias('Job title'),
            pl.lit(None, pl.Utf8).alias('LinkedIn URL'),
            pl.lit(None, pl.Utf8).alias('Company'),
            pl.lit(None, pl.Utf8).alias('search_query'),
            pl.lit(None, pl.Utf8).alias('email')
        )

    # Convert the list of dictionaries into a Polars DataFrame
    profiles_df = pl.DataFrame(profile_data)

    # 2. Join the raw_df with the profiles_df on the 'Root Domain' column
    # The 'left_join' ensures that all rows from the original raw_df are kept
    # and matching profile data is added. Companies without matching profiles will have nulls.
    final_df = raw_df.join(profiles_df, on='Root Domain', how='left')

    return final_df


if __name__ == "__main__":
    main()
