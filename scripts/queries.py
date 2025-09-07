# Permutation of queries:
#  [Company Name] + [Job Title] + "site:linkedin.com/in"
#  [domain] + [Job Title] + "site:linkedin.com/in"
# Input:: JSON/Dictionary with Company Name, Company Website, and Job Titles
# Return:: JSON/Dictionary with Query as key and Company Name, and Job Title as values
def gen_queries(companies_data, job_titles, logger):
    queries = {}
    count = 0
    for company in companies_data:
        company_name = company["company"]
        root_domain = company["Root Domain"]
        for title in job_titles:
            queries[f"{company_name} {title} site:linkedin.com/in"] = {"company": company_name, "title": title, "domain": root_domain}
            queries[f"{root_domain} {title} site:linkedin.com/in"] = {"company": company_name, "title": title, "domain": root_domain}
    return queries