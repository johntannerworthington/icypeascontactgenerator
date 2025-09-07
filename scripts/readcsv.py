import polars as pl

# Read in CSV with Company Name, Company Website, and Job Titles for every row
# Job Titles is a list of different titles separated by commas
# Function:: read_input
# Input:: CSV file
# Return:: String: Company Name, String: Company Website, String: Root Domain, List[String]: Job Titles
def read_input(filepath):
    # Read the CSV file into a Polars DataFrame
    input_df = pl.read_csv(filepath)
    
    # Select the relevant columns and return the DataFrame with the updated 'job titles' column
    output_df = output_df.select(
        "company", "website", "Root Domain"
    )

    return output_df