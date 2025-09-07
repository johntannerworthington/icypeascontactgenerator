# Contact Generation for cold.email

## Overview
This repository contains a lead generation tool that processes company data, generates queries, fetches LinkedIn URLs, enriches profiles, validates them, and extracts emails. The project supports both sequential (`debug.py`) and parallel (`parallel.py`) processing modes.

## Prerequisites
- Python 3.8 or higher
- Required Python packages:
  - `polars`
  - `pandas`
  - `requests`
  - `json`
  - `re`
  - `threading` (for `parallel.py`)
  - `concurrent.futures` (for `parallel.py`)

## Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/rbustillo/Contact-Generation.git
   cd Contact-Generation
   ```
2. Create a virtual environment and activate it:
   Mac:
   ```bash
   python -m venv .env
   source env/bin/activate  # On Windows: env\Scripts\activate
   ```
   Windows:
   ```bash
   python -m venv .env
   env\Scripts\activate
   ```
3. Install the required packages:
   ```bash
   pip install polars pandas requests
   ```

## Configuration
- Ensure the input CSV file (`first10input.csv`) is placed in the root directory.
- Create `creds.py` with your API credentials for Serper, Icypeas, OpenAI, and FindMyMail services if required by the scripts.
  ```python
  # creds.py
  API_KEYS = {
      "SERPER_API_KEY": "977eca7c02e44ee144f448ca644d5513999f3950",
      "FINDMYMAIL_API_KEY": "mfRH3tWYHY8NBWvhMb8Vz1ScyEhFi0sKLv6oMDc667e393ec",
      "ICYPEAS_API_KEY": "68544cb29f734dd6ad88060182636499909a4ae9f57042028d93746ab092896f",
      "GEMINI_API_KEY": "AIzaSyCgCJMIhabwLiSbqXAGJDXJZe0KecZibFA",
      "OPENAI_API_KEY": "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"
  }
  ```

## Usage
### Running Sequentially
To run the project in sequential mode:
```bash
python debug.py
```
- Output files: `all_queries.csv`, `output.csv`, and `clean_output.csv` will be generated.

### Running in Parallel
To run the project with parallel processing:
```bash
python parallel.py
```
- Output files: `all_queries.csv`, `output.csv`, and `clean_output.csv` will be generated.
- Note: Adjust `max_workers` in `parallel.py` (default is 3) based on your system capacity.

## Input File
- The input file (`first10input.csv`) should contain columns: `company`, `Root Domain`, and `job titles`.

## Output Files
- `output.csv`: Contains the enriched data with all original and processed fields.
- `clean_output.csv`: Contains cleaned and validated profile data.
- `all_queries.csv`: Contains all the queries and their exit reason with a LinkedIn URL for easy debugging

## Logging and Metrics
- The script logs various metrics such as queries processed, URLs found, emails extracted, and associated costs (Serper, Icypeas, OpenAI, FindMyMail).
- These are printed to the console upon completion.

## Notes
- Ensure an internet connection is available for API calls.
- The `query_tracker.py` module tracks the behavior of each query and their exit reason
- Adjust the `debug` variable in `debug.py` to limit the number of companies processed for testing.

## Troubleshooting
- If API calls fail, verify your credentials in `creds.py`.
- For performance issues, reduce `max_workers` in `parallel.py` or switch to `debug.py`.
