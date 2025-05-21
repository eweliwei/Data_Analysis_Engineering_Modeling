import requests
import logging
from typing import Optional, List, Dict, Union
import logging
import pandas as pd

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class DataExtractor:
    def __init__(self, base_url: str = "https://api.data.gov.my/data-catalogue") -> None:
        self.base_url = base_url

    def fetch_api_data(self, endpoint_id: str, max_retries: int = 3) -> Optional[List[Dict]]:
        """Fetch data from the API endpoint.

        Args:
            endpoint_id: endpoint to fetch data from
            max_retries: Maximum number of retry attempts

        """
        endpoint = f"{self.base_url}?id={endpoint_id}"
        for attempt in range(max_retries):
            logger.info(f"Fetching data from {endpoint}")
            try:
                response = requests.get(endpoint)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as e:
                logger.error(
                    f"Attempt {attempt + 1} failed: Error fetching data from {endpoint}: {e}")
                if attempt == max_retries - 1:
                    logger.error(
                        f"Max retries ({max_retries}) reached for endpoint {endpoint}")
                    return None

    def fetch_github_data(self, github_url: str, max_retries: int = 3) -> Optional[Union[pd.DataFrame, Dict]]:
        """Fetch data from a GitHub repository file.

        Args:
            github_url: GitHub URL
            max_retries: Maximum number of retry attempts
        """
        # Convert regular GitHub URL to raw format if needed
        if 'github.com' in github_url and 'raw.githubusercontent.com' not in github_url:
            github_url = github_url.replace(
                'github.com', 'raw.githubusercontent.com').replace('/blob/', '/')

        # Detect file type from URL
        file_type = github_url.split('.')[-1].lower()

        for attempt in range(max_retries):
            logger.info(f"Fetching data from GitHub: {github_url}")
            try:
                if file_type == 'csv':
                    df = pd.read_csv(github_url)
                    return df
                elif file_type == 'json':
                    response = requests.get(github_url)
                    response.raise_for_status()
                    return response.json()
                elif file_type in ['xlsx', 'xls']:
                    df = pd.read_excel(github_url)
                    return df
                else:
                    logger.error(f"Unsupported file type: {file_type}")
                    return None

            except Exception as e:
                logger.error(
                    f"Attempt {attempt + 1} failed: Error fetching data from GitHub: {e}")
                if attempt == max_retries - 1:
                    logger.error(
                        f"Max retries ({max_retries}) reached for GitHub file {github_url}")
                    return None
