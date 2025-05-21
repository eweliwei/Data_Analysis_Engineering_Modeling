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
