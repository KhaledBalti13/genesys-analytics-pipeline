"""
Genesys API client.

- Token-based authentication
- Generic GET method for reference data
- POST method for analytics queries
- No credentials hardcoded
"""

import os
import requests
from time import sleep
from typing import Dict, List


GENESYS_API_BASE_URL = "https://api.genesyscloud.com/api/v2"
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3


class GenesysAPIClient:
    def __init__(self):
        self.client_id = os.getenv("GENESYS_CLIENT_ID")
        self.client_secret = os.getenv("GENESYS_CLIENT_SECRET")

        if not self.client_id or not self.client_secret:
            raise ValueError("Genesys API credentials are not set")

        self.access_token = self._authenticate()

    def _authenticate(self) -> str:
        auth_url = f"{GENESYS_API_BASE_URL}/oauth/token"

        response = requests.post(
            auth_url,
            data={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret
            },
            timeout=REQUEST_TIMEOUT
        )

        response.raise_for_status()
        return response.json()["access_token"]

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }

    # ----------------------------------
    # Generic GET (dimensions, reference data)
    # ----------------------------------
    def get(self, endpoint: str, params: dict) -> Dict:
        response = requests.get(
            f"{GENESYS_API_BASE_URL}{endpoint}",
            headers=self._headers(),
            params=params,
            timeout=REQUEST_TIMEOUT
        )

        response.raise_for_status()
        return response.json()

    # ----------------------------------
    # POST analytics (conversations)
    # ----------------------------------
    def fetch_interactions(
        self,
        start_date: str,
        end_date: str
    ) -> List[Dict]:

        url = f"{GENESYS_API_BASE_URL}/analytics/conversations/details/query"

        payload = {
            "interval": f"{start_date}/{end_date}",
            "paging": {
                "pageSize": 100,
                "pageNumber": 1
            }
        }

        results = []

        while True:
            for attempt in range(MAX_RETRIES):
                response = requests.post(
                    url,
                    headers=self._headers(),
                    json=payload,
                    timeout=REQUEST_TIMEOUT
                )

                if response.status_code == 200:
                    break

                sleep(2)

            response.raise_for_status()
            data = response.json()

            results.extend(data.get("conversations", []))

            if not data.get("paging") or not data["paging"].get("nextPage"):
                break

            payload["paging"]["pageNumber"] += 1

        return results
