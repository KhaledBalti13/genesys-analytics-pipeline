"""
Extract divisions reference data from Genesys API.
"""

from extract.genesys_api_client import GenesysAPIClient


def extract_divisions():
    client = GenesysAPIClient()

    endpoint = "/authorization/divisions"
    params = {
        "pageSize": 100,
        "pageNumber": 1
    }

    divisions = []

    while True:
        response = client.get(endpoint, params)
        divisions.extend(response.get("entities", []))

        if params["pageNumber"] >= response.get("pageCount", 1):
            break

        params["pageNumber"] += 1

    return divisions
