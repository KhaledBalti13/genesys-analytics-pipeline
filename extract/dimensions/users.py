"""
Extract users reference data from Genesys API.
"""

from extract.genesys_api_client import GenesysAPIClient


def extract_users():
    client = GenesysAPIClient()

    endpoint = "/users"
    params = {
        "pageSize": 100,
        "pageNumber": 1
    }

    users = []

    while True:
        response = client.get(endpoint, params)
        users.extend(response.get("entities", []))

        if params["pageNumber"] >= response.get("pageCount", 1):
            break

        params["pageNumber"] += 1

    return users
