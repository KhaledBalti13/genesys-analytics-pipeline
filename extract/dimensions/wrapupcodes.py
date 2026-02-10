"""
Extract wrap-up (qualification) codes from Genesys API.
"""

from extract.genesys_api_client import GenesysAPIClient


def extract_wrapup_codes():
    client = GenesysAPIClient()

    endpoint = "/routing/wrapupcodes"
    params = {
        "pageSize": 100,
        "pageNumber": 1
    }

    wrapup_codes = []

    while True:
        response = client.get(endpoint, params)
        wrapup_codes.extend(response.get("entities", []))

        if params["pageNumber"] >= response.get("pageCount", 1):
            break

        params["pageNumber"] += 1

    return wrapup_codes
