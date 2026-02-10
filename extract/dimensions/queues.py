"""
Extract queues reference data from Genesys API.
"""

from extract.genesys_api_client import GenesysAPIClient


def extract_queues():
    client = GenesysAPIClient()

    endpoint = "/routing/queues"
    params = {
        "pageSize": 100,
        "pageNumber": 1
    }

    queues = []

    while True:
        response = client.get(endpoint, params)
        queues.extend(response.get("entities", []))

        if params["pageNumber"] >= response.get("pageCount", 1):
            break

        params["pageNumber"] += 1

    return queues
