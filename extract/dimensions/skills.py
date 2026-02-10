"""
Extract skills reference data from Genesys API.
"""

from extract.genesys_api_client import GenesysAPIClient


def extract_skills():
    client = GenesysAPIClient()

    endpoint = "/routing/skills"
    params = {
        "pageSize": 100,
        "pageNumber": 1
    }

    skills = []

    while True:
        response = client.get(endpoint, params)
        skills.extend(response.get("entities", []))

        if params["pageNumber"] >= response.get("pageCount", 1):
            break

        params["pageNumber"] += 1

    return skills
