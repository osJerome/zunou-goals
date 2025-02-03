import requests


def request_fireflies(fireflies_api_key: str, query: dict) -> dict:
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {fireflies_api_key}"
    }
    
    try:
        response = requests.post("https://api.fireflies.ai/graphql", json=query, headers=headers)
    
        status_code = response.status_code
        if status_code != 200:
            raise Exception(f"Failed to request Fireflies, status code: {status_code}")
        return response.json()
    except Exception as e:
        raise Exception(f"Failed to request fireflies, {str(e)}")
