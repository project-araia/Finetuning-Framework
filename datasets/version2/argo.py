import requests
import json


def linguistic_variance(user, prompt):

    # API endpoint to POST
    url = "https://apps-dev.inside.anl.gov/argoapi/api/v1/resource/chat/"

    # Data to be sent as a POST in JSON format
    data = {
        "user": user,
        "model": "gpt35",
        "system": "Rephrase the given prompt using natural, grammatically correct English. Introduce linguistic variance in style, tone, or word choice, while keeping the meaning identical.",
        "prompt": [prompt],
        "stop": [],
        "temperature": 0.1,
        "top_p": 0.9,
    }

    # Convert the dict to JSON
    payload = json.dumps(data)

    # Add a header stating that the content type is JSON
    headers = {"Content-Type": "application/json"}

    # Send POST request
    response = requests.post(url, data=payload, headers=headers)

    return response.status_code, response.json()["response"]
