import climparser
import templater
import argo
import json
import requests

ARGO_USER = ""

# --- Load chat templates with placeholder-based questions and answers ---
chat_templates = templater.load_template("Dataset.json")

# Loop over each Q&A template
for template in chat_templates[:1]:
    reference_response = template["assistant"]
    template["assistant"] = ""

    status_code, llm_response = argo.climrr_query(ARGO_USER, json.dumps(template))
    print(reference_response)
    print(llm_response)
