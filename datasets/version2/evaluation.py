import climparser
import templater
import argo
import json
import requests

ARGO_USER = ""
#MODEL = "gemini25pro"
MODEL = "gpt5"

# --- Load chat templates with placeholder-based questions and answers ---
chat_templates = templater.load_template("Dataset_testing.json")
evaluation_entries = []

idx = 0

# Loop over each Q&A template
for template in chat_templates:
    reference_response = template["assistant"]
    template["assistant"] = ""

    status_code, llm_response = argo.climrr_query(ARGO_USER, json.dumps(template), MODEL)
    evaluation_entries.append({"reference": reference_response, "llm": llm_response})

    idx +=1
    print(f"Generated {idx} entries")

    templater.append_last_entry(f"Evaluation_{MODEL.upper()}.json", evaluation_entries)    

#templater.save_template(f"Evaluation_{MODEL.upper()}.json", "w", evaluation_entries)
