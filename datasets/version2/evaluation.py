import climparser
import templater
import argo
import json
import requests
from tqdm import tqdm
from joblib import Parallel, delayed
from multiprocessing import Manager
from alive_progress import alive_bar

MODEL = "gpt5"

# --- Load chat templates with placeholder-based questions and answers ---
chat_templates = templater.load_template("Dataset_testing.json")
evaluation_entries = Manager().list()


def evaluate_template(template, progress_bar):
    reference_response = template["assistant"]
    template["assistant"] = ""

    status_code, llm_response = argo.climrr_query(json.dumps(template), MODEL)
    evaluation_entries.append({"reference": reference_response, "llm": llm_response})

    progress_bar()


# Parallel processing with tqdm for progress tracking
with alive_bar(total=len(chat_templates), title="Evaluating Templates") as progress_bar:
    Parallel(n_jobs=10, backend="threading")(
        delayed(evaluate_template)(template, progress_bar)
        for template in chat_templates
    )

templater.append_entries(f"Evaluation_{MODEL.upper()}.json", list(evaluation_entries))
