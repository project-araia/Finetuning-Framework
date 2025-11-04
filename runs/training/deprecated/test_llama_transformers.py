import os
import torch
from transformers import AutoModelForCausalLM, AutoTokenizer, TextStreamer
from datasets import load_dataset

# Model configuration
model_name = "meta-llama/Llama-3.1-70B" # Adjust if using a different model
#model_name = "/home/adhruv/.cache/huggingface/hub/models--unsloth--meta-llama-3.1-70b-bnb-4bit/snapshots/a009b8db2439814febe725486a5ed388f12a8744"
max_seq_length = 2048
load_in_4bit = True

tokenizer = AutoTokenizer.from_pretrained(model_name)

# Load model with quantization if applicable
if load_in_4bit:
    from transformers import BitsAndBytesConfig

    bnb_config = BitsAndBytesConfig(
        load_in_4bit=True,
        bnb_4bit_compute_dtype=torch.float16,
        bnb_4bit_use_double_quant=True,
    )

    model = AutoModelForCausalLM.from_pretrained(
        model_name,
        quantization_config=bnb_config,
        device_map="auto"
    )
else:
    model = AutoModelForCausalLM.from_pretrained(model_name, device_map="auto")

# Formatting prompt
araia_prompt = """
Below is a User query that describes a task or a question, paired with an Input along with its context.
Write the Assistant's response that appropriately completes the request. If the Input is missing you should ignore it. 

### User:
{}

### Input:
{}

### Assistant:
"""

# Load dataset
testing_dataset = f'{os.getenv("PROJECT_HOME")}/datasets/Testing/AnnualTemperatureMaximum/WithInputContext.json'
dataset = load_dataset("json", data_files=testing_dataset)["train"]

queries = dataset["user"]
outputs = dataset["assistant"]
inputs = dataset.get("input", [""] * len(queries))

# Inference loop
streamer = TextStreamer(tokenizer)

for query, input_text, output in zip(queries, inputs, outputs):
    print("---------------------------------------------------------------------------------------------")
    prompt = araia_prompt.format(query, input_text, "")
    input_token = tokenizer(prompt, return_tensors="pt").to("cuda")

    with torch.no_grad():
        model.generate(**input_token, streamer=streamer, max_new_tokens=128)
