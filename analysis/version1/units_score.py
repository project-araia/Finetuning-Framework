import numpy as np
import re
import os
import json

def extract_assistant_responses(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        text = f.read()

    # Regular expression to extract Assistant responses
    pattern = re.compile(r"### Assistant:\s*(.*?)(?=\n###|<\|end_of_text\|>|$)", re.S)
    responses = pattern.findall(text)

    pattern_str = "---------------------------------------------------------------------------------------------"

    # Clean up unwanted headers and separators
    cleaned_responses = []
    for response in responses:
        split_response = response.split(pattern_str)
        cleaned_responses.append(split_response[0])

    return cleaned_responses

def extract_assistant_responses_json(file_path):
    # Load the JSON data from file
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Extract assistant responses
    responses = [entry['assistant'] for entry in data if 'assistant' in entry]

    return responses

# Function to extract (value, unit) pairs
def extract_temp_unit_pairs(text):
    pattern = r'([-+]?\d*\.\d+|\d+)\s*(°?F|degrees Fahrenheit|inches)'
    matches = re.findall(pattern, text, flags=re.IGNORECASE)
    return [(val, unit) for val, unit in matches]

def extract_preci_unit_pairs(text):
    matches = re.findall(r'([-+]?\d*\.\d+|\d+)\s?(inches)', text)
    return [(val, unit) for val, unit in matches]

# Extract grid cell
def extract_grid(text):
    match = re.search(r'R\d{3}C\d{3}', text)
    return match.group(0) if match else None

def normalize_unit(unit):
    unit = unit.strip().lower().replace('°', '')
    if 'fahrenheit' in unit or unit == 'f':
        return 'fahrenheit'
    elif 'inch' in unit:
        return 'inches'
    else:
        return unit  # fallback for other units if needed

def score_output(grid_out, temps_out, grid_ref, temps_ref):
    score = 0

    # Grid match
    if grid_out == grid_ref:
        score += 1

    # Temp + unit matching
    for val, unit in temps_out:
        for ref_val, ref_unit in temps_ref:
            if round(float(val),5) == round(float(ref_val),5):
                score += 1  # Value match
                if normalize_unit(unit) == normalize_unit(ref_unit):
                    score += 1  # Full normalized unit match
                else:
                    score += 0.5  # Mismatch but same value
                break
    return score

# Path to your text file
base_responses = extract_assistant_responses('../runs/training/outputs-may-2025/output_base.txt')
finetuned_responses = extract_assistant_responses('../runs/training/outputs-may-2025/output_finetuned.txt') 
ref_responses = extract_assistant_responses_json('../datasets/Testing/Test-v1.json')

score_count = min(len(base_responses),len(finetuned_responses))
print(score_count)
base_score = 0.
fine_tuned_score = 0.

for i in range(score_count):

    #if i<9:
    output1 = base_responses[i].strip("\n")
    #else:
    #    output1 = base_responses[i+1].strip("\n")
    output2 = finetuned_responses[i].strip("\n")
    correct_output = ref_responses[i]

    # Extract
    grid1 = extract_grid(output1)
    grid2 = extract_grid(output2)
    grid_correct = extract_grid(correct_output)

    val1 = extract_temp_unit_pairs(output1)
    val2 = extract_temp_unit_pairs(output2)

    val_correct = extract_temp_unit_pairs(correct_output)

    score1 = score_output(grid1, val1, grid_correct, val_correct)
    score2 = score_output(grid2, val2, grid_correct, val_correct)
    score_correct = score_output(grid_correct, val_correct, grid_correct, val_correct)

    if i==8:
        print(output1)
        print(output2)
        print(correct_output)
        print(grid2,grid_correct)
    print(i,score1, score2, score_correct)

    base_score = base_score + score1/score_correct
    fine_tuned_score = fine_tuned_score + score2/score_correct


# Display results
print(f"Base Model Similarity Score: {base_score/score_count:.4f}")
print(f"Fine-tuned Model Similarity Score: {fine_tuned_score/score_count:.4f}")
