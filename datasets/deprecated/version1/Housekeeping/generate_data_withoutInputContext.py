import pandas as pd
import json

# Load the CSV file
csv_file = "/Users/Akash/Box/Jarvis-Datashare/ClimRR-Data/AnnualTemperatureMaximum.csv"  # Replace with your actual CSV file path
df = pd.read_csv(csv_file)

df = df.head(200)

# Define column explanations
column_explanations = {
    "Crossmodel": "Represents the grid cell identifier.",
    "hist": "Historical annual maximum temperature in degrees Fahrenheit.",
    "rcp45_midc": "Projected mid-century temperature under RCP4.5 scenario.",
    "rcp45_endc": "Projected end-century temperature under RCP4.5 scenario.",
    "rcp85_midc": "Projected mid-century temperature under RCP8.5 scenario.",
    "rcp85_endc": "Projected end-century temperature under RCP8.5 scenario.",
    "mid45_hist": "Difference between mid-century RCP4.5 projection and historical temperature.",
    "end45_hist": "Difference between end-century RCP4.5 projection and historical temperature.",
    "mid85_hist": "Difference between mid-century RCP8.5 projection and historical temperature.",
    "end85_hist": "Difference between end-century RCP8.5 projection and historical temperature.",
    "mid85_45": "Difference between mid-century RCP8.5 and RCP4.5 projections.",
    "end85_45": "Difference between end-century RCP8.5 and RCP4.5 projections."
}

# Generate fine-tuning data
fine_tuning_data = []

# Add direct column definition Q&A examples
for col, desc in column_explanations.items():
    fine_tuning_data.append({
        "user": f"What does '{col}' represent in the dataset?",
        "assistant": desc,
    })

for _, row in df.iterrows():
    # Create a question about interpreting a specific row
    user_question = f"Interpret this data row: {', '.join(map(str, row.values))}"
    
    # Generate an assistant response explaining the data
    explanation = f"For grid cell {row['Crossmodel']}, the historical maximum temperature is {row['hist']:.2f}°F."
    explanation += f" Under scenario RCP4.5, the projected mid-century temperature is {row['rcp45_midc']:.2f}°F and end-century is {row['rcp45_endc']:.2f}°F."
    explanation += f" Under RCP8.5, the mid-century projection is {row['rcp85_midc']:.2f}°F and the end-century is {row['rcp85_endc']:.2f}°F."
    explanation += f" The temperature differences compared to historical values are as follows:"
    explanation += f" mid-century RCP4.5 ({row['mid45_hist']:.2f}°F), end-century RCP4.5 ({row['end45_hist']:.2f}°F),"
    explanation += f" mid-century RCP8.5 ({row['mid85_hist']:.2f}°F), end-century RCP8.5 ({row['end85_hist']:.2f}°F)."
    
    # Append formatted JSON entry
    fine_tuning_data.append({
        "user": user_question,
        "assistant": explanation,
    })

# Save to JSONL
training_file = "Training/AnnualTemperatureMaximum/WithoutInputContext.json"
with open(training_file, "w") as f:
   json.dump(fine_tuning_data[:179], f, indent=4) 

# Save to JSONL
testing_file = "Testing/AnnualTemperatureMaximum/WithoutInputContext.json"
with open(testing_file, "w") as f:
   json.dump(fine_tuning_data[180:], f, indent=4) 

print(f"Fine-tuning dataset saved to {training_file} and {testing_file}")
