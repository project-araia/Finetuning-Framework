import csv
import random
import json

# Function to calculate percentiles for a list of values
def calculate_percentile(value, all_values):
    sorted_values = sorted(all_values)
    index = sorted_values.index(value)
    return (index + 1) / len(sorted_values) * 100

# Reading the CSV data
input_file = '/Users/Akash/Box/Jarvis-Datashare/ClimRR-Data/AnnualTemperatureMaximum.csv'  # Replace with the path to your input file
training_file = 'Training/AnnualTemperatureMaximum/WithInputContext.json'
testing_file = 'Testing/AnnualTemperatureMaximum/WithInputContext.json'

grid_cells = []  # List to store grid cells data
state_values = {}  # Dictionary to store grid cell values by state
country_values = []  # List to store all grid cells' data for national comparison

# Read the CSV file and load the rows
with open(input_file, 'r') as csvfile:
    csvreader = csv.DictReader(csvfile)
    rows = list(csvreader)  # Convert to list of rows for easier random sampling

# Randomly select 100 rows
random.seed(42)
random_rows = random.sample(rows, 200)

# Process the selected rows
for row in random_rows:
    grid_cell = row['Crossmodel']
    historical_temp = float(row['hist'])
    rcp45_mid = float(row['rcp45_midc'])
    rcp45_end = float(row['rcp45_endc'])
    rcp85_mid = float(row['rcp85_midc'])
    rcp85_end = float(row['rcp85_endc'])
    mid45_hist = float(row['mid45_hist'])
    end45_hist = float(row['end45_hist'])
    mid85_hist = float(row['mid85_hist'])
    end85_hist = float(row['end85_hist'])
    mid85_45 = float(row['mid85_45'])
    end85_45 = float(row['end85_45'])

    # Calculate changes
    change_rcp45 = end45_hist
    change_rcp85 = end85_hist

    # Prepare the dictionary for each grid cell
    grid_cells.append({
        "grid_cell": grid_cell,
        "historical": {"temp": historical_temp},
        "rcp85": {
            "mid_century": rcp85_mid,
            "end_century": rcp85_end,
            "change_from_hist_end": change_rcp85,
            "percentile_increase_state": 0,  # Placeholder for percentile (will be calculated later)
            "percentile_increase_country": 0  # Placeholder for percentile (will be calculated later)
        },
        "rcp45": {
            "mid_century": rcp45_mid,
            "end_century": rcp45_end,
            "change_from_hist_end": change_rcp45
        }
    })

    # Collect values for state and national comparisons
    state_values[grid_cell] = {
        "rcp85": change_rcp85,
        "rcp45": change_rcp45
    }
    country_values.append(change_rcp85)
    country_values.append(change_rcp45)

# Calculate percentiles for state and country
for grid_cell in grid_cells:
    # Get all RCP 8.5 changes for the state (state comparison logic not provided in your example)
    rcp85_changes = [state_values[cell]['rcp85'] for cell in state_values]
    grid_cell['rcp85']['percentile_increase_state'] = calculate_percentile(grid_cell['rcp85']['change_from_hist_end'], rcp85_changes)

    # Country comparison for RCP 8.5 (using all data)
    grid_cell['rcp85']['percentile_increase_country'] = calculate_percentile(grid_cell['rcp85']['change_from_hist_end'], country_values)

# Prepare the output JSON format
output_data = []

for grid_cell in grid_cells:
    output_data.append({
        "user": f"Compare the maximum annual temperature increase at grid {grid_cell['grid_cell']} to nearby areas.",
        "input": {
            "grid_cell": grid_cell['grid_cell'],
            "historical": grid_cell['historical'],
            "rcp85": grid_cell['rcp85'],
            "rcp45": grid_cell['rcp45']
        },
        "assistant": (f"The temperature at grid {grid_cell['grid_cell']} is projected to increase by "
                   f"{grid_cell['rcp85']['change_from_hist_end']}°F by the end of the century under RCP 8.5. "
                   f"This places it in the {grid_cell['rcp85']['percentile_increase_state']}th percentile for warming "
                   f"within the state and {grid_cell['rcp85']['percentile_increase_country']}th percentile nationally.")
    })

# Write the output to a JSON file
with open(training_file, 'w') as jsonfile:
    json.dump(output_data[:179], jsonfile, indent=4)

# Write the output to a JSON file
with open(testing_file, 'w') as jsonfile:
    json.dump(output_data[180:], jsonfile, indent=4)

print("Data processing complete. Output saved to", training_file, testing_file)
