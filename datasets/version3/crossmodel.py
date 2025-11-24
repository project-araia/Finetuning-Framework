import climparser
import templater
import csv

# --- Load climate dataset ---
climate_df = climparser.load_dataset("FullData.csv")

# Define locations for which climate Q&A data will be generated
target_locations = [
    "Cook, IL",
    "Montgomery, MD",
    "Flathead, MT",
    "King, WA",
    "Glacier, MT",
    "DuPage, IL",
    "Fairfax, VA",
    "Los Angeles, CA",
    "Harris, TX",
    "Maricopa, AZ",
    "Miami-Dade, FL",
    "Clark, NV",
    "Travis, TX",
    "Multnomah, OR",
    "Salt Lake, UT",
    "Denver, CO",
    "Cuyahoga, OH",
    "Allegheny, PA",
    "Hennepin, MN",
    "Pima, AZ",
    "Pierce, WA",
    "San Diego, CA",
    "Kings, NY",
    "Erie, NY",
    "Suffolk, MA",
    "Worcester, MA",
]

# Final dataset entries to be stored
generated_entries = []

# For each main location in the target set
for location_str in target_locations:

    # query data from location.
    county, state = map(str.strip, location_str.split(","))
    loc_data = climparser.query_center(climate_df, county, state)

    generated_entries.append(
        {
            "State": state,
            "County": county,
            "Crossmodel": loc_data["Crossmodel"],
            "Crossmodel_1": loc_data["Crossmodel_1"],
            "Crossmodel_12": loc_data["Crossmodel_12"],
        }
    )

# Find max width of each column for pretty alignment
headers = ["County", "State", "Crossmodel", "Crossmodel_1", "Crossmodel_12"]
col_widths = {
    h: max(len(h), max(len(str(row[h])) for row in generated_entries)) for h in headers
}

with open("output.txt", "w") as f:
    # Header row
    header_line = "  ".join(h.ljust(col_widths[h]) for h in headers)
    f.write(header_line + "\n")
    f.write("-" * len(header_line) + "\n")

    # Data rows
    for row in generated_entries:
        line = "  ".join(str(row[h]).ljust(col_widths[h]) for h in headers)
        f.write(line + "\n")
