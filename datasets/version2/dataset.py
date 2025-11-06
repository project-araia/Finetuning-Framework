import climparser
import templater
import argo
import json
import requests
from alive_progress import alive_bar
from joblib import Parallel, delayed
from multiprocessing import Manager
from operator import itemgetter

ARGO_USER = "adhruv"
FORMAT_DATA_ENDPOINT = "http://dishost1.dis.anl.gov:5057/api/v1/format_climate_data"
DATASET_TYPE = "testing"

# --- Load climate dataset ---
climate_df = climparser.load_dataset("FullData.csv")

ignored_climrr_keys = [
    "OID_",
    "Crossmodel",
    "NAME",
    "State",
    "State_Abbr",
    "GlobalID",
    "created_us",
    "created_da",
    "last_edite",
    "last_edi_1",
    "Shape_STAr",
    "Shape_STLe",
    "X",
    "Y",
    "TRACTCE",
    "GEOID",
    "NAME_1",
    "NAMELSAD",
    "Percentage_of_the_population_65",
    "Gini_Index_of_income_inequality",
    "Pop_below_U_S__Census_poverty_l",
    "Percentage_of_housing_units_tha",
    "Aggregate_Resilience_Indicator",
    "Aggregate_Resilience_Indicator_",
    "The_net_migration__internationa",
    "OBJECTID_12",
    "OBJECTID_12_13",
    "Crossmodel_12",
    "OBJECTID_1",
    "Crossmodel_1",
]

# --- Load chat templates with placeholder-based questions and answers ---
chat_templates = templater.load_template("Templates_Extended.json")

print(f"Generating dataset from {len(chat_templates)} template questions.")

target_locations = {
    "training": [
        # Midwest
        "Cook, IL",
        "Lake, IL",
        "DuPage, IL",
        "Dane, WI",
        "Milwaukee, WI",
        "Hennepin, MN",
        "Johnson, IA",
        "Lancaster, NE",
        "Douglas, NE",
        "Franklin, OH",
        "Cuyahoga, OH",
        "Hamilton, OH",
        "Wayne, MI",
        "Oakland, MI",
        "Washtenaw, MI",
        "Marion, IN",
        "Allen, IN",
        "St. Louis, MO",
        "Jackson, MO",
        # Northeast
        "Montgomery, MD",
        "Baltimore, MD",
        "Fairfax, VA",
        "Loudoun, VA",
        "Allegheny, PA",
        "Monroe, NY",
        "Onondaga, NY",
        "Erie, NY",
        "Suffolk, NY",
        "Nassau, NY",
        "Essex, MA",
        "Middlesex, MA",
        "Hampden, MA",
        "Hartford, CT",
        "New Haven, CT",
        "Providence, RI",
        "Chittenden, VT",
        "Rockingham, NH",
        # Southeast
        "Fulton, GA",
        "DeKalb, GA",
        "Cobb, GA",
        "Orange, FL",
        "Miami-Dade, FL",
        "Hillsborough, FL",
        "Duval, FL",
        "Jefferson, AL",
        "Shelby, AL",
        "Knox, TN",
        "Davidson, TN",
        "Wake, NC",
        "Mecklenburg, NC",
        "Charleston, SC",
        "Richland, SC",
        "Pulaski, AR",
        "Orleans, LA",
        "East Baton Rouge, LA",
        "Hinds, MS",
        "Mobile, AL",
        # Great Plains / Mountain West
        "Flathead, MT",
        "Gallatin, MT",
        "Yellowstone, MT",
        "Larimer, CO",
        "Boulder, CO",
        "Denver, CO",
        "El Paso, CO",
        "Laramie, WY",
        "Natrona, WY",
        "Ada, ID",
        "Bonneville, ID",
        "Salt Lake, UT",
        "Utah, UT",
        "Washoe, NV",
        "Clark, NV",
        "Johnson, KS",
        "Sedgwick, KS",
        "Tulsa, OK",
        "Oklahoma, OK",
        # Southwest
        "Maricopa, AZ",
        "Pima, AZ",
        "Yavapai, AZ",
        "Bernalillo, NM",
        "Santa Fe, NM",
        "El Paso, TX",
        "Travis, TX",
        "Bexar, TX",
        "Dallas, TX",
        "Harris, TX",
        "Tarrant, TX",
        "Collin, TX",
        "Williamson, TX",
        "Lubbock, TX",
        "McLennan, TX",
        # West Coast
        "Los Angeles, CA",
        "Orange, CA",
        "San Diego, CA",
        "Riverside, CA",
        "San Bernardino, CA",
        "Santa Clara, CA",
        "San Mateo, CA",
        "Alameda, CA",
        "Contra Costa, CA",
        "Sacramento, CA",
        "Placer, CA",
        "Multnomah, OR",
        "Washington, OR",
        "Lane, OR",
        "King, WA",
        "Snohomish, WA",
        "Pierce, WA",
        "Spokane, WA",
        "Whatcom, WA",
    ],
    "testing": [
        # Northeast
        "Albany, NY",
        "Broome, NY",
        "Saratoga, NY",
        "Oneida, NY",
        "Burlington, NJ",
        "Mercer, NJ",
        "Camden, NJ",
        "Bergen, NJ",
        "New Castle, DE",
        "Penobscot, ME",
        "Cumberland, ME",
        "Strafford, NH",
        "Addison, VT",
        "Franklin, MA",
        "Worcester, MA",
        "New London, CT",
        "York, ME",
        # Mid-Atlantic / Appalachia
        "Frederick, MD",
        "Prince George's, MD",
        "Chester, PA",
        "Lancaster, PA",
        "Lehigh, PA",
        "Roanoke, VA",
        "Augusta, VA",
        "Kanawha, WV",
        "Boone, KY",
        "Fayette, WV",
        # Midwest
        "Peoria, IL",
        "McLean, IL",
        "Story, IA",
        "Polk, IA",
        "Sedgwick, KS",
        "Riley, KS",
        "Cass, ND",
        "Burleigh, ND",
        "Minnehaha, SD",
        "Pennington, SD",
        "Shawnee, KS",
        "Wood, OH",
        "Lucas, OH",
        "Genesee, MI",
        "Kent, MI",
        "Brown, WI",
        "Outagamie, WI",
        # South / Southeast
        "Leon, FL",
        "Escambia, FL",
        "Lee, AL",
        "Madison, AL",
        "Lauderdale, MS",
        "Greene, NC",
        "Forsyth, NC",
        "Buncombe, NC",
        "Knox, KY",
        "Jefferson, GA",
        "Chatham, GA",
        "Leon, FL",
        "St. Johns, FL",
        "Manatee, FL",
        "Collier, FL",
        # Gulf / Central Plains
        "Rapides, LA",
        "Ouachita, LA",
        "Harrison, TX",
        "Midland, TX",
        "Tom Green, TX",
        "Ector, TX",
        "Nueces, TX",
        "Cameron, TX",
        "Hidalgo, TX",
        "Grayson, TX",
        # Mountain West
        "Missoula, MT",
        "Ravalli, MT",
        "Park, WY",
        "Carbon, WY",
        "Bannock, ID",
        "Twin Falls, ID",
        "Cache, UT",
        "Davis, UT",
        "Mesa, CO",
        "Pueblo, CO",
        "Grand, CO",
        "Yuma, AZ",
        "Mohave, AZ",
        # Pacific / West Coast
        "Humboldt, CA",
        "Shasta, CA",
        "San Luis Obispo, CA",
        "Sonoma, CA",
        "Santa Barbara, CA",
        "Jackson, OR",
        "Deschutes, OR",
        "Benton, WA",
        "Chelan, WA",
        "Skagit, WA",
        "Kitsap, WA",
        "Clallam, WA",
    ],
}

comparison_locations = {"training": ["Pitkin, CO"], "testing": ["Yukon-Koyukuk, AK"]}


def dataset_loop(target_location, generated_entries, progress_bar):

    # For each main location in the target set
    for location_str in [target_location]:
        county1, state1 = map(str.strip, location_str.split(","))
        template_context = {}
        input_record = {}

        # Loop over each Q&A template
        for template in chat_templates:
            question_template = template["question"]
            answer_template = template["answer"]

            # Extract placeholder variables and embedded expressions from templates
            variable_keys, expression_placeholders = templater.separate_vars_and_exprs(
                question_template + answer_template
            )

            # Fill in the location name if it's used in the template
            if "location" in variable_keys:
                template_context["location"] = location_str

            # CASE 1: Comparison between two locations
            if "compared_location" in variable_keys:
                for compare_str in comparison_locations[DATASET_TYPE]:
                    progress_bar.text(
                        f"Location: {location_str}, Compared: {compare_str}"
                    )
                    progress_bar()

                    county2, state2 = map(str.strip, compare_str.split(","))
                    template_context["compared_location"] = compare_str

                    # Get average climate data for both locations
                    loc_data1 = climparser.query_mean(climate_df, county1, state1)
                    loc_data2 = climparser.query_mean(climate_df, county2, state2)

                    # Populate template context with variables from the primary location
                    for key, value in loc_data1.items():
                        if key in variable_keys:
                            template_context[key] = value
                        if key not in ignored_climrr_keys:
                            input_record[key] = value

                    # Populate template context with variables from the comparison location
                    for key, value in loc_data2.items():
                        key_loc2 = key + "_loc2"
                        if key_loc2 in variable_keys:
                            template_context[key_loc2] = value
                        if key not in ignored_climrr_keys:
                            input_record[key_loc2] = value

                    # Evaluate expressions using the full template context
                    for expr in expression_placeholders:
                        try:
                            template_context[expr] = eval(expr, {}, template_context)
                        except Exception as e:
                            template_context[expr] = f"[eval error: {e}]"

                    # Format the final question and answer using resolved variables/expressions
                    question = question_template.format(**template_context)
                    answer = answer_template.format(**template_context)

                    request_body = {
                        "climate_data": {
                            key: value
                            for key, value in input_record.items()
                            if not key.endswith("_loc2")
                        },
                        "output_type": "json",
                        "location_name": location_str,
                    }

                    format_data_request = requests.post(
                        FORMAT_DATA_ENDPOINT,
                        data=json.dumps(request_body),
                        headers={"Content-Type": "application/json"},
                    )

                    request_body = {
                        "climate_data": {
                            key.strip("_loc2"): value
                            for key, value in input_record.items()
                            if key.endswith("_loc2")
                        },
                        "output_type": "json",
                        "location_name": compare_str,
                    }

                    format_data_request_loc2 = requests.post(
                        FORMAT_DATA_ENDPOINT,
                        data=json.dumps(request_body),
                        headers={"Content-Type": "application/json"},
                    )

                    status_code, question = argo.linguistic_variance(question)
                    status_code, answer = argo.linguistic_variance(answer)

                    generated_entries.append(
                        {
                            "user": question,
                            "input": [
                                format_data_request.json(),
                                format_data_request_loc2.json(),
                            ],
                            "assistant": answer,
                        }
                    )

            # CASE 2: Single-location (no comparison)
            else:

                loc_data = climparser.query_center(climate_df, county1, state1)

                for key, value in loc_data.items():
                    if key in variable_keys:
                        template_context[key] = value
                    if key not in ignored_climrr_keys:
                        input_record[key] = value

                # Evaluate any expressions that use the context
                for expr in expression_placeholders:
                    try:
                        template_context[expr] = eval(expr, {}, template_context)
                    except Exception as e:
                        template_context[expr] = f"[eval error: {e}]"

                # Format question and answer for the current template and location
                question = question_template.format(**template_context)
                answer = answer_template.format(**template_context)

                request_body = {
                    "climate_data": input_record,
                    "output_type": "json",
                    "location_name": location_str,
                }

                format_data_request = requests.post(
                    FORMAT_DATA_ENDPOINT,
                    data=json.dumps(request_body),
                    headers={"Content-Type": "application/json"},
                )

                status_code, question = argo.linguistic_variance(question)
                status_code, answer = argo.linguistic_variance(answer)

                generated_entries.append(
                    {
                        "user": question,
                        "input": [format_data_request.json()],
                        "assistant": answer,
                    }
                )
                progress_bar.text(f"Location: {location_str}")
                progress_bar()


manager = Manager()
generated_entries = manager.list()

data_chunks = itemgetter(*range(0, 10))

# Create a shared alive_progress bar
total_entries = (
    len(data_chunks(target_locations[DATASET_TYPE]))
    * len(chat_templates)
    * len(comparison_locations[DATASET_TYPE])
)

# Wrap the shared progress tracker with alive_progress
with alive_bar(total_entries, title="Processing Datasets") as progress_bar:
    # Run the parallel processing
    Parallel(n_jobs=10, backend="threading")(
        delayed(dataset_loop)(target_location, generated_entries, progress_bar)
        for target_location in data_chunks(target_locations[DATASET_TYPE])
    )

templater.append_entries(f"Dataset_{DATASET_TYPE}.json", list(generated_entries))
