import climparser
import templater
import argo
import json
import requests

ARGO_USER = ""
FORMAT_DATA_ENDPOINT = "http://dishost1.dis.anl.gov:5057/api/v1/format_climate_data"

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

# Define locations for which climate Q&A data will be generated
target_locations = ["Cook, IL", "Montgomery, MD"]#, "Flathead, MT"]  # You can add more locations here

comparison_locations = ["King, WA"]


# Final dataset entries to be stored
generated_entries = []

# Loop over each Q&A template
for template in chat_templates:
    question_template = template["question"]
    answer_template = template["answer"]

    # Extract placeholder variables and embedded expressions from templates
    variable_keys, expression_placeholders = templater.separate_vars_and_exprs(
        question_template + answer_template
    )

    # For each main location in the target set
    for location_str in target_locations:
        county1, state1 = map(str.strip, location_str.split(","))
        template_context = {}
        input_record = {}

        # Fill in the location name if it's used in the template
        if "location" in variable_keys:
            template_context["location"] = location_str

        # CASE 1: Comparison between two locations
        if "compared_location" in variable_keys:
            for compare_str in comparison_locations:
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
                    "climate_data": {key: value for key,value in input_record.items() if not key.endswith("_loc2")},
                    "output_type": "json",
                    "location_name": location_str,
                }

                format_data_request = requests.post(
                    FORMAT_DATA_ENDPOINT,
                    data=json.dumps(request_body),
                    headers={"Content-Type": "application/json"},
                )

                request_body = {
                    "climate_data": {key.strip("_loc2"): value for key,value in input_record.items() if key.endswith("_loc2")},
                    "output_type": "json",
                    "location_name": compare_str,
                }

                format_data_request_loc2 = requests.post(
                    FORMAT_DATA_ENDPOINT,
                    data=json.dumps(request_body),
                    headers={"Content-Type": "application/json"},
                )
 
                status_code, question = argo.linguistic_variance(ARGO_USER, question)
                status_code, answer = argo.linguistic_variance(ARGO_USER, answer)

                generated_entries.append(
                    {
                        "user": question,
                        "input": [format_data_request.json(),format_data_request_loc2.json()],
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

            status_code, question = argo.linguistic_variance(ARGO_USER, question)
            status_code, answer = argo.linguistic_variance(ARGO_USER, answer)

            generated_entries.append(
                {"user": question, "input": [format_data_request.json()], "assistant": answer}
            )

# Save the fully populated training dataset
templater.save_template("Dataset.json", "w", generated_entries)
