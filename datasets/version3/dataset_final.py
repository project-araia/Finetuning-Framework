import climparser
import templater
import json
import requests
import re
from alive_progress import alive_bar
from joblib import Parallel, delayed
from multiprocessing import Manager
from operator import itemgetter
import os

# --- CONFIGURATION ---
FORMAT_DATA_ENDPOINT = "https://climrr-llm.dis.anl.gov/llm/api/v1/format_climate_data"
#"http://dishost1.dis.anl.gov:5057/api/v1/format_climate_data"
DATASET_TYPE = "testing"
ARGO_API_URL = "https://apps-dev.inside.anl.gov/argoapi/api/v1/resource/chat/"
ARGO_USER = os.environ.get("ARGO_USER", "saketc")
ARGO_MODEL = os.environ.get("ARGO_MODEL", "gpt35")
TEMPLATE_FILE = "templates_climrr_queries.txt"

# --- Load climate dataset ---
climate_df = climparser.load_dataset("FullData.csv")

ignored_climrr_keys = [
    "OID_", "Crossmodel", "NAME", "State", "State_Abbr", "GlobalID",
    "created_us", "created_da", "last_edite", "last_edi_1", "Shape_STAr",
    "Shape_STLe", "X", "Y", "TRACTCE", "GEOID", "NAME_1", "NAMELSAD",
    "Percentage_of_the_population_65", "Gini_Index_of_income_inequality",
    "Pop_below_U_S__Census_poverty_l", "Percentage_of_housing_units_tha",
    "Aggregate_Resilience_Indicator", "Aggregate_Resilience_Indicator_",
    "The_net_migration__internationa", "OBJECTID_12", "OBJECTID_12_13",
    "Crossmodel_12", "OBJECTID_1", "Crossmodel_1"
]

# --- TARGET LOCATIONS ---
target_locations = {
    "training": [
        "Cook, IL", "Lake, IL", "DuPage, IL", "Dane, WI", "Milwaukee, WI",
        "Hennepin, MN", "Johnson, IA", "Lancaster, NE", "Douglas, NE",
        "Franklin, OH", "Cuyahoga, OH", "Hamilton, OH", "Wayne, MI",
        "Oakland, MI", "Washtenaw, MI", "Marion, IN", "Allen, IN",
        "St. Louis, MO", "Jackson, MO", "Montgomery, MD", "Baltimore, MD",
        "Fairfax, VA", "Loudoun, VA", "Allegheny, PA", "Monroe, NY",
        "Onondaga, NY", "Erie, NY", "Suffolk, NY", "Nassau, NY", "Essex, MA",
        "Middlesex, MA", "Hampden, MA", "Hartford, CT", "New Haven, CT",
        "Providence, RI", "Chittenden, VT", "Rockingham, NH", "Fulton, GA",
        "DeKalb, GA", "Cobb, GA", "Orange, FL", "Miami-Dade, FL",
        "Hillsborough, FL", "Duval, FL", "Jefferson, AL", "Shelby, AL",
        "Knox, TN", "Davidson, TN", "Wake, NC", "Mecklenburg, NC",
        "Charleston, SC", "Richland, SC", "Pulaski, AR", "Orleans, LA",
        "East Baton Rouge, LA", "Hinds, MS", "Mobile, AL", "Flathead, MT",
        "Gallatin, MT", "Yellowstone, MT", "Larimer, CO", "Boulder, CO",
        "Denver, CO", "El Paso, CO", "Laramie, WY", "Natrona, WY", "Ada, ID",
        "Bonneville, ID", "Salt Lake, UT", "Utah, UT", "Washoe, NV",
        "Clark, NV", "Johnson, KS", "Sedgwick, KS", "Tulsa, OK",
        "Oklahoma, OK", "Maricopa, AZ", "Pima, AZ", "Yavapai, AZ",
        "Bernalillo, NM", "Santa Fe, NM", "El Paso, TX", "Travis, TX",
        "Bexar, TX", "Dallas, TX", "Harris, TX", "Tarrant, TX",
        "Collin, TX", "Williamson, TX", "Lubbock, TX", "McLennan, TX",
        "Los Angeles, CA", "Orange, CA", "San Diego, CA", "Riverside, CA",
        "San Bernardino, CA", "Santa Clara, CA", "San Mateo, CA",
        "Alameda, CA", "Contra Costa, CA", "Sacramento, CA", "Placer, CA",
        "Multnomah, OR", "Washington, OR", "Lane, OR", "King, WA",
        "Snohomish, WA", "Pierce, WA", "Spokane, WA", "Whatcom, WA",
    ],
    "testing": [
        "Albany, NY", "Broome, NY", "Saratoga, NY", "Oneida, NY",
        "Burlington, NJ", "Mercer, NJ", "Camden, NJ", "Bergen, NJ",
        "New Castle, DE", "Penobscot, ME", "Cumberland, ME", "Strafford, NH",
        "Addison, VT", "Franklin, MA", "Worcester, MA", "New London, CT",
        "York, ME", "Frederick, MD", "Prince George's, MD", "Chester, PA",
        "Lancaster, PA", "Lehigh, PA", "Roanoke, VA", "Augusta, VA",
        "Kanawha, WV", "Boone, KY", "Fayette, WV", "Peoria, IL", "McLean, IL",
        "Story, IA", "Polk, IA", "Sedgwick, KS", "Riley, KS", "Cass, ND",
        "Burleigh, ND", "Minnehaha, SD", "Pennington, SD", "Shawnee, KS",
        "Wood, OH", "Lucas, OH", "Genesee, MI", "Kent, MI", "Brown, WI",
        "Outagamie, WI", "Leon, FL", "Escambia, FL", "Lee, AL", "Madison, AL",
        "Lauderdale, MS", "Greene, NC", "Forsyth, NC", "Buncombe, NC",
        "Knox, KY", "Jefferson, GA", "Chatham, GA", "St. Johns, FL",
        "Manatee, FL", "Collier, FL", "Rapides, LA", "Ouachita, LA",
        "Harrison, TX", "Midland, TX", "Tom Green, TX", "Ector, TX",
        "Nueces, TX", "Cameron, TX", "Hidalgo, TX", "Grayson, TX",
        "Missoula, MT", "Ravalli, MT", "Park, WY", "Carbon, WY", "Bannock, ID",
        "Twin Falls, ID", "Cache, UT", "Davis, UT", "Mesa, CO", "Pueblo, CO",
        "Grand, CO", "Yuma, AZ", "Mohave, AZ", "Humboldt, CA", "Shasta, CA",
        "San Luis Obispo, CA", "Sonoma, CA", "Santa Barbara, CA", "Jackson, OR",
        "Deschutes, OR", "Benton, WA", "Chelan, WA", "Skagit, WA",
        "Kitsap, WA", "Clallam, WA",
    ],
}

comparison_locations = {"training": ["Pitkin, CO"], "testing": ["Yukon-Koyukuk, AK"]}


# --- 1. TEXT TEMPLATE PARSER ---
def parse_txt_templates(filepath):
    """Parses the custom text file format into a list of dictionaries."""
    templates = []
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
    except FileNotFoundError:
        print(f"[ERROR] Template file not found: {filepath}")
        return []

    raw_entries = re.split(r'-{10,}', content)
    
    for entry in raw_entries:
        entry = entry.strip()
        if not entry:
            continue
            
        query_match = re.search(r'Query:\s*(.+)', entry)
        answer_match = re.search(r'Answer:\s*(.+)', entry)
        
        if query_match and answer_match:
            templates.append({
                "question": query_match.group(1).strip(),
                "answer": answer_match.group(1).strip()
            })
            
    return templates


# --- 2. METADATA INFERENCE ---
def infer_metadata(key):
    """
    Infers distinct metadata (TimePeriod, Seasonality, Variable Name) 
    based on the ClimRR variable key.
    """
    meta = {
        "TimePeriod": "historical",
        "Seasonality": "annual",
        "ClimateVariable": "variable",
        "RCPscenario": "",
    }

    if "_hist" in key:
        meta["TimePeriod"] = "historical"
    elif "_midc" in key:
        meta["TimePeriod"] = "projected mid-century"
    elif "_endc" in key:
        meta["TimePeriod"] = "projected end-century"
    
    if "_rcp45" in key:
        meta["RCPscenario"] = "RCP 4.5"
    elif "_rcp85" in key:
        meta["RCPscenario"] = "RCP 8.5"

    if "_winter" in key or "Win" in key:
        meta["Seasonality"] = "winter"
    elif "_spring" in key or "Spr" in key:
        meta["Seasonality"] = "spring"
    elif "_summer" in key or "Sum" in key:
        meta["Seasonality"] = "summer"
    elif "_autumn" in key or "_autum" in key or "Aut" in key:
        meta["Seasonality"] = "autumn"
    else:
        meta["Seasonality"] = "annual"

    if "hdd" in key: meta["ClimateVariable"] = "Heating Degree Days"
    elif "cdd" in key: meta["ClimateVariable"] = "Cooling Degree Days"
    elif "tempmax" in key: meta["ClimateVariable"] = "Average Maximum Temperature"
    elif "tempmin" in key: meta["ClimateVariable"] = "Average Minimum Temperature"
    elif "precip" in key: meta["ClimateVariable"] = "Total Precipitation"
    elif "wind" in key: meta["ClimateVariable"] = "Wind Speed"
    elif "FWI" in key: meta["ClimateVariable"] = "Fire Weather Index"
    elif "noprecip" in key: meta["ClimateVariable"] = "Days without Precipitation"

    return meta


# --- 3. COMPUTATION MODULE ---
def process_custom_logic(text, context):
    """
    Handles custom template logic tags: 
    {COMPARE|val1|val2} and {PERCENTAGE|val1|val2}
    """
    
    def compare_replacer(match):
        try:
            v1_key, v2_key = match.group(1), match.group(2)
            val1 = float(context.get(v1_key, 0))
            val2 = float(context.get(v2_key, 0))
            
            diff = abs(val1 - val2)
            threshold = 0.5
            
            if diff < threshold:
                return "about the same as"
            elif val1 > val2:
                return "higher"
            else:
                return "lower"
        except (ValueError, TypeError):
            return "[comparison unavailable]"

    def percentage_replacer(match):
        try:
            v1_key, v2_key = match.group(1), match.group(2)
            val1 = float(context.get(v1_key, 0))
            total = float(context.get(v2_key, 1))
            if total == 0: return "0"
            pct = (val1 / total) * 100
            return f"{pct:.1f}"
        except (ValueError, TypeError):
            return "[calc unavailable]"

    # Regex updated to allow sanitized keys (which only contain alphanumeric and underscores)
    text = re.sub(r'\{COMPARE\|([a-zA-Z0-9_]+)\|([a-zA-Z0-9_]+)\}', compare_replacer, text)
    text = re.sub(r'\{PERCENTAGE\|([a-zA-Z0-9_]+)\|([a-zA-Z0-9_]+)\}', percentage_replacer, text)
    
    return text

def sanitize_formatting(text, context):
    """
    Replaces dots (.), spaces ( ), and tildes (~) with underscores (_) 
    in both the text placeholders and the context dictionary keys.
    This prevents .format() from crashing on keys like "RCP4.5".
    """
    # 1. Sanitize Context Keys
    clean_ctx = {}
    for k, v in context.items():
        # Replace illegal characers for python formatting
        new_k = k.replace("~", "_").replace(".", "_").replace(" ", "_")
        clean_ctx[new_k] = v

    # 2. Sanitize Placeholders in Text
    # Looks for {Any_String_Here} and replaces the key inside
    def key_replacer(match):
        full_content = match.group(1)
        
        # Check if it's a custom logic tag (COMPARE|...)
        if "|" in full_content:
            parts = full_content.split("|")
            # Sanitize arguments (indexes 1+)
            sanitized_args = [p.replace("~", "_").replace(".", "_").replace(" ", "_") for p in parts[1:]]
            # Reconstruct: {COMPARE|arg1_clean|arg2_clean}
            return "{" + parts[0] + "|" + "|".join(sanitized_args) + "}"
        else:
            # Standard placeholder
            return "{" + full_content.replace("~", "_").replace(".", "_").replace(" ", "_") + "}"

    # Regex matches anything inside curly braces
    clean_text = re.sub(r'\{([^{}]+)\}', key_replacer, text)
    
    return clean_text, clean_ctx


# --- ARGO API ---
def call_argo_api(prompt: str, system_role: str = "You are a content safety classifier.") -> str:
    headers = {"Content-Type": "application/json"}
    payload = {
        "user": ARGO_USER,
        "model": ARGO_MODEL,
        "system": system_role,
        "prompt": [prompt],
        "stop": [],
        "temperature": 0.0,
        "top_p": 0.9,
        "max_tokens": 512,
        "max_completion_tokens": 200,
        "logprobs": False,
        "top_logprobs": 0
    }

    try:
        resp = requests.post(ARGO_API_URL, headers=headers, data=json.dumps(payload), timeout=60)
        resp.raise_for_status()
        data = resp.json()
        return data.get("response", "").strip()
    except Exception as e:
        return "error"

def linguistic_variance(prompt: str) -> tuple[int, str]:
    if not prompt or not isinstance(prompt, str):
        return 400, prompt

    system_role = (
        "Rephrase the given prompt using natural, grammatically correct English. "
        "Introduce linguistic variance in style, tone, or word choice, "
        "while keeping the meaning identical."
    )

    try:
        response_text = call_argo_api(prompt, system_role)
        if response_text == "error":
            return 500, prompt
        return 200, response_text
    except Exception as e:
        print(f"[ERROR] linguistic_variance failed: {e}")
        return 500, prompt


# --- DATASET LOOP ---
def dataset_loop(target_location, generated_entries, progress_bar, TARGET_LIMIT):
    
    if len(generated_entries) >= TARGET_LIMIT:
        return

    chat_templates = parse_txt_templates(TEMPLATE_FILE)

    for location_str in [target_location]:
        county1, state1 = map(str.strip, location_str.split(","))
        
        for template in chat_templates:
            if len(generated_entries) >= TARGET_LIMIT:
                return

            raw_q = template["question"]
            raw_a = template["answer"]
            
            is_loc_comparison = "{Location~2}" in raw_q or "{Location~2}" in raw_a
            context_list = []

            if is_loc_comparison:
                for compare_str in comparison_locations[DATASET_TYPE]:
                    county2, state2 = map(str.strip, compare_str.split(","))
                    loc_data1 = climparser.query_mean(climate_df, county1, state1)
                    loc_data2 = climparser.query_mean(climate_df, county2, state2)
                    
                    ctx = {}
                    ctx.update(loc_data1)
                    for k, v in loc_data2.items():
                        ctx[f"{k}_loc2"] = v
                    ctx["Location"] = location_str
                    ctx["Location~1"] = location_str
                    ctx["Location~2"] = compare_str
                    context_list.append((ctx, compare_str))
            else:
                loc_data = climparser.query_center(climate_df, county1, state1)
                loc_data["Location"] = location_str
                context_list.append((loc_data, None))

            for context, compare_str in context_list:
                
                # --- Metadata Inference (FIXED) ---
                vars_in_template = re.findall(r'\{([^{}]+)\}', raw_q + raw_a)
                
                primary_key = None
                # Iterate through variables found in template
                for k in vars_in_template:
                    # Strip suffix like ~1 or ~2 to check against raw data keys
                    base_k = k.split("~")[0]
                    # Check if this base key exists in our data context
                    if base_k in context and base_k not in ["Location", "Location_1", "Location_2", "compared_location"]:
                        primary_key = base_k
                        break
                
                if primary_key:
                    metadata = infer_metadata(primary_key)
                    context.update(metadata)
                    for m_key, m_val in metadata.items():
                        context[f"{m_key}~1"] = m_val
                        context[f"{m_key}~2"] = m_val
                        context[f"{m_key}"] = m_val

                # Handle Literal Placeholders (like "Mid-Century RCP4.5")
                for var in vars_in_template:
                    # Check if var is not in context but looks like a label
                    if var not in context and ("RCP" in var or "Century" in var):
                        context[var] = var
                        if "~" in var:
                            base_var = var.split("~")[0]
                            context[var] = base_var

                # --- Handle Comparison Logic (Pre-processing) ---
                temp_q = raw_q
                temp_a = raw_a
                
                if compare_str:
                    # Important: Update Regex to allow spaces/dots in the match group [^{}\|]+
                    def loc2_sub(m):
                        return f"{{COMPARE|{m.group(1)}|{m.group(2)}_loc2}}"
                    
                    temp_a = re.sub(r'\{COMPARE\|([^{}\|]+)\|(\1)\}', loc2_sub, temp_a)

                    if "{Location~2} of {" in temp_a:
                        cmp_match = re.search(r'\{COMPARE\|([^{}\|]+)\|', temp_a)
                        if cmp_match:
                            var_name = cmp_match.group(1)
                            last_idx = temp_a.rfind(f"{{{var_name}}}")
                            if last_idx != -1:
                                temp_a = temp_a[:last_idx] + f"{{{var_name}_loc2}}" + temp_a[last_idx+len(f"{{{var_name}}}"):]
                
                try:
                    # 1. Sanitize Keys & Placeholders (Handles ., ~, and spaces)
                    clean_q_str, clean_ctx = sanitize_formatting(temp_q, context)
                    clean_a_str, _ = sanitize_formatting(temp_a, context)
                    
                    # 2. Run Computation (Replaces {COMPARE...} and {PERCENTAGE...})
                    final_q = process_custom_logic(clean_q_str, clean_ctx)
                    final_a = process_custom_logic(clean_a_str, clean_ctx)
                    
                    # 3. Format Strings (Fills remaining {Placeholders})
                    final_q = final_q.format(**clean_ctx)
                    final_a = final_a.format(**clean_ctx)

                    # Prepare Primary Location Data
                    primary_data = {
                        k: v for k, v in context.items() 
                        if k in climate_df.columns 
                        and k not in ignored_climrr_keys
                        and "_loc2" not in k
                    }

                    request_body = {
                        "climate_data": primary_data,
                        "output_type": "json",
                        "location_name": location_str
                    }

                    # API Call 1: Primary Location
                    format_data_request = requests.post(
                        FORMAT_DATA_ENDPOINT,
                        data=json.dumps(request_body),
                        headers={"Content-Type": "application/json"},
                    )

                    inputs = []

                    if compare_str:
                        # Prepare Secondary Location Data
                        secondary_data = {
                            k.replace("_loc2", ""): v for k, v in context.items() 
                            if "_loc2" in k 
                            and k.replace("_loc2", "") in climate_df.columns
                            and k.replace("_loc2", "") not in ignored_climrr_keys
                        }

                        request_body_loc2 = {
                            "climate_data": secondary_data,
                            "output_type": "json",
                            "location_name": compare_str
                        }

                        # API Call 2: Comparison Location
                        format_data_request_loc2 = requests.post(
                            FORMAT_DATA_ENDPOINT,
                            data=json.dumps(request_body_loc2),
                            headers={"Content-Type": "application/json"},
                        )

                        # Combine both API responses
                        inputs = [format_data_request.json(), format_data_request_loc2.json()]
                    else:
                        # Single input response
                        inputs = [format_data_request.json()]

                    # --- Linguistic Variance ---
                    _, final_q = linguistic_variance(final_q)
                    _, final_a = linguistic_variance(final_a)

                    generated_entries.append({
                        "user": final_q,
                        "input": inputs,
                        "assistant": final_a
                    })
                    
                    progress_bar()

                except KeyError as e:
                    # Optional: uncomment to debug missing keys
                    # print(f"[ERROR] Missing placeholder in context: {e}")
                    pass
                except Exception as e:
                    print(f"Error processing template: {e}")

if __name__ == "__main__":
    manager = Manager()
    generated_entries = manager.list()

    raw_templates = parse_txt_templates(TEMPLATE_FILE)
    if not raw_templates:
        print("No templates found. Exiting.")
        exit()

    TARGET_LIMIT = 10000
    target_subset = target_locations[DATASET_TYPE][:25]

    print(f"Running test generation for max {TARGET_LIMIT} entries...")

    with alive_bar(TARGET_LIMIT, title="Processing Datasets (Test Run)") as progress_bar:
        Parallel(n_jobs=1, backend="threading")(
            delayed(dataset_loop)(target_location, generated_entries, progress_bar, TARGET_LIMIT)
            for target_location in target_subset
        )

    print(f"\nFinished. Generated {len(generated_entries)} entries.")
    output_filename = f"dataset_final/ClimRR_Dataset_{DATASET_TYPE}_Queries_new_n_final.json"
    templater.append_entries(output_filename, list(generated_entries))
    print(f"Saved prompts to {output_filename}")