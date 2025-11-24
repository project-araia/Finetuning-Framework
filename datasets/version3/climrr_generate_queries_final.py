import csv
import io
import os
import pandas as pd
from itertools import combinations, product

# Define the filename of the matrix Excel file.
# MATRIX_FILENAME = "ClimRR_Variable_Matrix_clean.xlsx"

# The matrix content is hardcoded here to remove external file dependency.
MATRIX_CSV_CONTENT = """
Climate Variable,Data Availability Class,Cumulative Measure,"Annual, Historical","Annual, Mid-Century RCP4.5","Annual, Mid-Century RCP8.5","Annual, End-Century RCP4.5","Annual, End-Century RCP8.5","Winter, Historical","Winter, Mid-Century RCP4.5","Winter, Mid-Century RCP8.5","Winter, End-Century RCP4.5","Winter, End-Century RCP8.5","Spring, Historical","Spring, Mid-Century RCP4.5","Spring, Mid-Century RCP8.5","Spring, End-Century RCP4.5","Spring, End-Century RCP8.5","Summer, Historical","Summer, Mid-Century RCP4.5","Summer, Mid-Century RCP8.5","Summer, End-Century RCP4.5","Summer, End-Century RCP8.5","Autumn, Historical","Autumn, Mid-Century RCP4.5","Autumn, Mid-Century RCP8.5","Autumn, End-Century RCP4.5","Autumn, End-Century RCP8.5"
Heating Degree Days,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1
Cooling Degree Days,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1
Average Maximum Temperature,2,,1,1,1,1,1,1,,1,,1,1,,1,,1,1,,1,,1,1,,1,,1
Average Minimum Temperature,2,,1,1,1,1,1,1,,1,,1,1,,1,,1,1,,1,,1,1,,1,,1
Wind Speed,3,,1,1,1,1,1,,,,,,,,,,,,,,,,,,,,
Fire Weather Index (95th percentile),4,,1,,1,,1,1,,1,,1,1,,1,,1,1,,1,,1,1,,1,,1
Fire Weather Index (Average),5,,,,,,,1,,1,,1,1,,1,,1,1,,1,,1,1,,1,,1
Fire Weather Index (Class),4,,1,,1,,1,1,,1,,1,1,,1,1,1,1,1,1,1,1,1,1,1,1,1
WBGT Mean Daily Maximum,6,,,,,,,,,,,,,,,,,1,1,1,,,,,,,
WBGT Daytime Hours above 78F,6,1,,,,,,,,,,,,,,,,1,1,1,,,,,,,
WBGT Daytime Hours above 82F,6,1,,,,,,,,,,,,,,,,1,1,1,,,,,,,
WBGT Daytime Hours above 86F,6,1,,,,,,,,,,,,,,,,1,1,1,,,,,,,
WBGT Days above 78F,6,1,,,,,,,,,,,,,,,,1,1,1,,,,,,,
WBGT Days above 82F,6,1,,,,,,,,,,,,,,,,1,1,1,,,,,,,
WBGT Days above 86F,6,1,,,,,,,,,,,,,,,,1,1,1,,,,,,,
Heat Index Days above 80F,7,1,,,,,,,,,,,1,1,1,,,1,1,1,,1,1,1,1,,
Heat Index Days above 95F,8,1,,,,,,,,,,,1,1,1,,,1,1,1,,1,1,1,1,,
Heat Index Days above 105F,8,1,,,,,,,,,,,1,1,1,,,1,1,1,,1,1,1,1,,
Heat Index Days above 115F,8,1,,,,,,,,,,,1,1,1,,,1,1,1,,1,1,1,1,,
Heat Index Days above 125F,8,1,,,,,,,,,,,1,1,1,,,1,1,1,,1,1,1,1,,
Daily Max Heat Index,8,,,,,,,,,,,,1,1,1,,,1,1,1,,1,1,1,1,,
Seasonal Max Heat Index,8,,,,,,,,,,,,1,1,1,,,1,1,1,,1,1,1,1,,
Days without Precipitation,3,1,1,1,1,1,1,,,,,,,,,,,,,,,,,,,,
Total Precipitation,3,1,1,1,1,1,1,,,,,,,,,,,,,,,,,,,,
Average Daily Precipitation,5,,,,,,,1,,1,,1,1,,1,,1,1,,1,,1,1,,1,,1
Maximum Daily Precipitation,5,,,,,,,1,,1,,1,1,,1,1,1,1,1,1,1,1,1,1,1,1,1
"""

def get_final_data_key(variable_name, seasonality, scenario_part):
    """
    Translates the combination of (Variable Name, Seasonality, Scenario) into 
    the exact column name used in the FullData.csv (e.g., 'hdd_hist', 'FWIBins_MidSum_95').
    
    This function returns the raw key without the location/value suffix.
    """
    # Map (Long Variable Name, Seasonality, Scenario) to the EXACT Data Column Name
    FULL_KEY_MAP = {
        # ------------------ SIMPLE ANNUALS (hdd, cdd, noprecip, precipann, windspeed) ------------------
        ("Heating Degree Days", "Annual", "Historical"): "hdd_hist",
        ("Heating Degree Days", "Annual", "Mid-Century RCP8.5"): "hdd_rcp85_midc",
        ("Cooling Degree Days", "Annual", "Historical"): "cdd_hist",
        ("Cooling Degree Days", "Annual", "Mid-Century RCP8.5"): "cdd_rcp85_midc",
        ("Days without Precipitation", "Annual", "Historical"): "noprecip_hist",
        ("Days without Precipitation", "Annual", "Mid-Century RCP4.5"): "noprecip_rcp45_midc",
        ("Days without Precipitation", "Annual", "End-Century RCP4.5"): "noprecip_rcp45_endc",
        ("Days without Precipitation", "Annual", "Mid-Century RCP8.5"): "noprecip_rcp85_midc",
        ("Days without Precipitation", "Annual", "End-Century RCP8.5"): "noprecip_rcp85_endc",
        ("Total Precipitation", "Annual", "Historical"): "precipann_hist",
        ("Total Precipitation", "Annual", "Mid-Century RCP4.5"): "precipann_rcp45_midc",
        ("Total Precipitation", "Annual", "End-Century RCP4.5"): "precipann_rcp45_endc",
        ("Total Precipitation", "Annual", "Mid-Century RCP8.5"): "precipann_rcp85_midc",
        ("Total Precipitation", "Annual", "End-Century RCP8.5"): "precipann_rcp85_endc",
        ("Wind Speed", "Annual", "Historical"): "windspeed_hist",
        ("Wind Speed", "Annual", "Mid-Century RCP4.5"): "windspeed_rcp45_midc",
        ("Wind Speed", "Annual", "End-Century RCP4.5"): "windspeed_rcp45_endc",
        ("Wind Speed", "Annual", "Mid-Century RCP8.5"): "windspeed_rcp85_midc",
        ("Wind Speed", "Annual", "End-Century RCP8.5"): "windspeed_rcp85_endc",
        
        # ------------------ TEMP MAX/MIN ------------------
        # Annual
        ("Average Maximum Temperature", "Annual", "Historical"): "tempmaxann_hist",
        ("Average Maximum Temperature", "Annual", "Mid-Century RCP4.5"): "tempmaxann_rcp45_midc",
        ("Average Maximum Temperature", "Annual", "End-Century RCP4.5"): "tempmaxann_rcp45_endc",
        ("Average Maximum Temperature", "Annual", "Mid-Century RCP8.5"): "tempmaxann_rcp85_midc",
        ("Average Maximum Temperature", "Annual", "End-Century RCP8.5"): "tempmaxann_rcp85_endc",
        ("Average Minimum Temperature", "Annual", "Historical"): "tempminann_hist",
        ("Average Minimum Temperature", "Annual", "Mid-Century RCP4.5"): "tempminann_rcp45_midc",
        ("Average Minimum Temperature", "Annual", "End-Century RCP4.5"): "tempminann_rcp45_endc",
        ("Average Minimum Temperature", "Annual", "Mid-Century RCP8.5"): "tempminann_rcp85_midc",
        ("Average Minimum Temperature", "Annual", "End-Century RCP8.5"): "tempminann_rcp85_endc",
        # Seasonal (Note: Spring and Autumn use truncated spelling in data keys)
        # Winter
        ("Average Maximum Temperature", "Winter", "Historical"): "tempmax_seas_hist_winter",
        ("Average Maximum Temperature", "Winter", "Mid-Century RCP8.5"): "tempmax_seas_rcp85_midc_winter",
        ("Average Maximum Temperature", "Winter", "End-Century RCP8.5"): "tempmax_seas_rcp85_endc_winter",
        ("Average Minimum Temperature", "Winter", "Historical"): "tempmin_seas_hist_winter",
        ("Average Minimum Temperature", "Winter", "Mid-Century RCP8.5"): "tempmin_seas_rcp85_midc_winter",
        ("Average Minimum Temperature", "Winter", "End-Century RCP8.5"): "tempmin_seas_rcp85_endc_winter",
        # Spring
        ("Average Maximum Temperature", "Spring", "Historical"): "tempmax_seas_hist_spring",
        ("Average Maximum Temperature", "Spring", "Mid-Century RCP8.5"): "tempmax_seas_rcp85_mid_spring",
        ("Average Maximum Temperature", "Spring", "End-Century RCP8.5"): "tempmax_seas_rcp85_end_spring",
        ("Average Minimum Temperature", "Spring", "Historical"): "tempmin_seas_hist_spring",
        ("Average Minimum Temperature", "Spring", "Mid-Century RCP8.5"): "tempmin_seas_rcp85_mid_spring",
        ("Average Minimum Temperature", "Spring", "End-Century RCP8.5"): "tempmin_seas_rcp85_end_spring",
        # Summer
        ("Average Maximum Temperature", "Summer", "Historical"): "tempmax_seas_hist_summer",
        ("Average Maximum Temperature", "Summer", "Mid-Century RCP8.5"): "tempmax_seas_rcp85_mid_summer",
        ("Average Maximum Temperature", "Summer", "End-Century RCP8.5"): "tempmax_seas_rcp85_end_summer",
        ("Average Minimum Temperature", "Summer", "Historical"): "tempmin_seas_hist_summer",
        ("Average Minimum Temperature", "Summer", "Mid-Century RCP8.5"): "tempmin_seas_rcp85_mid_summer",
        ("Average Minimum Temperature", "Summer", "End-Century RCP8.5"): "tempmin_seas_rcp85_end_summer",
        # Autumn
        ("Average Maximum Temperature", "Autumn", "Historical"): "tempmax_seas_hist_autum",
        ("Average Maximum Temperature", "Autumn", "Mid-Century RCP8.5"): "tempmax_seas_rcp85_mid_autumn",
        ("Average Maximum Temperature", "Autumn", "End-Century RCP8.5"): "tempmax_seas_rcp85_end_autumn",
        ("Average Minimum Temperature", "Autumn", "Historical"): "tempmin_seas_hist_autum",
        ("Average Minimum Temperature", "Autumn", "Mid-Century RCP8.5"): "tempmin_seas_rcp85_mid_autumn",
        ("Average Minimum Temperature", "Autumn", "End-Century RCP8.5"): "tempmin_seas_rcp85_end_autumn",
        
        # ------------------ HEAT INDEX (Annual Only in provided keys) ------------------
        # Keys use HIS/M85/E85 for Historical/Mid-RCP8.5/End-RCP8.5
        ("Heat Index Days above 80F", "Annual", "Historical"): "heatindex_HIS_Day80",
        ("Heat Index Days above 80F", "Annual", "Mid-Century RCP8.5"): "heatindex_M85_Day80",
        ("Heat Index Days above 80F", "Annual", "End-Century RCP8.5"): "heatindex_E85_Day80",
        ("Heat Index Days above 95F", "Annual", "Historical"): "heatindex_HIS_Day95",
        ("Heat Index Days above 95F", "Annual", "Mid-Century RCP8.5"): "heatindex_M85_Day95",
        ("Heat Index Days above 95F", "Annual", "End-Century RCP8.5"): "heatindex_E85_Day95",
        ("Heat Index Days above 105F", "Annual", "Historical"): "heatindex_HIS_Day105",
        ("Heat Index Days above 105F", "Annual", "Mid-Century RCP8.5"): "heatindex_M85_Day105",
        ("Heat Index Days above 105F", "Annual", "End-Century RCP8.5"): "heatindex_E85_Day105",
        ("Heat Index Days above 115F", "Annual", "Historical"): "heatindex_HIS_Day115",
        ("Heat Index Days above 115F", "Annual", "Mid-Century RCP8.5"): "heatindex_M85_Day115",
        ("Heat Index Days above 115F", "Annual", "End-Century RCP8.5"): "heatindex_E85_Day115",
        ("Heat Index Days above 125F", "Annual", "Historical"): "heatindex_HIS_Day125",
        ("Heat Index Days above 125F", "Annual", "Mid-Century RCP8.5"): "heatindex_M85_Day125",
        ("Heat Index Days above 125F", "Annual", "End-Century RCP8.5"): "heatindex_E85_Day125",
        ("Daily Max Heat Index", "Annual", "Historical"): "heatindex_HIS_DayMax",
        ("Daily Max Heat Index", "Annual", "Mid-Century RCP8.5"): "heatindex_M85_DayMax",
        ("Daily Max Heat Index", "Annual", "End-Century RCP8.5"): "heatindex_E85_DayMax",
        ("Seasonal Max Heat Index", "Annual", "Historical"): "heatindex_HIS_SeaMax",
        ("Seasonal Max Heat Index", "Annual", "Mid-Century RCP8.5"): "heatindex_M85_SeaMax",
        ("Seasonal Max Heat Index", "Annual", "End-Century RCP8.5"): "heatindex_E85_SeaMax",

        # ------------------ FWI BINS (Seasonal Only) ------------------
        # FWI (95th percentile) -> uses _95 suffix | FWI (Class) -> uses _NC suffix
        # Keys use Hist/Mid/End and season abbreviations (Win/Spr/Sum/Aut)
        
        # Winter
        ("Fire Weather Index (95th percentile)", "Winter", "Historical"): "FWIBins_HistWin_95",
        ("Fire Weather Index (95th percentile)", "Winter", "Mid-Century RCP8.5"): "FWIBins_MidWin_95",
        ("Fire Weather Index (95th percentile)", "Winter", "End-Century RCP8.5"): "FWIBins_EndWin_95",
        ("Fire Weather Index (Class)", "Winter", "Historical"): "FWIBins_HistWin_NC",
        ("Fire Weather Index (Class)", "Winter", "Mid-Century RCP8.5"): "FWIBins_MidWin_NC",
        ("Fire Weather Index (Class)", "Winter", "End-Century RCP8.5"): "FWIBins_EndWin_NC",

        # Spring
        ("Fire Weather Index (95th percentile)", "Spring", "Historical"): "FWIBins_HistSpr_95",
        ("Fire Weather Index (95th percentile)", "Spring", "Mid-Century RCP8.5"): "FWIBins_MidSpr_95",
        ("Fire Weather Index (95th percentile)", "Spring", "End-Century RCP8.5"): "FWIBins_EndSpr_95",
        ("Fire Weather Index (Class)", "Spring", "Historical"): "FWIBins_HistSpr_NC",
        ("Fire Weather Index (Class)", "Spring", "Mid-Century RCP8.5"): "FWIBins_MidSpr_NC",
        ("Fire Weather Index (Class)", "Spring", "End-Century RCP8.5"): "FWIBins_EndSpr_NC",

        # Summer
        ("Fire Weather Index (95th percentile)", "Summer", "Historical"): "FWIBins_HistSum_95",
        ("Fire Weather Index (95th percentile)", "Summer", "Mid-Century RCP8.5"): "FWIBins_MidSum_95",
        ("Fire Weather Index (95th percentile)", "Summer", "End-Century RCP8.5"): "FWIBins_EndSum_95",
        ("Fire Weather Index (Class)", "Summer", "Historical"): "FWIBins_HistSum_NC",
        ("Fire Weather Index (Class)", "Summer", "Mid-Century RCP8.5"): "FWIBins_MidSum_NC",
        ("Fire Weather Index (Class)", "Summer", "End-Century RCP8.5"): "FWIBins_EndSum_NC",

        # Autumn
        ("Fire Weather Index (95th percentile)", "Autumn", "Historical"): "FWIBins_HistAut_95",
        ("Fire Weather Index (95th percentile)", "Autumn", "Mid-Century RCP8.5"): "FWIBins_MidAut_95",
        ("Fire Weather Index (95th percentile)", "Autumn", "End-Century RCP8.5"): "FWIBins_EndAut_95",
        ("Fire Weather Index (Class)", "Autumn", "Historical"): "FWIBins_HistAut_NC",
        ("Fire Weather Index (Class)", "Autumn", "Mid-Century RCP8.5"): "FWIBins_MidAut_NC",
        ("Fire Weather Index (Class)", "Autumn", "End-Century RCP8.5"): "FWIBins_EndAut_NC",
        
        # Note: All WBGT, FWI (Average), and daily precipitation annual/RCP4.5 keys 
        # are intentionally omitted as they do not have a direct match in the provided list.
    }
    
    exact_column_name = FULL_KEY_MAP.get((variable_name, seasonality, scenario_part))
    
    if exact_column_name:
        return exact_column_name
    
    return None

def generate_climate_templates():
    """
    Generates all six types of climate query and answer templates (single-value
    and five comparison types) based on ClimRR_Variable_Matrix_clean.xlsv file.
    """
    
    generated_pairs = []
    valid_combinations = [] # Stores all valid single combinations from the matrix

    # Define the headers that correspond to the data combinations (from ClimRR_Variable_Matrix_clean.xlsv)
    HEADERS_TO_CHECK = [
        "Annual, Historical", 
        "Annual, Mid-Century RCP4.5", "Annual, Mid-Century RCP8.5",
        "Annual, End-Century RCP4.5", "Annual, End-Century RCP8.5",
        "Winter, Historical", 
        "Winter, Mid-Century RCP4.5", "Winter, Mid-Century RCP8.5",
        "Winter, End-Century RCP4.5", "Winter, End-Century RCP8.5",
        "Spring, Historical", 
        "Spring, Mid-Century RCP4.5", "Spring, Mid-Century RCP8.5",
        "Spring, End-Century RCP4.5", "Spring, End-Century RCP8.5",
        "Summer, Historical", 
        "Summer, Mid-Century RCP4.5", "Summer, Mid-Century RCP8.5",
        "Summer, End-Century RCP4.5", "Summer, End-Century RCP8.5",
        "Autumn, Historical", 
        "Autumn, Mid-Century RCP4.5", "Autumn, Mid-Century RCP8.5",
        "Autumn, End-Century RCP4.5", "Autumn, End-Century RCP8.5"
    ]

    # --- Read data from hardcoded CSV string ---
    try:
        # Use io.StringIO to treat the string as a file, then read with pandas
        data = io.StringIO(MATRIX_CSV_CONTENT)
        df = pd.read_csv(data)
        
        for _, row_series in df.iterrows():
            row = row_series.to_dict()
            variable_name = str(row.get("Climate Variable")).strip()

            if not variable_name:
                continue
                
            for header in HEADERS_TO_CHECK:
                # print("header:", header)
                value = row.get(header)
                # print("value:", value)
                # This check ensures we only proceed if the cell contains '1'
                if pd.isna(value) or (str(value).strip() != '1' and (not isinstance(value, (int, float)) or value != 1)):
                    # print(EARLY_STOP)
                    continue
                
                parts = header.split(", ")
                seasonality = parts[0].strip()
                scenario_part = parts[1].strip()

                raw_key = get_final_data_key(variable_name, seasonality, scenario_part)
                # print("raw_key:", raw_key)
                
                if not raw_key:
                    continue 
                    
                time_period = "historical" if "Historical" in scenario_part else "projected"
                rcp_scenario = scenario_part if time_period == "projected" else None
                
                combo = {
                    'variable_name': variable_name,
                    'seasonality': seasonality,
                    'scenario_part': scenario_part,
                    'time_period': time_period,
                    'rcp_scenario': rcp_scenario,
                    'raw_key': raw_key
                }
                # print(combo)
                valid_combinations.append(combo)

    except Exception as e:
        # Catch errors related to pandas/data parsing now that file IO is handled
        print(f"An error occurred during matrix data processing: {e}")
        return []

    # print("valid_combinations:", valid_combinations)
    # print(STOP)

    # --- 1. Type 0: Single Value Lookup (Original Functionality) ---
    generated_pairs.append(("--- TYPE 0: SINGLE VALUE LOOKUP ---", ""))
    
    for combo in valid_combinations:
        raw_key = combo['raw_key']
        
        # Placeholder key uses Location~1 as the default
        value_placeholder = f"{raw_key}"

        season = combo['seasonality']
        varname = combo['variable_name']
        
        # Q: What is the historical Annual Average Maximum Temperature in {Location}?
        # Q: What is the projected Annual Average Maximum Temperature in {Location} under Mid-Century RCP4.5?
        
        if combo['time_period'] == "historical":
            query = f"What is the historical {season} {varname} in {{Location}}?"
            answer = (
                f"The {{TimePeriod}} {{Seasonality}} {{ClimateVariable}} in {{Location}} is "
                f"{{{value_placeholder}}}."
            )
        else:
            query = f"What is the projected {season} {varname} in {{Location}} under {combo['rcp_scenario']}?"
            answer = (
                f"The {{TimePeriod}} {{Seasonality}} {{ClimateVariable}} in {{Location}} "
                f"under {{RCPscenario}} is {{{value_placeholder}}}."
            )
            
        generated_pairs.append((query, answer))


    # --- 2. Type 1: Data Comparison between Time Periods (Hist vs. Projected) ---
    generated_pairs.append(("\n--- TYPE 1: TIME PERIOD COMPARISON (Hist vs. Projected) ---", ""))

    for combo1 in valid_combinations:
        for combo2 in valid_combinations:
            # Must be the same variable and seasonality
            if combo1['variable_name'] != combo2['variable_name'] or \
               combo1['seasonality'] != combo2['seasonality']:
                continue

            # Must be one historical and one projected
            if combo1['time_period'] == combo2['time_period']:
                continue
            
            # C1 is the first mentioned element, C2 is the element being compared against.
            # We must ensure the question asks for a comparison of different time periods.
            C1, C2 = combo1, combo2 

            season = C1["seasonality"]
            varname = C1["variable_name"]

            # Raw keys for placeholders
            k1 = f"{C1['raw_key']}"
            k2 = f"{C2['raw_key']}"
            
            # Generate the Question based on the C1 (TimePeriod~1) structure
            if C1['time_period'] == 'historical':
                # Q: How does the historical Annual TempMax in {Location} compare to projected Mid-Century RCP4.5?
                question = (
                    f"How does the historical {season} {varname} in {{Location}} "
                    f"compare to the projected {C2['scenario_part']}?"
                )
                ans_rcp1_text = ""
                ans_rcp2_text = f" under {{RCPscenario~2}}"
            else: # C1 is projected
                # Q: How does the projected Mid-Century RCP4.5 Annual TempMax in {Location} compare to historical?
                question = (
                    f"How does the projected {season} {varname} in {{Location}} "
                    f"under {C1['rcp_scenario']} compare to the historical?"
                )
                ans_rcp1_text = f" under {{RCPscenario~1}}"
                ans_rcp2_text = ""
            
            # The NLP engine must process the COMPARE instruction and keys
            answer = (
                f"The {{TimePeriod~1}} {{Seasonality}} {{ClimateVariable}} in {{Location}}{ans_rcp1_text} is "
                f"{{{k1}}}, which is {{COMPARE|{k1}|{k2}}} than the {{TimePeriod~2}} {{Seasonality}} {{ClimateVariable}} "
                f"{ans_rcp2_text} of {{{k2}}}."
            )
            
            generated_pairs.append((question, answer))


    # --- 3. Type 2: Data Comparison between RCP Scenarios (TimePeriod ≠ “historical”) ---
    generated_pairs.append(("\n--- TYPE 2: RCP SCENARIO COMPARISON (Projected Only) ---", ""))

    # Group valid combos by (Variable, Seasonality, TimePeriod)
    # This allows us to find pairs that only differ by RCP
    projection_groups = {}
    for combo in valid_combinations:
        if combo['time_period'] == 'projected':
            key = (combo['variable_name'], combo['seasonality'], combo['scenario_part'].split()[0]) # (Var, Season, Mid/End-Century)
            if key not in projection_groups:
                projection_groups[key] = []
            projection_groups[key].append(combo)

    for key, combos in projection_groups.items():
        # RCP comparisons only happen between RCP4.5 and RCP8.5 for the same Time Period (Mid/End-Century)
        rcp45_combos = [c for c in combos if 'RCP4.5' in c['rcp_scenario']]
        rcp85_combos = [c for c in combos if 'RCP8.5' in c['rcp_scenario']]

        if rcp45_combos and rcp85_combos:
            C1, C2 = rcp45_combos[0], rcp85_combos[0] # Compare 4.5 vs 8.5

            season = C1["seasonality"]
            varname = C1["variable_name"]
            
            # Question: How does the projected Mid-Century Annual TempMax in {Location} under RCP4.5 compare to RCP8.5?
            question = (
                f"How does the projected {season} {varname} in {{Location}} "
                f"under {C1['rcp_scenario']} compare to {C2['rcp_scenario']}?"
            )

            # Answer placeholders
            k1 = f"{C1['raw_key']}"
            k2 = f"{C2['raw_key']}"
            
            # The NLP engine must process the COMPARE instruction and keys
            answer = (
                f"The projected {{TimePeriod}} {{Seasonality}} {{ClimateVariable}} in {{Location}} under "
                f"{{{C1['rcp_scenario']}}} is {{{k1}}}, which is {{COMPARE|{k1}|{k2}}} than the {{TimePeriod}} "
                f"{{Seasonality}} {{ClimateVariable}} under {{{C2['rcp_scenario']}}} of {{{k2}}}."
            )
            
            generated_pairs.append((question, answer))


    # --- 4. Type 3: Data Comparison between Seasonal/Annual Data ---
    generated_pairs.append(("\n--- TYPE 3: SEASONAL/ANNUAL COMPARISON (Same Scenario) ---", ""))
    
    # Group valid combos by (Variable, Scenario)
    scenario_groups = {}
    for combo in valid_combinations:
        key = (combo['variable_name'], combo['scenario_part'])
        if key not in scenario_groups:
            scenario_groups[key] = []
        scenario_groups[key].append(combo)
        
    # Iterate through each group to compare different seasonalities within that group
    for _, combos in scenario_groups.items():
        if len(combos) < 2:
            continue
            
        for C1, C2 in combinations(combos, 2):
            # Must be different seasonality
            if C1['seasonality'] == C2['seasonality']:
                continue

            season1 = C1["seasonality"]
            season2 = C2["seasonality"]
            varname = C1["variable_name"]
                
            # Raw keys for placeholders
            k1 = f"{C1['raw_key']}"
            k2 = f"{C2['raw_key']}"
            
            # Generate the Question based on the scenario type
            rcp_q_text = f" under {C1['rcp_scenario']}" if C1["time_period"] == "projected" else ""
            
            # Q: How does the historical Annual TempMax in {Location} compare to Winter?
            # Q: How does the projected Mid-Century RCP4.5 Annual TempMax in {Location} under Mid-Century RCP4.5 compare to Winter?
            question = (
                f"How does the {C1['time_period']} {season1} {varname} in {{Location}}{rcp_q_text} "
                f"compare to {season2}?"
            )

            # Generate the Answer based on the scenario type
            rcp_ans_text = f" under {{RCPscenario}}" if C1['time_period'] == 'projected' else ""

            # The NLP engine must process the COMPARE instruction and keys
            answer = (
                f"The {{TimePeriod}} {{Seasonality~1}} {{ClimateVariable}} in {{Location}}{rcp_ans_text} is {{{k1}}}, "
                f"which is {{COMPARE|{k1}|{k2}}} than the {{TimePeriod}} {{Seasonality~2}} {{ClimateVariable}} "
                f"{rcp_ans_text} of {{{k2}}}."
            )
            
            generated_pairs.append((question, answer))

    # --- 5. Type 4: Relative comparison of seasonal contribution to annual data (Cumulative variables only) ---
    generated_pairs.append(("\n--- TYPE 4: SEASONAL CONTRIBUTION TO ANNUAL (%) ---", ""))

    # Only Total Precipitation ("Total Precipitation") is treated as cumulative (Seasonal + Annual comparison)
    CUMULATIVE_VARIABLES = ["Total Precipitation", "Average Maximum Temperature", "Average Minimum Temperature"]

    for combo1, combo2 in product(valid_combinations, repeat=2):
        # C1 must be Annual, C2 must be a specific Season
        if combo1['variable_name'] not in CUMULATIVE_VARIABLES or \
           combo1['variable_name'] != combo2['variable_name'] or \
           combo1['scenario_part'] != combo2['scenario_part'] or \
           combo1['seasonality'] != 'Annual' or \
           combo2['seasonality'] == 'Annual':
            continue

        C1_Annual, C2_Season = combo1, combo2

        # Raw keys for placeholders
        k1 = f"{C1_Annual['raw_key']}" # Annual (Denominator)
        k2 = f"{C2_Season['raw_key']}" # Season (Numerator)

        varname = C1_Annual["variable_name"]
        season = C2_Season["seasonality"]

        # Question parts
        rcp_q_text = f" under {C1_Annual['rcp_scenario']}" if C1_Annual["time_period"] == "projected" else ""
        
        # Q: What percentage of the projected Mid-Century RCP8.5 annual Total Precipitation in {Location} occurs in Spring?
        question = (
            f"What percentage of the {C1_Annual['time_period']} annual {varname} in {{Location}}{rcp_q_text} "
            f"occurs in {season}?"
        )

        # Answer parts
        rcp_ans_text = f" under {{RCPscenario}}" if C1_Annual['time_period'] == 'projected' else ""

        # {{PERCENTAGE|Numerator|Denominator}} calculates [k2 / k1 * 100]
        answer = (
            f"The {{TimePeriod}} {{Seasonality~2}} {{ClimateVariable}} in {{Location}}{rcp_ans_text} is {{{k2}}}, which "
            f"accounts for {{PERCENTAGE|{k2}|{k1}}} percent of the annual {{ClimateVariable}} ({{{k1}}})."
        )
        
        generated_pairs.append((question, answer))


    # --- 6. Type 5: Data Comparison between Locations ---
    generated_pairs.append(("\n--- TYPE 5: LOCATION COMPARISON ---", ""))

    # Use combinations that have both RCP4.5 and RCP8.5 scenarios for projected comparison, plus Historical
    # This loop generates a template for every valid combination of (Variable, Seasonality, Scenario)
    for combo in valid_combinations:
        season = combo["seasonality"]
        varname = combo["variable_name"]
        # Question parts
        rcp_q_text = f" under {combo['rcp_scenario']}" if combo["time_period"] == "projected" else ""
        
        # Question: How does the historical Annual TempMax in {Location~1} compare to {Location~2}?
        question = (
            f"How does the {combo['time_period']} {season} {varname} in {{Location~1}}{rcp_q_text} "
            f"compare to {{Location~2}}?"
        )

        # Answer placeholders (different locations)
        k1 = f"{combo['raw_key']}"
        k2 = f"{combo['raw_key']}"
        
        # Answer parts
        rcp_ans_text = f" under {{RCPscenario}}" if combo['time_period'] == 'projected' else ""

        # The NLP engine must process the COMPARE instruction and keys
        answer = (
            f"The {{TimePeriod}} {{Seasonality}} {{ClimateVariable}} in {{Location~1}}{rcp_ans_text} is {{{k1}}}, "
            f"which is {{COMPARE|{k1}|{k2}}} than the {{TimePeriod}} {{Seasonality}} {{ClimateVariable}} "
            f"in {{Location~2}}{rcp_ans_text} of {{{k2}}}."
        )
        
        generated_pairs.append((question, answer))

    return generated_pairs

def save_to_file(pairs, filename="templates_climrr_queries.txt"):
    """Saves the generated query/answer pairs to a text file."""
    with open(filename, "w") as f:
        for q, a in pairs:
            # Handle the template section headers
            # print("q, a:", q, a)
            if q.startswith("--- TYPE") or q.startswith("\n--- TYPE"):
                f.write(f"\n{q}\n\n")
            elif a: # Only write if the answer is not empty
                f.write(f"Query: {q}\n")
                f.write(f"Answer: {a}\n")
                f.write("-" * 40 + "\n")

if __name__ == "__main__":
    pairs = generate_climate_templates()
    # print("pairs:", pairs)
    # print(STOP)
    save_to_file(pairs) 

    # Print a summary to console
    print(f"Successfully generated {len(pairs)} templates across all 6 types.")
    print(f"Full output saved to 'templates_1_new.txt'")
