templates = [
    # --- Temperature Maximum - Annual ---
    {
        "user": "What is the projected annual max temperature at {grid} by the end of the century?",
        "assistant": "At grid {grid}, the annual maximum temperature is projected to reach {temp}°F under RCP 8.5 by the end of the century.",
    },
    {
        "user": "How does the max annual temperature increase at {grid} compare to nearby grid cells?",
        "assistant": "Grid {grid} is projected to warm by {change}°F, placing it in the {state_percentile} percentile within the state for temperature increases.",
    },
    # --- Temperature Minimum - Seasonal ---
    {
        "user": "What is the projected change in winter minimum temperature at {grid}?",
        "assistant": "The winter minimum temperature at {grid} is expected to increase by {change}°F, which is considered {relative_value} relative to the rest of the state.",
    },
    {
        "user": "How does the projected seasonal min temperature change at {grid} compare nationally?",
        "assistant": "Grid {grid} ranks in the {national_percentile} percentile for minimum seasonal temperature rise under RCP 8.5.",
    },
    # --- Precipitation – Annual Total ---
    {
        "user": "What is the projected change in annual precipitation at {grid}?",
        "assistant": "Annual precipitation at {grid} is projected to increase by {change} inches, with variability depending on the scenario.",
    },
    {
        "user": "Compare total annual rainfall at {grid} to the rest of the state.",
        "assistant": "With {value} inches of projected rainfall, grid {grid} falls in the {state_percentile} percentile compared to other areas in the state.",
    },
    # --- Precipitation None – Annual Average ---
    {
        "user": "How many dry days per year are projected for {grid}?",
        "assistant": "Grid {grid} is projected to have an average of {dry_days} dry days annually.",
    },
    # --- Wind Speed – Annual Average ---
    {
        "user": "What is the expected average wind speed at {grid}?",
        "assistant": "The projected average wind speed at grid {grid} is {value} mph.",
    },
    {
        "user": "How does wind speed at {grid} compare to nearby areas?",
        "assistant": "Grid {grid} falls in the {state_percentile} percentile for wind speed, indicating it is {wind_trend} windier than most grid cells in the state.",
    },
    # --- Cooling Degree Days – Annual Total ---
    {
        "user": "How are cooling needs changing at {grid}?",
        "assistant": "Cooling degree days at grid {grid} are expected to change by {change}.",
    },
    # --- Heating Degree Days – Annual Total ---
    {
        "user": "How is the need for winter heating projected to change at {grid}?",
        "assistant": "Heating degree days are projected to changed at {grid}, with a total of {change_days} days annually.",
    },
    # --- Fire Weather Index – Averages ---
    {
        "user": "What is the projected fire weather index at {grid}?",
        "assistant": "The average fire weather index (FWI) at grid {grid} is projected to be {value}, placing it in the {state_percentile} percentile statewide.",
    },
    {
        "user": "How does fire risk at {grid} compare to national trends?",
        "assistant": "Grid {grid} has an FWI percentile of {national_percentile} under RCP 8.5, indicating {risk_description} relative fire weather risk nationally.",
    },
    # --- Fire Weather Index – FWI Classes ---
    {
        "user": "How frequently does {grid} experience extreme fire conditions?",
        "assistant": "Grid {grid} is projected to experience {value} days per year in the 'Extreme' FWI class by the end of the century.",
    },
    # --- Heat Index – Summer Seasonal ---
    {
        "user": "What will summer feel like at {grid} in the future?",
        "assistant": "The summer heat index at {grid} is projected to rise to {value}°F, with more frequent days exceeding heat advisory thresholds.",
    },
    {
        "user": "How is summer heat stress expected to change at {grid}?",
        "assistant": "Summer heat stress is expected to worsen at grid {grid}, with the seasonal heat index increasing by {change}°F under RCP 8.5.",
    },
    # --- Relative Change Interpretation (with risk framing) ---
    {
        "user": "How much has the number of days above 95°F changed at {grid} from historical to mid-century?",
        "assistant": "Grid {grid} is projected to experience an increase from {hist_days} to {mid_days} days above 95°F by mid-century, a {percent_change}% increase.",
    },
    # --- Cross-Region Comparison ---
    {
        "user": "How does the heat risk at {grid} compare to other areas in the state?",
        "assistant": "Grid {grid} ranks in the top {state_percentile}% for days above 95°F.",
    },
    {
        "user": "How does wind speed at {grid} compare to other regions?",
        "assistant": "Grid {grid} has {relative_value} wind speeds than {comparison_region}.",
    },
    # --- Neighborhood/Adjacency Context ---
    {
        "user": "How does projected warming at {grid} compare to surrounding grid cells?",
        "assistant": "Grid {grid} has a projected maximum temperature increase of {relative_change}°F, relative to neighboring cells.",
    },
    # --- State/Regional Summary & Extremes ---
    {
        "user": "Which state shows the largest projected increase in fire weather index?",
        "assistant": "Among all states, {state} shows the highest average projected increase in fire weather index under RCP 8.5, with values rising by {value}, reflecting significantly elevated wildfire risk.",
    },
]
