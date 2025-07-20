import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# --- 1. Define Countries and Timeframe (Monthly) ---
countries = ['India', 'Australia', 'Germany', 'USA', 'UK', 'Canada']
start_year = 2010
end_year = 2025 # Inclusive, so data up to Dec 2025

# Create a list of all months in the specified range
dates = pd.date_range(start=f'{start_year}-01-01', end=f'{end_year}-12-31', freq='MS') # Month Start frequency

# Create an empty list to store data rows
data_rows = []

# --- 2. Define Country-Specific Baselines and Trends ---
# These values are designed for annual trends; monthly values will oscillate around these trends.
country_profiles = {
    'India': {
        'pop_base': 1200, 'pop_growth': 0.012,
        'gdp_base': 1500, 'gdp_growth': 0.06,
        'co2_base': 2000, 'co2_trend': -0.015, 'co2_start_decline_year': 2020, # Higher emissions, aiming for recent decline
        're_share_base': 0.1, 're_share_growth': 0.03,
        'solar_wind_growth': 0.3,
        'policy_base': 3, 'policy_growth': 0.2,
        'investment_base': 10, 'investment_growth': 0.25,
        'green_jobs_base': 100, 'green_jobs_growth': 0.15,
        'marketing_base': 0.05, 'marketing_growth': 0.15,
        'solar_seasonality': [0.8, 0.9, 1.0, 1.1, 1.2, 1.3, 1.2, 1.1, 1.0, 0.9, 0.8, 0.7], # Higher in summer (Northern Hemisphere)
        'wind_seasonality': [1.1, 1.2, 1.1, 1.0, 0.9, 0.8, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2], # Varies
        'energy_consumption_seasonality': [1.1, 1.0, 0.9, 0.9, 1.0, 1.1, 1.2, 1.2, 1.1, 1.0, 0.9, 1.0] # Higher in hot/cold months
    },
    'Australia': {
        'pop_base': 22, 'pop_growth': 0.015,
        'gdp_base': 1200, 'gdp_growth': 0.02,
        'co2_base': 400, 'co2_trend': -0.015, 'co2_start_decline_year': 2010,
        're_share_base': 0.15, 're_share_growth': 0.025,
        'solar_wind_growth': 0.2,
        'policy_base': 6, 'policy_growth': 0.1,
        'investment_base': 15, 'investment_growth': 0.15,
        'green_jobs_base': 50, 'green_jobs_growth': 0.1,
        'marketing_base': 0.5, 'marketing_growth': 0.1,
        'solar_seasonality': [1.2, 1.1, 1.0, 0.9, 0.8, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2, 1.3], # Higher in summer (Southern Hemisphere)
        'wind_seasonality': [0.9, 0.8, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2, 1.1, 1.0, 0.9, 0.8], # Varies
        'energy_consumption_seasonality': [1.2, 1.2, 1.1, 1.0, 0.9, 0.9, 1.0, 1.1, 1.2, 1.1, 1.0, 1.0] # Higher in hot/cold months
    },
    'Germany': {
        'pop_base': 80, 'pop_growth': 0.002,
        'gdp_base': 3500, 'gdp_growth': 0.015,
        'co2_base': 800, 'co2_trend': -0.03, 'co2_start_decline_year': 2010,
        're_share_base': 0.25, 're_share_growth': 0.04,
        'solar_wind_growth': 0.18,
        'policy_base': 8, 'policy_growth': 0.05,
        'investment_base': 30, 'investment_growth': 0.1,
        'green_jobs_base': 200, 'green_jobs_growth': 0.08,
        'marketing_base': 1.0, 'marketing_growth': 0.08,
        'solar_seasonality': [0.7, 0.8, 1.0, 1.2, 1.3, 1.4, 1.3, 1.2, 1.0, 0.9, 0.7, 0.6], # Strong summer peak
        'wind_seasonality': [1.3, 1.4, 1.2, 1.0, 0.8, 0.7, 0.6, 0.7, 0.9, 1.1, 1.3, 1.4], # Strong winter peak
        'energy_consumption_seasonality': [1.3, 1.2, 1.1, 1.0, 0.9, 0.8, 0.8, 0.9, 1.0, 1.1, 1.2, 1.3] # Higher in cold months
    },
    'USA': {
        'pop_base': 300, 'pop_growth': 0.007,
        'gdp_base': 15000, 'gdp_growth': 0.025,
        'co2_base': 5500, 'co2_trend': -0.025, 'co2_start_decline_year': 2010,
        're_share_base': 0.12, 're_share_growth': 0.035,
        'solar_wind_growth': 0.22,
        'policy_base': 5, 'policy_growth': 0.12,
        'investment_base': 50, 'investment_growth': 0.18,
        'green_jobs_base': 250, 'green_jobs_growth': 0.12,
        'marketing_base': 0.8, 'marketing_growth': 0.1,
        'solar_seasonality': [0.8, 0.9, 1.0, 1.1, 1.2, 1.3, 1.2, 1.1, 1.0, 0.9, 0.8, 0.7],
        'wind_seasonality': [1.1, 1.2, 1.1, 1.0, 0.9, 0.8, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2],
        'energy_consumption_seasonality': [1.2, 1.1, 1.0, 0.9, 0.9, 1.0, 1.1, 1.2, 1.1, 1.0, 0.9, 1.0]
    },
    'UK': {
        'pop_base': 60, 'pop_growth': 0.006,
        'gdp_base': 2500, 'gdp_growth': 0.01,
        'co2_base': 500, 'co2_trend': -0.035, 'co2_start_decline_year': 2010,
        're_share_base': 0.18, 're_share_growth': 0.045,
        'solar_wind_growth': 0.25,
        'policy_base': 7, 'policy_growth': 0.08,
        'investment_base': 20, 'investment_growth': 0.13,
        'green_jobs_base': 120, 'green_jobs_growth': 0.09,
        'marketing_base': 0.7, 'marketing_growth': 0.09,
        'solar_seasonality': [0.6, 0.7, 0.9, 1.1, 1.2, 1.3, 1.2, 1.1, 0.9, 0.8, 0.7, 0.6], # Lower solar, but still summer peak
        'wind_seasonality': [1.4, 1.3, 1.2, 1.0, 0.8, 0.7, 0.6, 0.7, 0.9, 1.1, 1.3, 1.4], # Strong winter peak
        'energy_consumption_seasonality': [1.3, 1.2, 1.1, 1.0, 0.9, 0.8, 0.8, 0.9, 1.0, 1.1, 1.2, 1.3]
    },
    'Canada': {
        'pop_base': 34, 'pop_growth': 0.01,
        'gdp_base': 1500, 'gdp_growth': 0.018,
        'co2_base': 600, 'co2_trend': -0.01, 'co2_start_decline_year': 2015,
        're_share_base': 0.20, 're_share_growth': 0.03,
        'solar_wind_growth': 0.19,
        'policy_base': 6.5, 'policy_growth': 0.09,
        'investment_base': 18, 'investment_growth': 0.14,
        'green_jobs_base': 70, 'green_jobs_growth': 0.11,
        'marketing_base': 0.6, 'marketing_growth': 0.09,
        'solar_seasonality': [0.7, 0.8, 1.0, 1.2, 1.3, 1.4, 1.3, 1.2, 1.0, 0.9, 0.7, 0.6],
        'wind_seasonality': [1.3, 1.4, 1.2, 1.0, 0.8, 0.7, 0.6, 0.7, 0.9, 1.1, 1.3, 1.4],
        'energy_consumption_seasonality': [1.4, 1.3, 1.2, 1.0, 0.9, 0.8, 0.8, 0.9, 1.0, 1.1, 1.2, 1.3] # Very high winter peak
    }
}

# --- 3. Data Generation Loop (Monthly) ---
for country in countries:
    profile = country_profiles[country]
    for date in dates:
        year = date.year
        month = date.month # 1-12
        
        row = {
            'Country': country,
            'Year': year,
            'Month': month,
            'Date': date.strftime('%Y-%m-%d') # For clearer date column
        }

        # Annual metrics, roughly constant per month within a year
        # Population (Millions) - Assume annual growth, constant per month
        row['Population_Millions'] = profile['pop_base'] * (1 + profile['pop_growth'])**(year - start_year) + np.random.normal(0, 0.5)
        
        # GDP (Billion USD) - Assume annual growth, constant per month
        row['GDP_Billion_USD'] = profile['gdp_base'] * (1 + profile['gdp_growth'])**(year - start_year) + np.random.normal(0, 10)
        row['GDP_Per_Capita_USD'] = (row['GDP_Billion_USD'] * 1000) / row['Population_Millions']

        # Environmental Impact (Annual trend, slight monthly noise)
        co2_modifier = 1
        if year >= profile['co2_start_decline_year']:
            co2_modifier = (1 + profile['co2_trend'])**(year - profile['co2_start_decline_year'])
        row['CO2_Emissions_Million_Tons'] = profile['co2_base'] * co2_modifier + np.random.normal(0, 0.5) # Reduced noise for monthly
        row['CO2_Emissions_Per_Capita_Tons'] = (row['CO2_Emissions_Million_Tons'] * 1000000) / (row['Population_Millions'] * 1000000)

        # Air Quality Index (Lower is better) - Simulate general improvement + monthly fluctuations
        row['Air_Quality_Index_Avg'] = max(20, 100 - (year - start_year) * 2 + np.random.normal(0, 3) + profile['energy_consumption_seasonality'][month-1] * 5) # Slight seasonality
        
        # Waste Recycling Rate Percentage - Smooth annual increase, constant monthly
        row['Waste_Recycling_Rate_Percentage'] = min(90, 30 + (year - start_year) * 1.5 + np.random.normal(0, 1))
        
        # Deforestation Rate Hectares Per Year - Smooth annual trend, constant monthly
        if country == 'India':
            row['Deforestation_Rate_Hectares_Per_Year'] = max(0, 10000 - (year - start_year) * 500 + np.random.normal(0, 500))
        else:
            row['Deforestation_Rate_Hectares_Per_Year'] = max(0, 1000 - (year - start_year) * 50 + np.random.normal(0, 100))

        # Energy Generation & Transition (Monthly variations added)
        # Calculate annual trends for TWh first, then apply monthly seasonality
        annual_total_energy_consumption = (row['GDP_Billion_USD'] / 100) * 50
        row['Total_Energy_Consumption_TWh'] = annual_total_energy_consumption * profile['energy_consumption_seasonality'][month-1] + np.random.normal(0, 5)

        # Renewable Energy Share Percentage - Overall annual trend
        annual_re_share_increase = profile['re_share_base'] + (year - start_year) * profile['re_share_growth'] * (1 + 0.05 * (year - start_year))
        row['Renewable_Energy_Share_Percentage'] = min(80, annual_re_share_increase * 100 + np.random.normal(0, 1))
        
        renewable_total_twh_annual = row['Total_Energy_Consumption_TWh'] * (row['Renewable_Energy_Share_Percentage'] / 100)
        fossil_fuel_total_twh_annual = row['Total_Energy_Consumption_TWh'] * (1 - (row['Renewable_Energy_Share_Percentage'] / 100))

        # Apply monthly seasonality for specific generation types
        row['Solar_Generation_TWh'] = (renewable_total_twh_annual * profile['solar_seasonality'][month-1] / 12) + np.random.normal(0, 0.5)
        row['Wind_Generation_TWh'] = (renewable_total_twh_annual * profile['wind_seasonality'][month-1] / 12) + np.random.normal(0, 0.5)
        
        # Hydro and Other might have less pronounced seasonality or specific country patterns
        row['Hydro_Generation_TWh'] = (renewable_total_twh_annual * (1 - profile['solar_seasonality'][month-1] - profile['wind_seasonality'][month-1]) / 12) * (0.2 + np.random.uniform(-0.05, 0.05)) # Adjust based on other renewables
        row['Other_Renewable_Generation_TWh'] = (renewable_total_twh_annual * 0.1 / 12) + np.random.normal(0, 0.1) # Simpler for "other"
        
        # Ensure all generated values are non-negative
        for gen_col in ['Solar_Generation_TWh', 'Wind_Generation_TWh', 'Hydro_Generation_TWh', 'Other_Renewable_Generation_TWh']:
            row[gen_col] = max(0, row[gen_col])

        row['Fossil_Fuel_Generation_TWh'] = (fossil_fuel_total_twh_annual / 12) + np.random.normal(0, 0.5) # Divide by 12 for monthly

        # Installed Renewable Capacity (GW) - Primarily annual; slight monthly growth
        row['Installed_Renewable_Capacity_GW'] = (row['Renewable_Energy_Share_Percentage'] / 100) * (500 + (year - start_year) * 200) + np.random.normal(0, 10)
        
        # EV Sales Percentage - Steep exponential growth in recent years, consistent monthly
        ev_growth_factor = max(0, (year - 2015) * 0.05 * (1 + 0.05 * (year - 2015)))
        row['EV_Sales_Percentage_of_New_Cars'] = min(80, ev_growth_factor * 100 + np.random.normal(0, 0.5))

        # Policy & Investment (Mostly annual, slight monthly noise)
        row['Green_Policy_Strength_Index'] = min(10, profile['policy_base'] + (year - start_year) * profile['policy_growth'] + np.random.normal(0, 0.2))
        row['Investment_in_Renewables_Billion_USD'] = (profile['investment_base'] * (1 + profile['investment_growth'])**(year - start_year) / 12) + np.random.normal(0, 0.2)
        row['Renewable_Energy_Subsidies_Billion_USD'] = (row['Investment_in_Renewables_Billion_USD'] * 0.2) + np.random.normal(0, 0.1)

        # Benefits & Marketing (Monthly impact)
        row['Green_Jobs_Created'] = int(profile['green_jobs_base'] * (1 + profile['green_jobs_growth'])**(year - start_year) * 1000 / 12 + np.random.normal(0, 200)) # Monthly portion of jobs
        row['Average_Electricity_Price_USD_per_kWh'] = max(0.08, 0.15 - (year - start_year) * 0.003 + np.random.normal(0, 0.005) + profile['energy_consumption_seasonality'][month-1] * 0.01) # Monthly fluctuation
        
        row['Green_Marketing_Spend_Per_Capita_USD'] = (profile['marketing_base'] * (1 + profile['marketing_growth'])**(year - start_year) / 12) + np.random.normal(0, 0.01)
        row['Public_Green_Awareness_Score'] = min(10, 5 + (row['Green_Policy_Strength_Index'] / 2) + (row['Green_Marketing_Spend_Per_Capita_USD'] * 10) + np.random.normal(0, 0.2)) # Higher influence from monthly marketing

        data_rows.append(row)

# Create DataFrame
df_green_energy_monthly = pd.DataFrame(data_rows)

# Post-processing for realism and missing values
np.random.seed(42) # for reproducibility
for col in ['Green_Policy_Strength_Index', 'Air_Quality_Index_Avg', 'EV_Sales_Percentage_of_New_Cars', 'Waste_Recycling_Rate_Percentage']:
    df_green_energy_monthly.loc[df_green_energy_monthly.sample(frac=0.03).index, col] = np.nan # Reduced fraction for monthly data

# Ensure all numerical columns are indeed numeric and apply rounding
for col in df_green_energy_monthly.columns:
    if col not in ['Country', 'Year', 'Month', 'Date']:
        df_green_energy_monthly[col] = pd.to_numeric(df_green_energy_monthly[col], errors='coerce')
        if 'Percentage' in col or 'Index' in col or 'Price' in col or 'Per_Capita' in col or 'Share' in col:
            df_green_energy_monthly[col] = df_green_energy_monthly[col].round(2)
        elif 'Billion_USD' in col or 'Million_Tons' in col or 'TWh' in col or 'GW' in col or 'Hectares' in col:
            df_green_energy_monthly[col] = df_green_energy_monthly[col].round(3) # More precision for monthly
        else:
            df_green_energy_monthly[col] = df_green_energy_monthly[col].round(0)

# Display head, info, and shape
print("Generated Monthly Green Energy Dataset Head:")
print(df_green_energy_monthly.head())
print("\nGenerated Monthly Green Energy Dataset Info:")
print(df_green_energy_monthly.info())
print("\nGenerated Monthly Green Energy Dataset Shape (Rows, Columns):")
print(df_green_energy_monthly.shape)

# Optional: Save to CSV
df_green_energy_monthly.to_csv('custom_green_energy_monthly_data.csv', index=False)
print("\nDataset saved to 'custom_green_energy_monthly_data.csv'")