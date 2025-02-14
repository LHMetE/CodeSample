import requests
import logging
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patheffects as path_effects
import numpy as np
from mpl_toolkits.basemap import Basemap

# Calls the CIL API to get buoy data
# Plots Water Temperature on a map

class MetOceanAPI:
    def __init__(self, base_url):
        self.base_url = base_url

    def get_data(self, endpoint):
        try:
            response = requests.get(f'{self.base_url}/{endpoint}')
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data from {endpoint}: {e}")
            return None

class BuoyDataProcessor:
    def __init__(self, api):
        self.api = api

    def get_top_level_data(self, endpoint):
        all_buoys = self.api.get_data(endpoint=endpoint)
        return pd.DataFrame(all_buoys)

    def get_latest_data(self, mmsi_list, endpoint_latest):
        latest_data = []
        for mmsi in mmsi_list:
            data = self.api.get_data(endpoint=endpoint_latest + str(mmsi))
            # Some buoys will be offline for maintenance etc, ensure there is data & it's a dictionary
            if data and isinstance(data, dict): 
                latest_data.append(data)
            else:
                logger.warning(f"No valid data returned for MMSI {mmsi}")

        return pd.DataFrame(latest_data) if latest_data else pd.DataFrame()

    def merge_data(self, buoys_df, latest_df):
        # Join the dataframes on mmsi (unique buoy id), use suffixes if there is an overlap
        return pd.merge(buoys_df, latest_df, on='mmsi', how='left', suffixes=('_top', '_latest'))

    def clean_data(self, df, column, threshold=30.0):
        # Remove unrealistic values, have left default as 30 because this is currently only used for water temp
        # dreturn df[df[str(column)] <= threshold]
        return df.loc[df[column] <= threshold] # removes rows where the value of specified column is above threshold        

    def plot_water_temp(self, df_clean):
        # Get min and max lat/longitude, adding a margin of 2 degree 
        min_lat, max_lat = df_clean['latitude'].min() - 2, df_clean['latitude'].max() + 2
        min_lon, max_lon = df_clean['longitude'].min() - 2, df_clean['longitude'].max() + 2
        
        map = Basemap(projection='merc', llcrnrlat=min_lat, urcrnrlat=max_lat, 
              llcrnrlon=min_lon, urcrnrlon=max_lon, resolution='i')

        map.drawcoastlines()
        map.drawcountries()
        map.drawmapboundary(fill_color='lightblue')
        map.fillcontinents(color='lightgreen', lake_color='lightblue')
        map.drawparallels(range(50, 58, 1), labels=[1, 0, 0, 0])
        map.drawmeridians(range(-11, -4, 1), labels=[0, 0, 0, 1])

        # Convert latitude and longitude to map coordinates
        x, y = map(df_clean['longitude'].values, df_clean['latitude'].values)

        
        temp_min = df_clean['waterTemperature_latest'].min()
        temp_max = df_clean['waterTemperature_latest'].max()

        # Create scatter plot with actual temperature values
        scatter = map.scatter(x, y, c=df_clean['waterTemperature_latest'], cmap='plasma', s=100, edgecolors='black', vmin=temp_min, vmax=temp_max)

        # Add labels for each point, bbox to make them legible against the map
        for i, (xi, yi) in enumerate(zip(x, y)):
            txt = plt.text(xi, yi, df_clean.iloc[i]['siteName'], fontsize=9, ha='center', va='top', color='white', 
               bbox=dict(facecolor='black', alpha=0.5, edgecolor='none', boxstyle='round,pad=0.05'))
            txt.set_path_effects([path_effects.Stroke(linewidth=2, foreground='black'), path_effects.Normal()])
            

        # Add colour bar
        cbar = plt.colorbar(scatter)
        cbar.set_label('Water Temperature (°C)')
        cbar.set_ticks(np.linspace(temp_min, temp_max, num=5)) # numpy to get evenly spaced ticks

        plt.title('Buoy Locations with Water Temperatures')
        plt.show()

# Setup Logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
log_file_handler = logging.FileHandler('./api_log.log')
log_file_handler.setFormatter(formatter)
logger.addHandler(log_file_handler)

base_api_url = "https://cilpublic.cil.ie/metoceanapi/api/"
endpoint_top = "metoceansitesensors/"
endpoint_latest = "realtime/latest/"

logger.info(f"Met Ocean Api - Starting\n Initializing API at: {base_api_url}")

# Initialize API and Buoy Processor
api = MetOceanAPI(base_url=base_api_url)
processor = BuoyDataProcessor(api)

# Fetch Data - Top level shows which buoys are available
top_level_data = processor.get_top_level_data(endpoint_top)
logger.info(f"Received list of {len(top_level_data)} mmsi's from: \n {endpoint_top}")

# Latest data for each buoy is requestd using the mmsi's from top_level_data
latest_data = processor.get_latest_data(top_level_data['mmsi'].tolist(), endpoint_latest)

# Merge latest data for each buoy into the top level data to enrich it
df = processor.merge_data(top_level_data, latest_data)
logger.info(f"Top Level and Latest data merged successfully.")
logger.info(df)

# Clean Data - some buoys were reporting 50 degrees C, setting a threshold to filter that out
df_clean = processor.clean_data(df, 'waterTemperature_latest', 30.0)

# Plot water temperature data on map
processor.plot_water_temp(df_clean)

# Log Data
logger.info(f"Final dataset has {df_clean.shape[0]} rows and {df_clean.shape[1]} columns.")
logger.info("MetOcean Api requester completed successfully")
