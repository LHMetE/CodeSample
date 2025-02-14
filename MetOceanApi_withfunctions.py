import requests
import logging
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patheffects as path_effects
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
            print(f"Error fetching data from {endpoint}: {e}")
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
            # Some buoys will be offline for maintenance etc, so check there is latest data before trying to append
            if data is not None:
                latest_data.append(data)
        return pd.DataFrame(latest_data)

    def merge_data(self, buoys_df, latest_df):
        # Join the dataframes on mmsi (unique buoy id), use suffixes if there is an overlap
        return pd.merge(buoys_df, latest_df, on='mmsi', how='left', suffixes=('_site', '_latest'))

    def clean_data(self, df, column, threshold=30.0):
        # Remove unrealistic values, have left default as 30 because this is currently only used for water temp
        df_clean = df[df[str(column)] <= threshold]
        return df_clean

    def plot_water_temp(self, df_clean):
        # Create a map centered around the average latitude and longitude
        map_center_lat = df_clean['latitude'].mean()
        map_center_lon = df_clean['longitude'].mean()
        m = Basemap(projection='merc', llcrnrlat=50, urcrnrlat=57, llcrnrlon=-11, urcrnrlon=-5, lat_ts=51.5, resolution='i')

        m.drawcoastlines()
        m.drawcountries()
        m.drawmapboundary(fill_color='lightblue')
        m.fillcontinents(color='lightgreen', lake_color='lightblue')
        m.drawparallels(range(50, 58, 1), labels=[1, 0, 0, 0])
        m.drawmeridians(range(-11, -4, 1), labels=[0, 0, 0, 1])

        # Convert latitude and longitude to map coordinates
        x, y = m(df_clean['longitude'].values, df_clean['latitude'].values)

        # Create scatter plot with actual temperature values
        scatter = m.scatter(x, y, c=df_clean['waterTemperature_latest'], cmap='plasma', s=100, vmin=df_clean['waterTemperature_latest'].min(), vmax=df_clean['waterTemperature_latest'].max())

        # Add labels for each point
        for i, (xi, yi) in enumerate(zip(x, y)):
            txt = plt.text(xi, yi, df_clean.iloc[i]['siteName'], fontsize=9, ha='right', color='white')
            txt.set_path_effects([path_effects.Stroke(linewidth=2, foreground='black'), path_effects.Normal()])

        # Add color bar
        cbar = plt.colorbar(scatter)
        cbar.set_label('Water Temperature (°C)')
        cbar.set_ticks([df_clean['waterTemperature_latest'].min(), df_clean['waterTemperature_latest'].max()])
        cbar.set_ticklabels([f"{df_clean['waterTemperature_latest'].min():.1f} °C", f"{df_clean['waterTemperature_latest'].max():.1f} °C"])

        plt.title('Buoy Locations with Water Temperatures')
        plt.show()

# Setup Logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
log_file_handler = logging.FileHandler('C:/Users/lhickey/Documents/Log/api_log.log')
log_file_handler.setFormatter(formatter)
logger.addHandler(log_file_handler)

base_api_url = "https://cilpublic.cil.ie/metoceanapi/api/"
endpoint_top = "metoceansitesensors/"
endpoint_latest = "realtime/latest/"

# Initialize API and Buoy Processor
api = MetOceanAPI(base_url=base_api_url)
processor = BuoyDataProcessor(api)

# Fetch Data - Top level shows which buoys are available
top_level_data = processor.get_top_level_data(endpoint_top)
# Latest data for each buoy is requestd using the mmsi's from top_level_data
latest_data = processor.get_latest_data(top_level_data['mmsi'].tolist(), endpoint_latest)

# Merge latest data for each buoy into the top level data to enrich it
df = processor.merge_data(top_level_data, latest_data)
logger.info(df)

# Clean Data - some buoys were reporting 50 degrees C, setting a threshold to filter that out
df_clean = processor.clean_data(df, 'waterTemperature_latest', 30.0)

# Plot water temperature data on map
processor.plot_water_temp(df_clean)

# Log Data
logger.info(df_clean)
