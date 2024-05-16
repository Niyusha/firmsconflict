#!/usr/bin/python3

import pandas as pd
import streamlit as st
import folium
import glob
import os
from datetime import datetime

from streamlit_folium import st_folium
    
def get_all_coords() -> list:
    # We will first organize the coordinates by date.
    df = pd.read_csv('cleaned_data.csv')

    # The number of rows in the CSV file.
    row_num = df.shape[0]

    coordinates = list()
    for i in range(row_num):
        # We will append a coordinate to the array.
        coordinates.append({
            'latitude': df['latitude'][i],
            'longitude': df['longitude'][i],
            'type': 'fire'
        })
    
    # List all KML files in the directory
    json_files = [ 'doc.json' ]
    for json_file in json_files:
        # TODO: Add pandas code for doing statistical analysis of the JSONs.
        df = pd.read_json(json_file)

        for index, row in df.iterrows():
            coordinate=row['coordinates'].split(',')[:2]
            # We will append a coordinate to the array.
            coordinates.append({
                'latitude': float(coordinate[0]),
                'longitude': float(coordinate[1]),
                'type': 'loss'
            })

    return coordinates


coordinates = get_all_coords()

st.title(f'Map of losses and fires in Ukraine based on CSVs and KMLs')
m = folium.Map(location=[coordinates[0]['latitude'], coordinates[0]['longitude']], zoom_start=10)
for coord in coordinates:
    if coord['type'] == 'fire':
        folium.CircleMarker(
            location=[coord['latitude'],coord['longitude']],
            radius=10,
            color='orange',
            fill=False,
            fill_color='orange',
            fill_opacity=0.6
        ).add_to(m)
    else:
        folium.CircleMarker(
            location=[coord['latitude'],coord['longitude']],
            radius=25,
            color='blue',
            fill=True,
            fill_color='blue',
            fill_opacity=0.6
        ).add_to(m)


st_folium(m)
st.write("The fires are marked in red while the losses are marked in blue.")
