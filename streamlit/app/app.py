#!/usr/bin/python3

import pandas as pd
import streamlit as st

# TODO: add a way to organize the coordinates by date.

# We will first organize the coordinates by date.
df = pd.read_csv("cleaned_data.csv")

# The number of rows in the CSV file.
row_num = df.shape[0]

# We will convert all of the dates to datetime format instead of strings.
df['acq_date'] = pd.to_datetime(df['acq_date'])

coordinates = dict()
for i in range(row_num):
    # The curr_date variable will represent the date that our loop is on.
    curr_date = df['acq_date'][i]
    # If we do not have the acq_date key in the curr_date dictionary for organizing the
    # coordinates based on date, we will declare a new list of dictionaries.
    if not curr_date in coordinates.keys():
        coordinates[curr_date] = list[dict]()
        
    coordinates[curr_date].append({
        'latitude': df['latitude'][i],
        'longitude': df['longitude'][i]
    })

selected_entry = st.sidebar.selectbox('Select date', coordinates.keys())
selected_date_coords = coordinates[selected_entry]

st.map(selected_date_coords)
