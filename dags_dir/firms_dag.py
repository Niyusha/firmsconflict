from __future__ import annotations

import logging
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import is_venv_installed

log = logging.getLogger(__name__)

if not is_venv_installed():
    log.warning("The tutorial_taskflow_api_virtualenv example DAG requires virtualenv, please install it.")
else:
    @dag(schedule=None, 
         start_date=datetime(2021, 1, 1), 
         catchup=False, 
         tags=["big_data"])

    def firms_pipeline():

        @task.virtualenv(
            use_dill=True,
            system_site_packages=False,
            requirements=["funcsigs"],
        )

        
        def firms_aquisition():
        #Data Acquisition from NASA
        
            # We import pandas since we will use to collect the satellite data.
            import pandas as pd

            # We are using the datetime module to add and subtract dates.
            from datetime import datetime, timedelta



            # We are going to acquire the data from the NASA FIRMS database.
            
        def check_status():

            #The MAP key we will use for downloading the data.
            MAP_KEY = '2e4281b9ae8d1da4a98529465a8bf118'
            url = 'https://firms.modaps.eosdis.nasa.gov/mapserver/mapkey_status/?MAP_KEY=' + MAP_KEY
            try:
                df = pd.read_json(url,  typ='series')
                return df
            except:
                # possible error, wrong MAP_KEY value, check for extra quotes, missing letters
                print("There is an issue with the query. \nTry in your browser: %s" % url)

            #The function for acquiring the data.
            #We will return an array with a list of CSV files.
            #We are limited to 1000 transactions per minute.

        def acquire():
            MAP_KEY = '2e4281b9ae8d1da4a98529465a8bf118'
            csv_arr = []
            start_date_str = "2023-02-24"
            start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
            end_date_str = "2024-02-24"
            end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
            url = "https://firms.modaps.eosdis.nasa.gov/api/country/csv/" + MAP_KEY + "/MODIS_NRT/UKR/10/" + start_date_str
            #We will keep downloading more data until we reach the entire year.
            while start_date < end_date:
                try:
                    #We want to gather the data for use as CSVs in order to be able to process them using Linux commands.
                    df_ukr = pd.read_csv(url)
                    #todo: Add the ability to store the data as JSONs as well for a greater ability to store and work with the data.
                    file_name = f"{start_date_str}.csv"
                    df_ukr.to_csv(file_name, index=False)
                    csv_arr.append(file_name)
                    #We will add ten days to the start date.
                    start_date = start_date + timedelta(days=10)
                    start_date_str = start_date.strftime("%Y-%m-%d")
                    #We must change the URL value in order to use the new date for continued use.
                    url = "https://firms.modaps.eosdis.nasa.gov/api/country/csv/" + MAP_KEY + "/MODIS_NRT/UKR/10/" + start_date_str
                except pd.errors.ParserError:
                        print("Could not obtain CSV due to transaction limits.")
                except:
                        print("There is an issue with the query. \nTry in your browser: %s" % url)
                
                return csv_arr

            # Function we will use to combine all of the separate CSVs into a single CSV.
        def combine(file_list):
            data_frame = [pd.read_csv for file in files]
            combined_data_frame = pd.concat(data_frame)
            combined_data_frame.to_csv('combined_data.csv', index=True)

            # We are testing our MAP_KEY and the URL for the API to see if it works.
            print(check_status())
            unfiltered_data = acquire()
            combine(unfiltered_data)
            print(check_status())

            
            ## return status
            return {"status" : "success", "data_path" : "/storage/firms_data/" }

        @task()
        def data_cleanse(data_package: dict):
            """
            #### Data Cleanse
            This tasks cleans the data given a path to the recently acquired data.
            """

            ### import modules/libraries
            import json

            ## perform data cleansing
            data_id = "1234"

            ## save data
            data_path = "/storage/cleanse/" + data_id

            ## return status
            return {"status" : "success", "data_path" : data_path}

        @task()
        def data_analysis(data_package: dict):
            """
            #### Analysis
            This task analyzes the data given a path to the recently cleansed data.
            """
            ### import modules/libraries
            import json

            ## perform data cleansing
            data_id = "1234"

            ## save data
            data_path = "/storage/analysis/" + data_id

            ## return status
            return {"status" : "success", "data_path" : data_path}


        @task()
        def visualize(data_package: dict):
            """
            #### Visualize
            This task performs visualization on the data given path to 

            """

            return 
        ## data flow

        data_a = firms_aquisition()
        data_b = data_cleanse(data_a)
        data_c = data_analysis(data_b)

        visualize(data_c)

    python_demo_dag = firms_pipeline()
