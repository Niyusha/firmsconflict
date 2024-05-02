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
            requirements=["funcsigs", "pandas", "pykml"],
        )

            
        # Function we will use to check our status in terms of the number of transactions we have made.
        def check_status():
            import pandas as pd
            #The MAP key we will use for downloading the data.
            MAP_KEY = '2e4281b9ae8d1da4a98529465a8bf118'
            url = 'https://firms.modaps.eosdis.nasa.gov/mapserver/mapkey_status/?MAP_KEY=' + MAP_KEY
            try:
                # We will use the API to get the current number of transactions and the transaction limit.
                df = pd.read_json(url,  typ='series')
                # We will get the transaction limit and the current transactions.
                transaction_limit = df["transaction_limit"]
                curr_transactions = df["current_transactions"]
                # We will subtract the transaction limit from the number of transactions done.
                return transaction_limit - curr_transactions
            except:
                # possible error, wrong MAP_KEY value, check for extra quotes, missing letters
                print("There is an issue with the query. \nTry in your browser: %s" % url)
                return -1

        
        """
        TODO: Add the feature that allows us to continue downloading after ten minutes have passed so the transaction
        limit has been reset.
        """
        def firms_acquire():
            import pandas as pd
            import time
            
            from datetime import datetime, timedelta

            MAP_KEY = '2e4281b9ae8d1da4a98529465a8bf118'
            # We will create an array of CSV files that we will combine into a single file.
            csv_arr = []
            # The start date that we will use for downloading the data.
            start_date_str = "2023-02-24"
            # The date class we will use for representing the start date.
            start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
            # The last date we will download the data from.
            end_date_str = "2024-02-24"
            # The date class we will use to represent the end date.
            end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
            url = "https://firms.modaps.eosdis.nasa.gov/api/country/csv/" + MAP_KEY + "/MODIS_NRT/UKR/10/" + start_date_str
            #We will keep downloading more data until we reach the entire year.
            while start_date < end_date:
                try:
                    #We want to gather the data for use as CSVs in order to be able to process them using Linux commands.
                    df_ukr = pd.read_csv(url)
                    #todo: Add the ability to store the data as JSONs as well for a greater ability to store and work with the data.
                    # We will declare a filename which will be the name of the CSV file based on the date.
                    file_name = f"/storage/{start_date_str}.csv"
                    # We will save the CSV data we acquired from firms to a file.
                    df_ukr.to_csv(file_name, index=False)
                    # We will append the filename to the list.
                    csv_arr.append(file_name)
                    #We will add ten days to the start date.
                    start_date = start_date + timedelta(days=10)
                    # We will then reassign the start date string to be the new start date.
                    start_date_str = start_date.strftime("%Y-%m-%d")
                    #We must change the URL value in order to use the new date for continued use.
                    url = "https://firms.modaps.eosdis.nasa.gov/api/country/csv/" + MAP_KEY + "/MODIS_NRT/UKR/10/" + start_date_str
                except:
                    # We will check the number of transactions left.
                    transactions_left = check_status()
                    if transactions_left > 50:
                        print("We have less than 100 queries left.  We may have to wait another ten minutes before we can continue scraping...")
                        # We have the function sleep for 10 minutes.
                        time.sleep(600)
                    else:
                        print("There is an issue with the query. \nTry in your browser: %s" % url)
                
                return {"status": "success", "data_path": csv_arr}

        
        # Function we will use to combine all of the separate CSVs into a single CSV.
        def combine(file_list):
            data_frame = [pd.read_csv(file) for file in file_list["data_path"]]
            combined_data_frame = pd.concat(data_frame)
            combined_data_frame.to_csv('/storage/combined_data.csv', index=True)
            
            ## return status
            return {"status" : "success", "data_path" : "/storage/combined_data.csv" }


        def firms_acquisition():
            file_path_list = firms_acquire()
            return combine(file_path_list)

        
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

        data_a = firms_acquisition()
        data_b = data_cleanse(data_a)
        data_c = data_analysis(data_b)

        visualize(data_c)

    python_demo_dag = firms_pipeline()
