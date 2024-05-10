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

        # The function for acquiring the data from FIRMS.
        def firms_acquire() -> dict :
            import os
            import re

            # Since we want to find the CSV files with the data, we will use regular expressions.
            regex=r"\d{,4}-\d{,2}-\d{4}.csv"
            files = os.listdir('/storage/firms_data')
            pattern = re.compile(regex)

            matches = [file for file in files if pattern.match(file)]
            for file in matches:
                print("acquired " + matches)

            # We will return the status of the function and the list of matches.
            return { "status" : "success", "data_path" : matches }


        # Function we will use to combine all of the separate CSVs into a single CSV.
        def combine(file_list) -> dict :
            import pandas as pd
            data_frame = [pd.read_csv(file) for file in file_list["data_path"]]
            combined_data_frame = pd.concat(data_frame)
            combined_data_frame.to_csv('/storage/combined_data.csv', index=True)
            
            # Return status
            return {"status" : "success", "data_path" : "/storage/combined_data.csv" }

        def firms_acquisition():
            file_path_list = firms_acquire()
            return combine(file_path_list)

        
        @task()
        def data_cleanse():
            import os
            import pandas as pd

            folder_path = '/storage/firms_data/raw_csv'
            save_path = '/storage/firms_data/cleaned_csv' 
            remove_columns_from_csv(folder_path)

            def remove_columns_from_csv(folder_path):

                # Define the columns to be removed
                columns_to_remove = ['scan', 'track', 'satellite', 'version', 'frp']

                 # Loop through all files in the specified folder
                for filename in os.listdir(folder_path):

                    if filename.endswith(".csv"):
                        file_path = os.path.join(folder_path, filename)
            
                        # Read the CSV file
                        df = pd.read_csv(file_path)
            
                        # Remove the specified columns
                        df = df.drop(columns=[col for col in columns_to_remove if col in df.columns])
            
                        # Save the modified DataFrame back to CSV
                        df.to_csv(save_path, index=False)
                        print(f"Processed {filename}")

                        ## return status
                        return {"status" : "success", "folder_path" : save_path}
                    
        @task()
        def data_analysis(data_package: dict):
          
            ## save data
            data_path = "/storage/analysis/"

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

        data_a = firms_acquire()
        data_b = data_cleanse(data_a)
        data_c = data_analysis(data_b)

        visualize(data_c)

    firmspipeline = firms_pipeline()
