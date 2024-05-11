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

        def firms_acquisition() -> dict:
            import os
            import re
            import pandas as pd

            # The function for acquiring the data from FIRMS.
            def firms_acquire() -> dict :
                # Since we want to find the CSV files with the data, we will use regular expressions.
                regex=r"\d{,4}-\d{,2}-\d{,2}.csv"
                files = os.listdir('/storage/firms_data/')
                pattern = re.compile(regex)

                matches = [f"/storage/firms_data/{file}" for file in files if pattern.match(file)]
                for file in matches:
                    print("acquired " + file)

                # We will return the status of the function and the list of matches.
                return { "status" : "success", "data_path" : matches }

            # Function we will use to combine all of the separate CSVs into a single CSV.
            def combine(file_list : dict) -> dict :
                data_frame = [pd.read_csv(file) for file in file_list["data_path"]]
                combined_data_frame = pd.concat(data_frame)
                combined_data_frame.to_csv('/storage/firms_data/combined_data.csv', index=True)
            
                # Return status
                return {"status" : "success", "data_path" : "/storage/combined_data.csv" }
            
            file_path_list = firms_acquire()
            return combine(file_path_list)
        
        @task()
        def data_cleanse(file_to_clean : dict) -> dict:
            import os
            import pandas as pd

            file_path = file_to_clean["data_path"]
            save_path = '/storage/firms_data/cleaned_data.csv' 

            # Moved the function definition to before it is used.
            def remove_columns_from_csv(folder_path : dict) -> dict:
                # Define the columns to be removed
                columns_to_remove = ['scan', 'track', 'satellite', 'version', 'frp']

                # Read the CSV file
                df = pd.read_csv(file_path)
            
                # Remove the specified columns
                df = df.drop(columns=[col for col in columns_to_remove if col in df.columns])
            
                # Save the modified DataFrame back to CSV
                df.to_csv(save_path, index=False)
                print(f"Processed {file_path}")

                ## return status
                return {"status" : "success", "data_path" : save_path}


            def convert_to_json(folder_path : dict) -> dict:
                df = pd.read_csv(folder_path["data_path"])
                json_string = df.to_json(orient='records')

                with open('/storage/firms_data/cleaned_data.json', 'w') as f:
                    f.write(json_string)

                return {"status" : "success", "data_path": "/storage/firms_data/cleaned_data.json"}

            cleaned_data = remove_columns_from_csv(file_path)
            return convert_to_json(data1)
                    
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
