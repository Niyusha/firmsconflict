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

    def geoconfirmed_pipeline():

        @task.virtualenv(
            use_dill=True,
            system_site_packages=False,
            requirements=["funcsigs", "pandas", "pykml"],
        )


        def geoconfirmed_acquisition() -> dict:
            import glob
            import os
            import json
            from pykml import parser

            input_directory = '/storage/geo_data'
            output_directory = '/storage/geo_data'

            # List all KML files in the directory
            kml_files = glob.glob(os.path.join(input_directory, '*.kml'))
    
            json_list = []
            for kml_file in kml_files:
                print(kml_file)
                output_path = os.path.join(output_directory, os.path.splitext(os.path.basename(kml_file))[0] + '.json')
                print(output_path)
        
                with open(kml_file, 'rt', encoding='utf-8') as f:
                    root = parser.parse(f).getroot()

                    # Assuming you want to extract placemarks as an example
                    placemarks = []
                    for placemark in root.Document.Folder.Placemark:
                        # Check if the placemark has a timestamp
                        if hasattr(placemark, 'TimeStamp'):
                            timestamp = placemark.TimeStamp.when.text
                            print(f'Timestamp: {timestamp}')

                        placemarks.append({
                            'name': str(placemark.name),
                            # 'description': str(placemark.description),
                            'coordinates': str(placemark.Point.coordinates).strip(),
                        })
        
                    with open(output_path, 'w', encoding='utf-8') as f:
                        json.dump(placemarks, f, indent=4)
                
                json_list.append(output_path)

            return { 'status': 'success', 'data_path': json_list }
             
        @task()
        def data_analysis(data_package: dict) -> dict:
            import glob
            import os
            import pandas as pd

            ## save data
            data_path = "/storage/geo_data"
            save_path = "/storage/analysis"

            # List all KML files in the directory
            json_files = glob.glob(os.path.join(data_path, '*.json'))
            for json_file in json_files:
                print(json_file)
                # TODO: Add pandas code for doing statistical analysis of the JSONs.
                df = pd.read_json(json_file)

                # We will declare an empty dictionary coordinates.
                coords = []
                for index, row in df.iterrows():
                    print(row['name'])
                    print(row['coordinates'])

            ## return status
            return {"status" : "success", "data_path" : save_path}


        @task()
        def visualize(data_package: dict):
            """
            #### Visualize
            This task performs visualization on the data given path to 

            """

            return 
        ## data flow

        data_a = geoconfirmed_acquisition()
        data_b = data_analysis(data_a)
        data_c = visualize(data_b)

    geoconfirmed_pipeline = geoconfirmed_pipeline()

