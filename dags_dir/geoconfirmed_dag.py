from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonVirtualenvOperator
from datetime import datetime, timedelta
import json
import os
import glob

# Define default_args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize the DAG
dag = DAG(
    'kml_to_json',
    default_args=default_args,
    description='Converts geoconfirmed KML data to JSON',
    schedule_interval=timedelta(days=1),
)

# Task decorator 
@task.virtualenv(
    use_dill=False,
    requirements=["pykml"],
    system_site_packages=False,
)

def convert_kml_to_json():
    from pykml import parser

    input_directory = '/storage/geo_data'
    output_directory = '/storage/geo_data'

    # List all KML files in the directory
    kml_files = glob.glob(os.path.join(input_directory, '*.kml'))
    
    for kml_file in kml_files:
        output_path = os.path.join(output_directory, os.path.splitext(os.path.basename(kml_file))[0] + '.json')
        
        with open(kml_file, 'rt', encoding='utf-8') as f:
            root = parser.parse(f).getroot()
        
        # Assuming you want to extract placemarks as an example
        placemarks = []
        for placemark in root.Document.Folder.Placemark:
            placemarks.append({
                'name': str(placemark.name),
                'description': str(placemark.description),
                'coordinates': str(placemark.Point.coordinates).strip(),
            })
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(placemarks, f, indent=4)

# Instantiate the task
convert_kml_to_json_task = convert_kml_to_json()

# Set task dependencies if any
convert_kml_to_json_task
