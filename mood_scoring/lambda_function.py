import boto3
import logging

# logging.info("Before importing time")
import time 

def lambda_handler(event, context):
    client = boto3.client('sagemaker')
    notebook_instance_name = 'sentimentScoring'
    
    response = client.describe_notebook_instance(NotebookInstanceName=notebook_instance_name)
    notebook_status = response['NotebookInstanceStatus']
    
    if notebook_status == 'Stopped':
        logging.info(f"Starting notebook instance {notebook_instance_name}...")
        client.start_notebook_instance(NotebookInstanceName=notebook_instance_name)
        logging.info(f"Notebook instance {notebook_instance_name} started.")
    elif notebook_status == 'InService':
        logging.info(f"Notebook instance {notebook_instance_name} is already running.")
        logging.info(f"Waiting for notebook instance {notebook_instance_name} to stop...")
        client.stop_notebook_instance(NotebookInstanceName=notebook_instance_name)

        while notebook_status != 'Stopped':
            time.sleep(10)
            response = client.describe_notebook_instance(NotebookInstanceName=notebook_instance_name)
            notebook_status = response['NotebookInstanceStatus']
        logging.info(f"Notebook instance {notebook_instance_name} stopped.")
        
        logging.info(f"Starting notebook instance {notebook_instance_name}...")
        client.start_notebook_instance(NotebookInstanceName=notebook_instance_name)
        logging.info(f"Notebook instance {notebook_instance_name} started.")
        
    else:
        logging.warning(f"Unexpected notebook instance status {notebook_status}")
        
        
    return 0
