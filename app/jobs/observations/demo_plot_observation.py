import asyncio
from utils.salesforce_client import upsert_to_salesforce
from utils.logging_config import logger
import os

# Process functions for each Salesforce object

# 1. Process Demo Plot Observation Object
def process_observation(data, sf_connection):
    url_string = 'https://www.commcarehq.org/a/' + data.get("domain") + '/api/form/attachment/' + data.get("form", {}).get("meta", {}).get("instanceID") + '/'
    gps_coordinates = data.get("form", {}).get("meta", {}).get("Location", {}).get("#Text")
    observation_fields = {
        "Observer__c": data.get("form", {}).get("Observer"),
        "Trainer__c": data.get("form", {}).get("trainer_salesforce_id"),
        "Training_Group__c": data.get("form", {}).get("training_group"),
        "RecordTypeId": data.get("form", {}).get("record_type"),
        "Date__c": data.get("form", {}).get("date"),
        "Comments_1__c": data.get("form", {}).get("visit_comments"),
        "Demo_Plot_Photo__c": url_string + data.get('form', {}).get('Demo_Plot_Photo'),
        "Observer_Signature__c": url_string + data.get("form", {}).get("Observer_Signature_Section", {}).get("Observer_Signature"),
        "Observation_Location__Latitude__s": gps_coordinates.split(" ")[0],
        "Observation_Location__Longitude__s": gps_coordinates.split(" ")[1],
        "Altitude__c": gps_coordinates.split(" ")[2]
    }
    upsert_to_salesforce(
        "Observation__c",
        "Submission_ID__c",
        data.get("id"),
        observation_fields,
        sf_connection
    )
    
    
# 2. Process Observation Results

def process_observation_results_sucker_selection(data, sf_connection):
    # We need to add a country and cohort field in the observation form in CommCare
    # Add condition for cohort_country unique ID
    observation_results_fields = {
        'Observation__r': {
            'Submission_ID__c': data.get('id')
        },
        'RecordTypeId': '01224000000gQe5AAE',
        'Observation_Criterion__r': {
            'Unique_Name__c': ''
        }
    }
    
    return None