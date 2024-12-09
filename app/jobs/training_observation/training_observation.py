import asyncio
from utils.salesforce_client import upsert_to_salesforce
from utils.attendance_util import process_attendance, process_training_session
from utils.logging_config import logger
import os

# Process functions for each Salesforce object

# 1. Process Training Observation Object
def process_observation(data, sf_connection):
    url_string = 'https://www.commcarehq.org/a/' + data.get("domain") + '/api/form/attachment/' + data.get("form", {}).get("meta", {}).get("instanceID") + '/'
    gps_coordinates = data.get("form", {}).get("meta", {}).get("Location", {}).get("#Text")
    observation_fields = {
        "Observer__c": data.get("form", {}).get("Observer"),
        "Trainer__c": data.get("form", {}).get("trainer_salesforce_id"),
        "Training_Group__c": data.get("form", {}).get("Which_Group_Is_Farmer_Trainer_Teaching"),
        "Training_Session__c": data.get("form", {}).get("selected_session"),
        "RecordTypeId": data.get("form", {}).get("Record_Type"),
        "Date__c": data.get("form", {}).get("Date"),
        "Male_Participants__c": int(data.get("form", {}).get("Current_session_participants", {}).get("Male_Participants_In_Attendance")),
        "Female_Participants__c": int(data.get("form", {}).get("Current_session_participants", {}).get("Female_Participants_In_Attendance")),
        "Number_of_Participants__c": int(data.get("form", {}).get("Current_session_participants", {}).get("Total_Participants_In_Attendance")),
        "Shared_Action_Plan__c": data.get("form", {}).get("Feedback_And_Coaching_With_The_Farmer_Trainer", {}).get("Share_Action_Plan") or None,
        "Shared_Action_Plan_Comments__c": data.get("form", {}).get("Feedback_And_Coaching_With_The_Farmer_Trainer", {}).get("Share_Action_Plan_Comments") or None,
        "Did_Well__c": data.get("form", {}).get("Current_Session_Review", {}).get("Did_Well"),
        "To_Improve__c": data.get("form", {}).get("Current_Session_Review", {}).get("To_Improve"),
        "Photo_of_Facilitator_URL__c": url_string + data.get("form", {}).get("Photo"),
        "Farmer_Trainer_Signature__c": url_string + data.get("form", {}).get("Farmer_Trainer_Signature_Section", {}).get("Farmer_Trainer_Signature"),
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