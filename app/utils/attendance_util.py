from utils.salesforce_client import upsert_to_salesforce
from utils.training_group_util import training_group_exists
from utils.logging_config import logger
import os
environment = os.getenv("SALESFORCE_ENV")

# Update the Training Session Object
def process_training_session(data: dict, sf_connection):
    form_name = data.get("form", {}).get("@name")
    
    # Determine if Training Group exists (only for sandbox)
    training_group_id = data.get("form", {}).get("Training_Group_Id")
    training_group_exists_flag = True
    if environment.lower() == "sandbox" and training_group_id:
        print(training_group_exists(sf_connection, training_group_id))
        if training_group_exists(sf_connection, training_group_id) is False:
            training_group_id = 'a0JOj00000EeZqeMAF'

    # 1. Process for Participant Registration
    survey_detail = data.get("form", {}).get("survey_detail")
    if survey_detail in ["New Farmer New Household", "New Farmer Existing Household", "Existing Farmer Change in FFG"]:
        training_session_fields = {
            "Trainer__c": data.get("form", {}).get("trainer"),
            "Updated_from_CommCare__c": True,
            "Date__c": data.get("form", {}).get("registration_date"),
            "Training_Group__c": training_group_id
        }

        upsert_to_salesforce(
            "Training_Session__c",
            "CommCare_Case_Id__c",
            data.get("form", {}).get("training_session"),
            training_session_fields,
            sf_connection
        )
    # 2. Process for Attendance Light
    else:
        if form_name == 'Attendance Full - Current Module':

             # Determine if Training Group exists (only for sandbox)
            training_session_id = data.get("form", {}).get("training_session")
            if training_session_id:
                if training_session_exists(sf_connection, training_session_id) is False:    
                    record = {
                        "Trainer__c": data.get("form", {}).get("trainer"),
                        "Number_in_Attendance__c": data.get("form", {}).get("attendance_count"),
                        "Session_Photo_URL__c": get_photo_url(data),
                        "Date__c": data.get("form", {}).get("date"),
                        "Location_GPS__Latitude__s": get_gps_part(data.get("form", {}).get("gps_coordinates"), 0),
                        "Location_GPS__Longitude__s": get_gps_part(data.get("form", {}).get("gps_coordinates"), 1),
                        "Altitude__c": get_gps_part(data.get("form", {}).get("gps_coordinates"), 2),
                        "Training_Group__c": 'a0JOj00000EeZqeMAF'
                    }

                else:
                    record = {
                        "Trainer__c": data.get("form", {}).get("trainer"),
                        "Number_in_Attendance__c": data.get("form", {}).get("attendance_count"),
                        "Session_Photo_URL__c": get_photo_url(data),
                        "Date__c": data.get("form", {}).get("date"),
                        "Location_GPS__Latitude__s": get_gps_part(data.get("form", {}).get("gps_coordinates"), 0),
                        "Location_GPS__Longitude__s": get_gps_part(data.get("form", {}).get("gps_coordinates"), 1),
                        "Altitude__c": get_gps_part(data.get("form", {}).get("gps_coordinates"), 2),
                    }

                upsert_to_salesforce(
                    "Training_Session__c",
                    "CommCare_Case_Id__c",
                    training_session_id,
                    record,
                    sf_connection
                )

    
def process_attendance(data: dict, sf_connection):

    form_name = data.get("form", {}).get("@name")
    
    survey_detail = data.get("form",{}).get("survey_detail")

    if form_name == 'Farmer Registration':
        # 1. Process for Farmer Registration - New Farmer
        if survey_detail in ["New Farmer New Household", "New Farmer Existing Household"]:
            attendance_fields = {
                "Status__c": "Present",
                "Training_Session__r": {
                    "CommCare_Case_Id__c": data.get("form", {}).get("training_session")
                },
                "Participant__r": {
                    "CommCare_Case_Id__c": data.get("form", {}).get("subcase_0", {}).get("case", {}).get("@case_id")
                }
            }
            upsert_to_salesforce (
                "Attendance__c",
                "Submission_ID__c",
                data.get("form", {}).get("training_session") + data.get("form", {}).get("subcase_0", {}).get("case", {}).get("@case_id"),
                attendance_fields,
                sf_connection
            )
            
        # 2. Process for Farmer Registraion - Existing Farmer
        elif survey_detail == "Existing Farmer Change in FFG":
            attendance_fields = {
                "Status__c": "Present",
                "Training_Session__r": {
                    "CommCare_Case_Id__c": data.get("form", {}).get("training_session")
                },
                "Participant__r": {
                    "CommCare_Case_Id__c": data.get("form", {}).get("existing_farmer_change_in_ffg", {}).get("old_farmer_id")
                }
            }
            upsert_to_salesforce (
                "Attendance__c",
                "Submission_ID__c",
                data.get("form", {}).get("training_session") + data.get("form", {}).get("existing_farmer_change_in_ffg", {}).get("old_farmer_id"),
                attendance_fields,
                sf_connection
            )
        
    # 3. Process for Attendance Full - Current Module
    elif form_name == "Attendance Full - Current Module":
        # Check if present participants exist
        present_participants = data.get("data", {}).get("form", {}).get("present_participants")
        if present_participants:
            # Split the participants and create records
            participants = present_participants.split(" ")
            for p_id in participants:
                session = data.get("data", {}).get("form", {}).get("training_session")
                photo = data.get("data", {}).get("form", {}).get("photo")
                submission_id = f"{session}{p_id}"

                record = {
                    "Status__c": "Present",
                    "Training_Session__r": {"CommCare_Case_Id__c": session},
                    "Participant__r": {"CommCare_Case_Id__c": p_id},
                    # "Session_Photo__c": photo
                }

                upsert_to_salesforce (
                    "Attendance__c",
                    "Submission_ID__c",
                    submission_id,
                    record,
                    sf_connection
                )
                
def training_session_exists(sf_connection, training_session_id):
        """Check if the Training Group exists in Salesforce."""
        try:
            response = sf_connection.Training_Session__c.get(training_session_id)
            return response is not None
        except Exception:
            return False
        
# Helper function to calculate photo URL
def get_photo_url(state):
    photo = state.get("form", {}).get("photo")
    if photo and photo.strip():
        domain = state.get("domain", "")
        instance_id = state.get("form", {}).get("meta", {}).get("instanceID", "")
        return f"https://www.commcarehq.org/a/{domain}/api/form/attachment/{instance_id}/{photo}"
    return ""

# Helper function to split GPS coordinates
def get_gps_part(coords, index):
    if coords:
        parts = coords.split(" ")
        if len(parts) > index:
            return parts[index]
    return ""