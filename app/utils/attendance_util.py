from utils.salesforce_client import upsert_to_salesforce
from utils.training_group_util import training_group_exists
from utils.logging_config import logger
import os
environment = os.getenv("SALESFORCE_ENV")

# Update the Training Session Object
def process_training_session(data: dict, sf_connection):
    form_name = data.get("form", {}).get("@name", "")
    survey_detail = data.get("form", {}).get("survey_detail", "")
    request_id = data.get("id")
    
    # 1. Process for Participant Registration
    if survey_detail in ["New Farmer New Household", "New Farmer Existing Household", "Existing Farmer Change in FFG"]:
        
        training_session_fields = {
            "Trainer__c": data.get("form", {}).get("trainer"),
            "Updated_from_CommCare__c": True,
            "Date__c": data.get("form", {}).get("registration_date"),
        }

        upsert_to_salesforce(
            "Training_Session__c",
            "CommCare_Case_Id__c",
            data.get("form", {}).get("training_session", ""),
            training_session_fields,
            sf_connection
        )
        
    # 2. Process for Attendance Full
    elif form_name == 'Attendance Full - Current Module':
        record = {
            "Trainer__c": data.get("form", {}).get("trainer"),
            "Number_in_Attendance__c": data.get("form", {}).get("attendance_count"),
            "Session_Photo_URL__c": get_photo_url(data),
            "Date__c": data.get("form", {}).get("date"),
            "Location_GPS__Latitude__s": get_gps_part(data.get("form", {}).get("gps_coordinates", ""), 0),
            "Location_GPS__Longitude__s": get_gps_part(data.get("form", {}).get("gps_coordinates", ""), 1),
            "Altitude__c": get_gps_part(data.get("form", {}).get("gps_coordinates", ""), 2),
        }

        upsert_to_salesforce(
            "Training_Session__c",
            "CommCare_Case_Id__c",
            data.get("form", {}).get("training_session", ""),
            record,
            sf_connection
        )
    
    # 3. Process for Attendance Light - Current Module (FT Level)
    elif (form_name == 'Attendance Light - Current Module' or (form_name == "Followup" and data.get("form", {}).get("survey_type", "") == "Attendance Light")) and data.get("form", {}).get("session", "") in ["first_session", None, ""]:
        record = {
            "Updated_from_CommCare__c": True,
            "Trainer__c": data.get("form", {}).get("trainer", ""),
            "Date__c": data.get("form", {}).get("Current_session_participants", {}).get("date", ""),
            "Male_Attendance__c": data.get("form", {}).get("Current_session_participants", {}).get("male_attendance", ""),
            "Female_Attendance__c": data.get("form", {}).get("Current_session_participants", {}).get("female_attendance", ""),
            "Number_in_Attendance__c": data.get("form", {}).get("Current_session_participants", {}).get("total_attendance", ""),
            "Session_Photo_URL__c": get_photo_url(data),
            "Location_GPS__Latitude__s": get_gps_part(data.get("form", {}).get("gps_coordinates", ""), 0),
            "Location_GPS__Longitude__s": get_gps_part(data.get("form", {}).get("gps_coordinates", ""), 1),
            "Altitude__c": get_gps_part(data.get("form", {}).get("gps_coordinates", ""), 2),
        }

        upsert_to_salesforce(
            "Training_Session__c",
            "CommCare_Case_Id__c",
            data.get("form", {}).get("selected_training_module", ""),
            record,
            sf_connection
        )
    
    # 4. Process for Attendance Light - Current Module (AA Level)
    elif (form_name == 'Attendance Light - Current Module' or (form_name == "Followup" and data.get("form", {}).get("survey_type", "") == "Attendance Light")) and data.get("form", {}).get("session", "") == "second_session":
        record = {
            "Updated_from_CommCare__c": True,
            "Trainer__c": data.get("form", {}).get("trainer", ""),
            "Date_2__c": data.get("form", {}).get("Current_session_participants", {}).get("date", ""),
            "Second_Male_Attendance__c": data.get("form", {}).get("Current_session_participants", {}).get("male_attendance", ""),
            "Second_Female_Attendance__c": data.get("form", {}).get("Current_session_participants", {}).get("female_attendance", ""),
            "Second_Number_in_Attendance__c": data.get("form", {}).get("Current_session_participants", {}).get("total_attendance", ""),
            "Session_Photo_URL_2__c": get_photo_url(data),
            "Second_Location_GPS__Latitude__s": get_gps_part(data.get("form", {}).get("gps_coordinates", ""), 0),
            "Second_Location_GPS__Longitude__s": get_gps_part(data.get("form", {}).get("gps_coordinates", ""), 1),
            "Second_Altitude__c": get_gps_part(data.get("form", {}).get("gps_coordinates", ""), 2),
        }

        upsert_to_salesforce(
            "Training_Session__c",
            "CommCare_Case_Id__c",
            data.get("form", {}).get("selected_training_module", ""),
            record,
            sf_connection
        )
    
    # 5. Process for field day attendance full
    elif form_name == "Field Day Attendance Full" or survey_detail == "Field Day Attendance Full":
        items = data.get("form", {}).get("barrios_repeat_group", {}).get("item", [])
        
        # 5.1. Get all training groups where trainees came from
        session_records = [i for i in items if i.get("attendance_count_repeat", "") not in ["", "0"]]
        
        # 5.2. Upsert the training sessions
        for session in session_records:
            session_data = {
                "Trainer__c": data.get("form", {}).get("trainer", ""),
                "Number_in_Attendance__c": session.get("attendance_count_repeat", ""),
                "Session_Photo_URL__c": get_photo_url(data),
                "Date__c": data.get("form", {}).get("date"),
                "Location_GPS__Latitude__s": get_gps_part(data.get("form", {}).get("gps_coordinates", ""), 0),
                "Location_GPS__Longitude__s": get_gps_part(data.get("form", {}).get("gps_coordinates", ""), 1),
                "Altitude__c": get_gps_part(data.get("form", {}).get("gps_coordinates", ""), 2),
            }
            
            # 5.2.1. Upsert to salesforce
            upsert_to_salesforce(
                "Training_Session__c",
                "CommCare_Case_Id__c",
                session.get("training_session", ""),
                session_data,
                sf_connection
            )
    
    else: logger.info({
            "message": "Skipping training session upsert logic",
            "request_id": request_id,
        })

# 2. Upsert attendances    
def process_attendance(data: dict, sf_connection):

    form_name = data.get("form", {}).get("@name", "")
    request_id = data.get("id")
    survey_detail = data.get("form",{}).get("survey_detail", "")

    if form_name == 'Farmer Registration' or form_name == 'Field Day Farmer Registration':
        # 1. Process for Farmer Registration - New Farmer
        if survey_detail in ["New Farmer New Household", "New Farmer Existing Household"]:
            attendance_fields = {
                "Status__c": "Present",
                "Training_Session__r": {
                    "CommCare_Case_Id__c": data.get("form", {}).get("training_session", "")
                },
                "Participant__r": {
                    "CommCare_Case_Id__c": data.get("form", {}).get("subcase_0", {}).get("case", {}).get("@case_id", "")
                }
            }
            upsert_to_salesforce (
                "Attendance__c",
                "Submission_ID__c",
                data.get("form", {}).get("training_session") + data.get("form", {}).get("subcase_0", {}).get("case", {}).get("@case_id", ""),
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
                data.get("form", {}).get("training_session") + data.get("form", {}).get("existing_farmer_change_in_ffg", {}).get("old_farmer_id", ""),
                attendance_fields,
                sf_connection
            )
        
    # 3. Process for Attendance Full - Current Module
    elif form_name == "Attendance Full - Current Module":
        # Check if present participants exist
        present_participants = data.get("form", {}).get("present_participants", "")
        if present_participants:
            # Split the participants and create records
            participants = present_participants.split(" ")
            for p_id in participants:
                session = data.get("form", {}).get("training_session", "")
                submission_id = f"{session}{p_id}"

                record = {
                    "Status__c": "Present",
                    "Training_Session__r": {"CommCare_Case_Id__c": session},
                    "Participant__r": {"CommCare_Case_Id__c": p_id},
                }

                upsert_to_salesforce (
                    "Attendance__c",
                    "Submission_ID__c",
                    submission_id,
                    record,
                    sf_connection
                )
                
    # 4. Process for Field Day Attendance Full
    elif form_name == "Field Day Attendance Full" or survey_detail == "Field Day Attendance Full":
        items = data.get("form", {}).get("barrios_repeat_group", {}).get("item", [])
        
        # 5.1. Get all training groups where trainees came from
        session_records = [i for i in items if i.get("attendance_count_repeat", "") not in ["", "0"]]
        
        for session in session_records:
            participants = session.get("present_participants_repeat", "").split(" ")
            
            # 5.1.1. Loop through each participant and upsert an attendance
            for participant in participants:
                attendance_fields = {
                    "Status__c": "Present",
                    "Training_Session__r": {"CommCare_Case_Id__c": session.get("training_session", "")},
                    "Participant__r": {"CommCare_Case_Id__c": participant},
                }
                
                # 5.1.1.1. Upsert to salesforce
                upsert_to_salesforce (
                    "Attendance__c",
                    "Submission_ID__c",
                    f'{session.get("training_session", "")}{participant}',
                    attendance_fields,
                    sf_connection
                )
    
    else: 
        logger.info({
            "message": "Skipping attendance upsert logic",
            "request_id": request_id,
        })
                
# def training_session_exists(sf_connection, training_session_id):
#         """Check if the Training Group exists in Salesforce."""
#         try:
#             response = sf_connection.Training_Session__c.get(training_session_id)
#             return response is not None
#         except Exception:
#             return False
        
# Helper function to calculate photo URL
def get_photo_url(state):
    photo = state.get("form", {}).get("photo", "")
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