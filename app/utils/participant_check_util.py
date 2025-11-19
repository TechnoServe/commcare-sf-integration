from utils.salesforce_client import upsert_to_salesforce
from utils.logging_config import logger

def process_participant_check_training_observation(data: dict, sf_connection):
    if (data.get("app_id", "") == '30cee26f064e403388e334ae7b0c403b' and data.get("metadata", {}).get("app_build_version", 0) >= 217) or (data.get("app_id", "") == '812728b8b35644dabb51561420938ee0' and data.get("metadata", {}).get("app_build_version", 0) > 34):  # Testing for ONLY KENYA
        for participant in ['Participant_One_Feedback', 'Participant_Two_Feedback', 'Participant_Three_Feedback']:            
            participant_check_fields = {
                # 1. Farmer
                "Participant__r": {
                    "CommCare_Case_Id__c": data.get("form" , {}).get(participant, {}).get("participant_id", "")
                },

                # 2. Agronomy Advisor
                "Checker__c": data.get("form", {}).get("Observer", ""),

                # 3. Date
                "Date_Completed__c": data.get("form", {}).get("Date", ""),

                # 4. Training Session
                "Training_Session__c": data.get("form", {}).get("selected_session", ""),

                # 5 Observation
                "Observation__r": {
                    "Submission_ID__c": data.get("id", "")
                },

                # 6. Attended Last Month's Training
                "Attended_Last_Months_Training__c": {
                    "No": "No",
                    "Yes": "Yes",
                    "No_training_was_offered": "No training was offered"
                }.get(data.get("form", {}).get(participant, {}).get("Attendend_Previous_Training_Module", ""), "N/A"),

                # 7. Record Type ID
                "RecordTypeId": "012Oj000009dilZIAQ"
            }
            upsert_to_salesforce(
                "Check__c",
                "Submission_ID__c",
                f'CHK-{data.get("id")}-{data.get("form" , {}).get(participant, {}).get("participant_id", "")}',
                participant_check_fields,
                sf_connection
            )
    else:
        logger.info({
            "message": "Skipping 'Participant attendance check - Training Observation' logic",
            "request_id": data.get("id")
        })

def process_participant_check_farm_visit_aa(data: dict, sf_connection):
    survey_detail = data.get('form', {}).get('@name') 
    request_id = data.get("id")
    if survey_detail == 'Farm Visit - AA' and ((data.get("app_id", "") == '30cee26f064e403388e334ae7b0c403b' and data.get("metadata", {}).get("app_build_version", 0) >= 217) or (data.get("app_id", "") == '812728b8b35644dabb51561420938ee0' and data.get("metadata", {}).get("app_build_version", 0) >= 69)): # Testing for ONLY KENYA
        for participant in ['farmer_1_questions', 'farmer_2_questions']:

            # Process farmer 2 ONLY if the dictionary exists in form data
            if participant == 'farmer_2_questions' and not data.get("form", {}).get(participant):
                continue

            participant_check_fields = {
                # 1. Farmer
                "Participant__r": {
                    "CommCare_Case_Id__c": data.get("form" , {}).get(participant, {}).get("farmer_id", "")
                },

                # 2. Agronomy Advisor
                "Checker__c": data.get("form", {}).get("trainer", ""),

                # 3. Date
                "Date_Completed__c": data.get("form", {}).get("date_of_visit", ""),

                # 4. Training Session
                "Training_Session__c": data.get("form", {}).get("training_session", ""),

                # 5 Farm Visit
                "Farm_Visit__r": {
                    "FV_Submission_ID__c": f'FV-{data.get("id", "")}'
                },

                # 6. Attended Last Month's Training
                "Attended_Last_Months_Training__c": {
                    "No": "No",
                    "Yes": "Yes",
                    "No_training_was_offered": "No training was offered"
                }.get(data.get("form", {}).get(participant, {}).get("Attendend_Previous_Training_Module", ""), "N/A"),

                # 7. Record Type ID
                "RecordTypeId": "012Oj000009dj9lIAA",

                # 8. Attended Any Trainings
                "Attended_Any_Trainings__c": {
                    "1": "Yes",
                    "0": "No"
                }.get(data.get("form", {}).get(participant, {}).get("attended_training", ""), "N/A"),

                # 9. Number of trainings attended
                "Number_of_Trainings_Attended__c": data.get("form", {}).get(participant, {}).get("number_of_trainings", ""),
            }
            upsert_to_salesforce(
                "Check__c",
                "Submission_ID__c",
                f'CHK-{data.get("id")}-{data.get("form" , {}).get(participant, {}).get("farmer_id", "")}',
                participant_check_fields,
                sf_connection
            )
    else:
        logger.info({
            "message": "Skipping 'Participant attendance check - FV AA' logic",
            "request_id": request_id
        })
