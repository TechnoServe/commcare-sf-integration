from utils.salesforce_client import upsert_to_salesforce

# Update the Training Session Object
def process_training_session(data, sf_connection):
    form_name = data.get("form",{}).get("@name")
    
    # 1. Process for Participant Registration
    survey_detail = data.get("form",{}).get("survey_detail")
    if survey_detail in ["New Farmer New Household", "New Farmer Existing Household", "Existing Farmer Change in FFG"]:
        training_session_fields = {
            "Trainer__c" : data.get("form", {}).get("trainer"),
            "Updated_from_CommCare__c": True,
            "Date__c": data.get("form", {}).get("registration_date"),
            "Training_Group__c": data.get("form").get("Training_Group_Id")
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
        None
    
def process_attendance(data, sf_connection):
    survey_detail = data.get("form",{}).get("survey_detail")

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
    else:
        None