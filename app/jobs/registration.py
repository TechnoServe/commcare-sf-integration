# from app.utils.salesforce_client import SalesforceClient
import asyncio
from utils.salesforce_client import upsert_to_salesforce
from utils.attendance_util import process_attendance, process_training_session
from utils.logging_config import logger
import os
  
# Process functions for each Salesforce object
def process_training_group(data: dict, sf_connection):
    survey_detail = data.get('form', {}).get('survey_detail')
    if survey_detail in ["New Farmer New Household", "Existing Farmer Change in FFG"]:
        training_group_fields = {
            "Household_Counter__c": data.get("form", {}).get("Update_Household_Counter")
        }
        upsert_to_salesforce(
            "Training_Group__c",
            "CommCare_Case_Id__c",
            data.get("form", {}).get("Training_Group_Id"),
            training_group_fields,
            sf_connection
        )

def process_household(data: dict, sf_connection):
    survey_detail = data.get('form', {}).get('survey_detail')
    primary_member = data.get("form", {}).get("Primary_Household_Member") == "Yes"
    
    # Adding a small module for PR Registration
    pr_registration = data.get('form', {}).get('survey_type') == 'Farm Visit Full - PR' and data.get('form', {}).get('new_farmer') == 1

    # 1. Farmer Registration Form
    if survey_detail in ["New Farmer New Household", "New Farmer Existing Household", "Existing Farmer Change in FFG"]:
 
        household_fields = {
            "Name": data.get("form", {}).get("Household_Number"),
            "Training_Group__c": data.get("form", {}).get("Training_Group_Id"),
            "Farm_Size__c": get_farm_size(data) or None,
            "Number_of_Coffee_Plots__c": get_number_of_plots(data) or None
        }
        
        if primary_member or survey_detail == "Existing Farmer Change in FFG":
            upsert_to_salesforce(
                "Household__c",
                "Household_ID__c",
                data.get("form", {}).get("Household_Id"),
                household_fields,
                sf_connection
            )
            
    # 2. Farmer Update Form       
    elif survey_detail == 'Participant Update':
        household_fields = {
            "Farm_Size__c": get_farm_size(data) or None,
            "Number_of_Coffee_Plots__c": get_number_of_plots(data) or None
        }
        
        if primary_member:
            upsert_to_salesforce(
                "Household__c",
                "Household_ID__c",
                data.get("form", {}).get("Household_Id"),
                household_fields,
                sf_connection
            )
    
    # 3. Special Case - PR Registration
    elif pr_registration:
        household_fields = {
            "Name": data.get("form", {}).get("participant_data", {}).get("farmer_registration_details").get("Household_Number"),
            "Training_Group__c": data.get("form", {}).get("participant_data", {}).get("farmer_registration_details").get("Training_Group_Id"),
            "Farm_Size__c": data.get("form", {}).get("participant_data", {}).get("farmer_registration_details").get("Farm_Size"),
            "Farm_Size_Before__c": data.get("form", {}).get("participant_data", {}).get("farmer_registration_details").get("farm_size_3_years_and_older"),
            "Farm_Size_After__c": data.get("form", {}).get("participant_data", {}).get("farmer_registration_details").get("farm_size_under_3_years")
        }
        
        upsert_to_salesforce(
            "Household__c",
            "Household_ID__c",
            data.get("form", {}).get("participant_data", {}).get("farmer_registration_details").get("Household_Id"),
            household_fields,
            sf_connection
        )
    else:
        None

# Registration Fields for New Farmers
def process_participant(data: dict, sf_connection):
    survey_detail = data.get('form', {}).get('survey_detail')
    
    # Adding a small module for PR Registration
    pr_registration = data.get('form', {}).get('survey_type') == 'Farm Visit Full - PR' and data.get('form', {}).get('new_farmer') == 1
    
    # 1. New Farmer Registration
    if survey_detail in ["New Farmer New Household", "New Farmer Existing Household"]:
        participant_fields = {
            # Relationships and Main IDs
            "TNS_Id__c": data.get("form", {}).get("Farmer_Id"),
            "Household__r":{
                "Household_ID__c": data.get("form", {}).get("Household_Id")
            },
            "Training_Group__c": data.get("form", {}).get("Training_Group_Id"),
            
            # Other IDs
            "Other_Id_Number__c": get_other_id_number(data),
            
            # Farmer Data
            "Farm_Size__c": data.get("form", {}).get("Number_of_Trees") or None,
            "Name": data.get("form", {}).get("First_Name"),
            "Middle_Name__c": data.get("form", {}).get("Middle_Name") or None,
            "Last_Name__c": data.get("form", {}).get("Last_Name"),
            "Age__c": data.get("form", {}).get("Age"),
            "Gender__c": data.get("form", {}).get("Gender"),
            "Status__c": data.get("form", {}).get("Status"),
            "Number_of_Coffee_Plots__c": data.get("form", {}).get("Number_of_Plots") or None,
            "Phone_Number__c": data.get("form", {}).get("Phone_Number") or None,
            "Primary_Household_Member__c": data.get("form", {}).get("Primary_Household_Member"),
            "Sent_to_OpenFn_Status__c": "Complete",
            "Registration_Date__c": data.get("form", {}).get("registration_date") or None,
            "Create_In_CommCare__c": False
        }
        upsert_to_salesforce(
            "Participant__c",
            "CommCare_Case_Id__c",
            data.get("form", {}).get("subcase_0", {}).get("case", {}).get("@case_id"),
            participant_fields,
            sf_connection
        )
    
    # 2. Existing Farmer Change in FFG
    elif survey_detail == "Existing Farmer Change in FFG":
        participant_fields = {
            # Relationships and Main IDs
            "TNS_Id__c": data.get("form", {}).get("Farmer_Id"),
            "Household__r":{
                "Household_ID__c": data.get("form", {}).get("Household_Id")
            },
            "Training_Group__c": data.get("form", {}).get("Training_Group_Id"),
            "Former_Training_Group__c": data.get("form", {}).get("existing_farmer_change_in_ffg", {}).get("old_farmer_ffg"),
            "Updated_Date__c": data.get("form", {}).get("registration_date") or None,
            "Create_In_CommCare__c": True
        }
        upsert_to_salesforce(
            "Participant__c",
            "CommCare_Case_Id__c",
            data.get("form", {}).get("existing_farmer_change_in_ffg", {}).get("old_farmer_id"),
            participant_fields,
            sf_connection
        )
    
    # 3. Existing Farmer Updating Details
    elif survey_detail == "Participant Update":
        participant_fields = {
            "Farm_Size__c": get_farm_size(data),
            "Phone_Number__c": data.get("form", {}).get("Phone_Number"),
            "Number_of_Coffee_Plots__c": get_number_of_plots(data),
            "Other_Id_Number__c": get_other_id_number(data),
            "Updated_Date__c": data.get("form", {}).get("registration_date") or None,
            "Create_In_CommCare__c": False
        }
        upsert_to_salesforce(
            "Participant__c",
            "CommCare_Case_Id__c",
            data.get("form", {}).get("case", {}).get("@case_id"),
            participant_fields,
            sf_connection
        )
    
    # 4. PR Registration
    elif pr_registration:
        participant_fields = {
            # Relationships and Main IDs
            "TNS_Id__c": data.get("form", {}).get('participant_data', {}).get('farmer_registration_details', {}).get("Farmer_Id"),
            "Household__r":{
                "Household_ID__c": data.get("form", {}).get('participant_data', {}).get('farmer_registration_details', {}).get("Household_Id")
            },
            "Training_Group__c": data.get("form", {}).get('participant_data', {}).get('farmer_registration_details', {}).get("Training_Group_Id"),
            
            # Farmer Data
            "Farm_Size__c": data.get("form", {}).get("participant_data", {}).get("farmer_registration_details").get("Farm_Size"),
            "Farm_Size_Before__c": data.get("form", {}).get("participant_data", {}).get("farmer_registration_details").get("farm_size_3_years_and_older"),
            "Farm_Size_After__c": data.get("form", {}).get("participant_data", {}).get("farmer_registration_details").get("farm_size_under_3_years"),
            "Name": data.get("form", {}).get('participant_data', {}).get('farmer_registration_details', {}).get("First_Name"),
            "Middle_Name__c": data.get("form", {}).get('participant_data', {}).get('farmer_registration_details', {}).get("Middle_Name") or None,
            "Last_Name__c": data.get("form", {}).get('participant_data', {}).get('farmer_registration_details', {}).get("Last_Name"),
            "Age__c": data.get("form", {}).get('participant_data', {}).get('farmer_registration_details', {}).get("Age"),
            "Gender__c": data.get("form", {}).get('participant_data', {}).get('farmer_registration_details', {}).get("Gender"),
            "Status__c": data.get("form", {}).get('participant_data', {}).get('farmer_registration_details', {}).get("Status"),
            "Phone_Number__c": data.get("form", {}).get('participant_data', {}).get('farmer_registration_details', {}).get("Phone_Number") or None,
            "Primary_Household_Member__c": data.get("form", {}).get('participant_data', {}).get('farmer_registration_details', {}).get("Primary_Household_Member"),
            "Sent_to_OpenFn_Status__c": "Complete",
            "Registration_Date__c": data.get("form", {}).get('participant_data', {}).get('farmer_registration_details', {}).get("registration_date") or None,
            "Create_In_CommCare__c": False
        }
        upsert_to_salesforce(
            "Participant__c",
            "CommCare_Case_Id__c",
            data.get("form", {}).get("subcase_0", {}).get("case", {}).get("@case_id"),
            participant_fields,
            sf_connection
        )

# Cases where the participant is being replaced   
def process_participant_deactivation(data: dict, sf_connection):
    cond_both_filled = data.get("form", {}).get("existing_household.both_filled", {}).get("replaced_member_full")
    cond_primary_filled = data.get("form", {}).get("existing_household.primary_spot_filled", {}).get("primary_replace_adding")
    cond_secondary_filled = data.get("form", {}).get("existing_household.secondary_spot_filled", {}).get("secondary_replace_adding")
    
    if cond_both_filled == "1" or cond_primary_filled == "2":
        participant_fields = {            
            "Status__c": "Inactive",
            "Updated_Date__c": data.get("form", {}).get("registration_date") or None,
            "Create_In_CommCare__c": True
        }
        upsert_to_salesforce(
            "Participant__c",
            "CommCare_Case_Id__c",
            data.get("form", {}).get("existing_household", {}).get("existing_primary_farmer_id"),
            participant_fields,
            sf_connection
        )
    elif cond_both_filled == "2" or cond_secondary_filled == "2":
        participant_fields = {            
            "Status__c": "Inactive",
            "Updated_Date__c": data.get("form", {}).get("registration_date") or None,
            "Create_In_CommCare__c": True
        }
        upsert_to_salesforce(
            "Participant__c",
            "CommCare_Case_Id__c",
            data.get("form", {}).get("existing_household", {}).get("existing_secondary_farmer_id"),
            participant_fields,
            sf_connection
        )

async def send_to_salesforce(data: dict, sf_connection):
    request_id = data.get("id")

    try:
        # Step 1: Process training group
        try:
            logger.info({
                "message": "Processing training group",
                "request_id": request_id,
            })
            process_training_group(data, sf_connection)
        except Exception as e:
            logger.error({
                "request_id": request_id,
                "error": str(e)
            })
            return False, str(e)

        # Step 2: Process household
        try:
            logger.info({
                "message": "Processing household",
                "request_id": request_id
            })
            process_household(data, sf_connection)
        except Exception as e:
            logger.error({
                "request_id": request_id,
                "error": str(e)
            })
            return False, str(e)

        # Step 3: Process participant
        try:
            logger.info({
                "message": "Processing participant",
                "request_id": request_id
            })
            process_participant(data, sf_connection)
        except Exception as e:
            logger.error({
                "request_id": request_id,
                "error": str(e)
            })
            return False, str(e)

        # Step 4: Process participant deactivation
        try:
            logger.info({
                "message": "Processing participant deactivation",
                "request_id": request_id
            })
            process_participant_deactivation(data, sf_connection)
        except Exception as e:
            logger.error({
                "request_id": request_id,
                "error": str(e)
            })
            return False, str(e)

        # Step 5: Process training session
        try:
            logger.info({
                "message": "Processing training session",
                "request_id": request_id
            })
            process_training_session(data, sf_connection)
        except Exception as e:
            logger.error({
                "request_id": request_id,
                "error": str(e)
            })
            return False, str(e)

        # Step 6: Process attendance
        try:
            logger.info({
                "message": "Processing attendance",
                "request_id": request_id
            })
            if os.getenv("SALESFORCE_ENV", "sandbox") != "sandbox": 
                process_attendance(data, sf_connection)
        except Exception as e:
            logger.error({
                "request_id": request_id,
                "error": str(e)
            })
            return False, str(e)

        return True, None

    except Exception as e:
        logger.error({
            "request_id": request_id,
            "error": str(e)
        })
        return False, str(e)

def get_farm_size(data):
    survey_detail = data.get('form', {}).get('survey_detail')
    if survey_detail != "Existing Farmer Change in FFG":
        return data.get("form", {}).get("Number_of_Trees") or None
    elif data.get("form", {}).get("existing_farmer_change_in_ffg", {}).get("former_farmer_primary_secondary_yn") == "Yes":
        return data.get("form", {}).get("existing_farmer_change_in_ffg", {}).get("former_farmer_coffeetrees") or None
    return None

def get_number_of_plots(data):
    survey_detail = data.get('form', {}).get('survey_detail')
    country = data.get("form", {}).get("coffee_project_country") == 'Burundi'
    if country and survey_detail != "Existing Farmer Change in FFG":
        return data.get("form", {}).get("Number_of_Plots") or None
    elif country and data.get("form", {}).get("existing_farmer_change_in_ffg", {}).get("former_farmer_primary_secondary_yn") == "Yes":
        return data.get("form", {}).get("existing_farmer_change_in_ffg", {}).get("former_farmer_coffeeplots") or None
    return data.get("form", {}).get("Number_of_Plots") or None

# Processing other ID number
def get_other_id_number(data):
    survey_detail = data.get('form', {}).get('survey_detail')
    country = data.get("form", {}).get("coffee_project_country")
    if survey_detail != "Existing Farmer Change in FFG":
        if country == 'Burundi':
            return data.get("form", {}).get("National_Id_Number") or None
        elif country == "Kenya":
            return data.get("form",{}).get("Cooperative_Membership_Number") or None
        elif country == "Zimbabwe":
            return data.get("form", {}).get("Grower_Number") or None
        return None
    else:
        return None    
    
