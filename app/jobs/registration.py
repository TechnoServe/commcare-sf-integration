# from app.utils.salesforce_client import SalesforceClient
import asyncio
from utils.salesforce_client import upsert_to_salesforce
  
# Process functions for each Salesforce object
def process_training_group(data, sf_connection):
    survey_detail = data.get('form', {}).get('survey_detail')
    if survey_detail in ["New Farmer New Household", "Existing Farmer Change in FFG"]:
        training_group_fields = {
            "Household_Counter__c": data.get("form").get("Update_Household_Counter")
        }
        upsert_to_salesforce(
            "Training_Group__c",
            "CommCare_Case_Id__c",
            data.get("form").get("Training_Group_Id"),
            training_group_fields,
            sf_connection
        )

def process_household(data, sf_connection):
    survey_detail = data.get('form', {}).get('survey_detail')
    primary_member = data.get("form", {}).get("Primary_Household_Member") == "Yes"

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
                data.get("form").get("Household_Id"),
                household_fields,
                sf_connection
            )

# Registration Fields for New Farmers
def process_participant(data, sf_connection):
    survey_detail = data.get('form', {}).get('survey_detail')
    
    if survey_detail in ["New Farmer New Household", "New Farmer Existing Household"]:
        participant_fields = {
            # Relationships and Main IDs
            "TNS_Id__c": data.get("form", {}).get("Farmer_Id"),
            "Household__r":{
                "Household_ID__c": data.get("form").get("Household_Id")
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

# Cases where the participant is being replaced   
def process_participant_deactivation(data, sf_connection):
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

async def send_to_salesforce(data, sf_connection):
    process_training_group(data, sf_connection)
    process_household(data, sf_connection)
    process_participant(data, sf_connection)
    process_participant_deactivation(data, sf_connection)

    return True

def get_farm_size(data):
    survey_detail = data.get('form', {}).get('survey_detail')
    if survey_detail != "Existing Farmer Change in FFG":
        return data.get("form", {}).get("Number_of_Trees") or None
    elif data.get("form", {}).get("existing_farmer_change_in_ffg", {}).get("former_farmer_primary_secondary_yn") == "Yes":
        return data.get("form", {}).get("existing_farmer_change_in_ffg", {}).get("former_farmer_coffeetrees") or None
    return None

def get_number_of_plots(data):
    survey_detail = data.get('form', {}).get('survey_detail')
    if survey_detail != "Existing Farmer Change in FFG":
        return data.get("form", {}).get("Number_of_Plots") or None
    elif data.get("form", {}).get("coffee_project_country") == 'Burundi' and \
         data.get("form", {}).get("existing_farmer_change_in_ffg", {}).get("former_farmer_primary_secondary_yn") == "Yes":
        return data.get("form", {}).get("existing_farmer_change_in_ffg", {}).get("former_farmer_coffeeplots") or None
    return None

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
    