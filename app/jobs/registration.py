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
    country = data.get("form", {}).get("coffee_project_country")

    if survey_detail in ["New Farmer New Household", "New Farmer Existing Household", "Existing Farmer Change in FFG"]:
        
        # Split these - Burundi is the only country that needs Coffee Plots data
        if country == 'Burundi':
            household_fields = {
                "Name": data.get("form", {}).get("Household_Number"),
                "Training_Group__c": data.get("form", {}).get("Training_Group_Id"),
                "Farm_Size__c": get_farm_size(data),
                "Number_of_Coffee_Plots__c": get_number_of_plots(data)
            }
        else:
            household_fields = {
                "Name": data.get("form", {}).get("Household_Number"),
                "Training_Group__c": data.get("form", {}).get("Training_Group_Id"),
                "Farm_Size__c": get_farm_size(data),
            }  
        
        household_fields = {
            "Name": data.get("form", {}).get("Household_Number"),
            "Training_Group__c": data.get("form", {}).get("Training_Group_Id"),
            "Farm_Size__c": get_farm_size(data),
            "Number_of_Coffee_Plots__c": get_number_of_plots(data)
        }
        
        if primary_member or survey_detail == "Existing Farmer Change in FFG":
            upsert_to_salesforce(
                "Household__c",
                "Household_ID__c",
                data.get("form").get("Household_Id"),
                household_fields,
                sf_connection
            )

def process_participant(data, sf_connection):
    survey_detail = data.get('form', {}).get('survey_detail')
    
    if survey_detail in ["New Farmer New Household", "New Farmer Existing Household"]:
        participant_fields = {
            "TNS_Id__c": data.get("form", {}).get("Farmer_Id"),
            "Age__c": data.get("form", {}).get("Age"),
            "Name": data.get("form", {}).get("First_Name"),
            "Middle_Name__c": data.get("form", {}).get("Middle_Name"),
            "Last_Name__c": data.get("form", {}).get("Last_Name"),
            "Gender__c": data.get("form", {}).get("Gender"),
            "Status__c": data.get("form", {}).get("Status"),
            "Farm_Size__c": data.get("form", {}).get("Number_of_Trees") or None,
            "Number_of_Coffee_Plots__c": data.get("form", {}).get("Number_of_Plots") or None,
            "Sent_to_OpenFn_Status__c": "Complete",
            "Phone_Number__c": data.get("form", {}).get("Phone_Number"),
            "Primary_Household_Member__c": data.get("form", {}).get("Primary_Household_Member"),
            "Registration_Date__c": data.get("form", {}).get("registration_date"),
            "Create_In_CommCare__c": False
        }
        upsert_to_salesforce(
            "Participant__c",
            "CommCare_Case_Id__c",
            data.get("form", {}).get("subcase_0", {}).get("case", {}).get("@case_id"),
            participant_fields,
            sf_connection
        )

async def send_to_salesforce(data, sf_connection):
    process_training_group(data, sf_connection)
    process_household(data, sf_connection)
    process_participant(data, sf_connection)

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

