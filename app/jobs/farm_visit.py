# from app.utils.salesforce_client import SalesforceClient
import asyncio
from utils.salesforce_client import upsert_to_salesforce
from utils.logging_config import logger
import os
  
# Process functions for each Salesforce object
def generate_common_farm_visit_fields(data, url_string, gps_coordinates):
    """Generate common fields for farm visit records."""
    return {
        "Name__c": f'FV-{data.get("id", {})}',
        "Farm_Visit_Type__c": data.get('form', {}).get('survey_type'),
        "Training_Group__c": data.get('form', {}).get('case', {}).get('@case_id'),
        "Training_Session__c": data.get('form', {}).get('training_session'),
        "Primary_Farmer_PIMA_ID__c": data.get('form', {}).get('farm_being_visited'),
        "Secondary_Farmer_PIMA_ID__c": data.get('form', {}).get('secondary_farmer'),
        "Date_Visited__c": data.get('form', {}).get('date_of_visit'),
        "visit_comments__c": data.get('form', {}).get('farm_visit_comments'),
        "Farmer_Trainer__c": data.get('form', {}).get('trainer'),
        "Field_Size__c": data.get('form', {}).get('field_size') or data.get('form', {}).get('field_size1'),
        "Farm_Visit_Photo_Url__c": f'{url_string}{data.get("form", {}).get("farm_visit_photo")}',
        "Signature__c": f'{url_string}{data.get("form", {}).get("signature_of_farmer_trainer")}',
        "Location_GPS__Latitude__s": gps_coordinates[0] if len(gps_coordinates) > 0 else None,
        "Location_GPS__Longitude__s": gps_coordinates[1] if len(gps_coordinates) > 1 else None,
        "Altitude__c": gps_coordinates[2] if len(gps_coordinates) > 2 else None,
        "No_of_curedas__c": data.get('form', {}).get('opening_questions', {}).get('number_of_curedas'),
        "No_of_separate_coffee_fields__c": data.get('form', {}).get('opening_questions', {}).get('number_of_separate_coffee_fields'),
        "Field_Age__c": data.get('form', {}).get('field_age'),
    }

def process_farm_visit(data, sf_connection):
    """Process farm visit data and upsert to Salesforce."""
    survey_detail = data.get('form', {}).get('@name')
    url_string = f'https://www.commcarehq.org/a/{data.get("domain")}/api/form/attachment/{data.get("form", {}).get("meta", {}).get("instanceID")}/'
    gps_coordinates = (data.get("form", {}).get("gps_coordinates", "") or "").split(" ")
    
    # Farmers present
    farmers_present = data.get('form', {}).get('farm_being_visited', "")
    farmers_list = farmers_present.split(" ") if farmers_present else []
    primary_farmer = farmers_list[0] if len(farmers_list) > 0 else None
    secondary_farmer = farmers_list[1] if len(farmers_list) > 1 else None

    # Generate common fields
    farm_visit_fields = generate_common_farm_visit_fields(data, url_string, gps_coordinates)
    
    if survey_detail == 'Farm Visit Full':
        # FT Level specific fields
        farm_visit_fields.update({
            "Farm_Visited__r": {
                "CommCare_Case_Id__c": data.get('form', {}).get('farm_being_visited')
            },
            "Secondary_Farmer__r": {
                "CommCare_Case_Id__c": data.get('form', {}).get('secondary_farmer')
            }
        })
    elif survey_detail == 'Farm Visit - AA':
        # AA Level specific fields
        farm_visit_fields.update({
            "Primary_Farmer_PIMA_ID__c": primary_farmer,
            "Secondary_Farmer_PIMA_ID__c": secondary_farmer,
            "Farm_Visited__r": {"CommCare_Case_Id__c": primary_farmer},
            "Secondary_Farmer__r": {"CommCare_Case_Id__c": secondary_farmer},
            "FV_AA_Farmer_1_Attended_Any_Training__c": 'Yes' if data.get('form', {}).get('farmer_1_questions', {}).get('attended_training') == 1 else 'No',
            "FV_AA_Farmer_2_Attended_Any_Training__c": 'Yes' if data.get('form', {}).get('farmer_2_questions', {}).get('attended_training') == 1 else 'No' if data.get('form', {}).get('farmer_2_questions', {}).get('attended_training') == 0 else None,
            "FV_AA_Farmer_1_Trainings_Attended__c": data.get('form', {}).get('farmer_1_questions', {}).get('number_of_trainings'),
            "FV_AA_Farmer_2_Trainings_Attended__c": data.get('form', {}).get('farmer_2_questions', {}).get('number_of_trainings'),
            "FV_AA_Visit_Done_By_AA__c": 'Yes'
        })
    
    # Upsert to Salesforce
    upsert_to_salesforce(
        "Farm_Visit__c",
        "FV_Submission_ID__c",
        f'FV-{data.get("id")}',
        farm_visit_fields,
        sf_connection
    )
