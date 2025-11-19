from utils.salesforce_client import upsert_to_salesforce
from utils.logging_config import logger

'''

PART 1: Process farm data

'''


def process_farm(data: dict, sf_connection):
    request_id = data.get("id")
    fis_data = data.get("form", {}).get("field_inventory_survey", {}).get("general_plot_information")
    url_string = f'https://www.commcarehq.org/a/{data.get("domain")}/api/form/attachment/{data.get("form", {}).get("meta", {}).get("instanceID")}/'
    
    # 1. In case there is more than one farm
    if isinstance(fis_data, list):
        print("It is a list")
        for farm in fis_data:
            farm_fields = generate_farm_fields(data, farm, request_id, url_string)
            
            # Upsert to Salesforce
            upsert_to_salesforce(
                "Farm__c",
                "TNS_Id__c",
                f'F-0{farm.get("current_index", "")}-{data.get("form", {}).get("household_tns_id", "")}',
                farm_fields,
                sf_connection
            )
    
    # 2. In case it's only one farm
    elif isinstance(fis_data, dict):
        print("It is a dictionary")
        farm_fields = generate_farm_fields(data, fis_data, request_id, url_string)
        # Upsert to Salesforce
        upsert_to_salesforce(
            "Farm__c",
            "TNS_Id__c",
            f'F-0{fis_data.get("current_index", "")}-{data.get("form", {}).get("household_tns_id", "")}',
            farm_fields,
            sf_connection
        )
    
    # 3. In case there's no FIS
    else:
        logger.info({
            "message": "Skipping farm upserting logic [FIS]",
            "request_id": request_id
        })


'''

PART 2: Process Coffee Varieties' Data

'''

def process_varieties(data: dict, sf_connection):
    request_id = data.get("id")
    fis_data = data.get("form", {}).get("field_inventory_survey", {}).get("general_plot_information")
    if isinstance(fis_data, list):
        for farm in fis_data:
            varieties = farm.get("varieties", "").split(" ")
            for variety in varieties:
                variety_fields = generate_variety_fields(data, farm, variety)
                
                # Upsert to Salesforce
                upsert_to_salesforce(
                    "Coffee_Variety__c",
                    "Name",
                    f'CV-0{variety}-F-0{farm.get("current_index", "")}-{data.get("form", {}).get("household_tns_id", "")}',
                    variety_fields,
                    sf_connection
                )
                
    
    elif isinstance(fis_data, dict):
        varieties = fis_data.get("varieties", "").split(" ")
        for variety in varieties:
            variety_fields = generate_variety_fields(data, fis_data, variety)
            
            # Upsert to Salesforce
            upsert_to_salesforce(
                "Coffee_Variety__c",
                "Name",
                f'CV-0{variety}-F-0{fis_data.get("current_index", "")}-{data.get("form", {}).get("household_tns_id", "")}',
                variety_fields,
                sf_connection
            )

    else: 
        logger.info({
            "message": "Skipping Coffee Variety Upsert Logic [FIS]",
            "request_id": request_id
        })


'''

PART 3: Update Household Data

'''

        
def update_household_fis(data: dict, sf_connection):
    request_id = data.get("id")
    fis_data = data.get("form", {}).get("field_inventory_survey", {}).get("general_plot_information")
    if isinstance(fis_data, (dict, list)):
        household_fields = {
            "FIS_Completed__c": True,
            "Lastest_Visit_with_FIS__r": {
                "FV_Submission_ID__c": f'FV-{data.get("id")}'
            }
        }
        
        # Upsert to Salesforce
        upsert_to_salesforce(
            "Household__c",
            "Household_ID__c",
            f'{data.get("form", {}).get("household_tns_id", "")}',
            household_fields,
            sf_connection
        )
    else:
        logger.info({
            "message": "Skipping Household Upsert Logic [FIS]",
            "request_id": request_id
        })
    

'''

PART 4: Helper functions

'''
# 1. Generate fields for coffee varieties
def generate_variety_fields(data: dict, farm: dict, variety):
    return {
        # "Name": f'CV-0{variety}-F-0{farm.get("current_index", "")}-{data.get("form", {}).get("household_tns_id", "")}',
        "Variety_Type_Name__c": {
            "1": "Costa Rica 95",
            "2": "SL28 or 34",
            "3": "K7",
            "4": "Catimor 129",
            "5": "Catuai",
            "6": "Yellow Catuai",
            "7": "F6",
            "8": "Caturra",
            "9": farm.get("other_variety", "")
        }.get(variety, "") or "",
        "Variety_Number_of_Trees__c": farm.get(f'variety_{variety}', "") or "",
        "Farm__r": {
            "TNS_Id__c": f'F-0{farm.get("current_index", "")}-{data.get("form", {}).get("household_tns_id", "")}'
        }
    }   

# 2. Generate fields for farm/plot
def generate_farm_fields(data: dict, farm: dict, request_id, url_string):
    return {
        "Name": f'0{farm.get("current_index", "")}' or "",
        "Farm_Size_Coffee_Trees__c": farm.get("total_coffee", "") or "",
        "Farm_Size_Land_Measurements__c": farm.get("farm_size_ha", "") or "",
        "Main_Coffee_Field__c": True if farm.get("best_practice_plot", None) in ["1", None] else False, # If there's no value, it means it's the main plot
        "Planted_on_visit_date__c": True if isinstance(farm.get("important_notes_planting_dates", {}), dict) and farm.get("important_notes_planting_dates", {}).get("planting_period_note_same_date_as_visit", "") == "yes" else False,
        "Planted_out_of_season__c": True if isinstance(farm.get("important_notes_planting_dates", {}), dict) and farm.get("important_notes_planting_dates", {}).get("planting_period_note_out_of_season", "") == "yes" else False,
        "Planted_out_of_season_comments__c": isinstance(farm.get("important_notes_planting_dates", {}), dict) and farm.get("important_notes_planting_dates", {}).get("planting_period_comment_out_of_season", "") or "",
        "Planting_Month_and_Year__c": farm.get("date_planted", "") or "",
        "Farm_GPS_Coordinates__Latitude__s": farm.get("final_gps", "").split(" ")[0] or "",
        "Farm_GPS_Coordinates__Longitude__s": farm.get("final_gps", "").split(" ")[1] or "",
        "Altitude__c": farm.get("final_gps", "").split(" ")[2] or "",
        "Farm_Image_URL__c": f'{url_string}{farm.get("plot_photo", "")}' or "",
        "Household__r": {
            "Household_ID__c": data.get("form", {}).get("household_tns_id", "") or ""
        },
        "Latest_Farm_Visit_Record__r": {
            "FV_Submission_ID__c": f'FV-{request_id}'
        }
    }