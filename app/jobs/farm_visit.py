# from app.utils.salesforce_client import SalesforceClient
import asyncio
from utils.salesforce_client import upsert_to_salesforce
from utils.logging_config import logger
# from registration import process_household, process_participant
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

def process_best_practices(data, sf_connection):
    farm_visit_type = data.get('form', {}).get('survey_type')
    bp_string = data.get('form', {}) if farm_visit_type == 'Farm Visit Full - ZM' else data.get('form', {}).get('best_practice_questions')
    url_string = f'https://www.commcarehq.org/a/{data.get("domain")}/api/form/attachment/{data.get("form", {}).get("meta", {}).get("instanceID")}/'
    
    best_practice_fields = {
        'Name__c': f'FV-{data.get('id')}',
        
        # 1. Nutrition
        'Color_of_coffee_tree_leaves__c': (
            '5% or more (5 or more in 100) of the leaves are yellow, pale green or brown.' if bp_string.get('nutrition', {}).get('are_the_leave_green_or_yellow_pale_green') == 0 else 
            'Nearly all leaves are dark green and less than 5% (less than 5 in 100) are yellow, pale green, or brown.' if bp_string.get('nutrition', {}).get('are_the_leave_green_or_yellow_pale_green') == 0 else 
            None
            ),
        
        # 2. Weeding
        'how_many_weeds_under_canopy_and_how_big__c': {
            1: "Few small weeds (less than 30cm) under the tree canopy",
            2: "Many small weeds under the tree canopy (ground is covered with weeds)",
            3: "Many large weeds under the tree canopy (ground is covered with weeds)"
        }.get(bp_string.get('weeding', {}).get('how_many_weeds_under_canopy_and_how_big_are_they')),
        'Have_herbicides_been_used_on_the_field__c': bp_string.get('weeding', {}).get('used_herbicides'),
        'photo_of_weeds_under_the_canopy__c': f'{url_string}{bp_string.get("weeding", {}).get("weeds_under_the_canopy_photo")}',
        
        # 3. Shade Management
        'level_of_shade_present_on_the_farm__c': {
            0: "NO shade, less than 5%",
            1: "Light shade, 5 to 20%",
            2: "Medium shade, 20 to 40%",
            3: "Heavy shade, over 40%"
        }.get(bp_string.get('shade_control', {}).get('level_of_shade_present_on_the_farm')),
        'photo_of_level_of_shade_on_the_plot__c': f'{url_string}{bp_string.get("shade_control", {}).get("photo_of_level_of_shade_on_the_plot")}',
        
        # 4. Compost & Manure
        'do_you_have_compost_manure__c': (
            'Yes' if bp_string.get('compost', {}).get('do_you_have_compost_manure') == 1 else
            'No' if bp_string.get('compost', {}).get('do_you_have_compost_manure') == 0 else
            None
            ),
        'photo_of_the_compost_manure__c': f'{url_string}{bp_string.get("compost", {}).get("photo_of_the_compost_manure")}' if bp_string.get('compost', {}).get('do_you_have_compost_manure') == 1 else None,
        
        # 5. Record Keeping
        'do_you_have_a_record_book__c': (
            'Yes' if bp_string.get('record_keeping', {}).get('do_you_have_a_record_book') == 1 else 
            'No' if bp_string.get('record_keeping', {}).get('do_you_have_a_record_book') == 0 else
            None
            ),
        'are_there_records_on_the_record_book__c': (
            'Yes' if bp_string.get('record_keeping', {}).get('are_there_records_on_the_record_book') == 1 else 
            'No' if bp_string.get('record_keeping', {}).get('are_there_records_on_the_record_book') == 0 else
            None
        ),
        'take_a_photo_of_the_record_book__c': f'{url_string}{bp_string.get("record_keeping", {}).get("take_a_photo_of_the_record_book")}' if bp_string.get('record_keeping', {}).get('do_you_have_a_record_book') == 1 else None
        
    }
    
    if farm_visit_type == 'Farm Visit Full - KE':
        best_practice_fields.update({
            # 1. Pesticide Use
            'used_pesticide__c': 'Yes' if bp_string.get('safe_use_of_pesticides', {}).get('used_pesticides') == 1 else 'No',
            'pesticide_number_of_times__c': bp_string.get('safe_use_of_pesticides', {}).get('pesticide_number_of_times'),
            'pesticide_spray_type__c': (
                'Routine spray' if bp_string.get('safe_use_of_pesticides', {}).get('pesticide_spray_type') == 1 else 
                'After scouting and seeing a pest' if bp_string.get('safe_use_of_pesticides', {}).get('pesticide_spray_type') == 2 else 
                None
                )
        })
    
    # Process FV Best Practices that aren't in ET (Mainstems & Erosion Control Photos)
    if farm_visit_type in ['Farm Visit Full - KE', 'Farm Visit Full - ZM', 'Farm Visit Full - PR']:
        best_practice_fields.update({
            # 1. Main Stems
            'number_of_main_stems_on_majority_trees__c': bp_string.get('mainstems', {}).get('number_of_main_stems_on_majority_trees'),
            'photo_of_trees_and_average_main_stems__c':f'{url_string}{bp_string.get("mainstems", {}).get("trees_and_main_stems_photo")}',
            
            # 2. Erosion Control Photos
            'Stabilizing_Grasses_Photo_URL__c': f'{url_string}{bp_string.get("erosion_control", {}).get("stabilizing_grasses_image")}' if bp_string.get("erosion_control", {}).get("stabilizing_grasses_image") != None else None,
            'Mulch_Photo_URL__c': f'{url_string}{bp_string.get("erosion_control", {}).get("mulch_image")}' if bp_string.get("erosion_control", {}).get("mulch_image") != None else None,
            'Water_Traps_Photo_URL__c': f'{url_string}{bp_string.get("erosion_control", {}).get("water_traps_or_trenches_image")}' if bp_string.get("erosion_control", {}).get("water_traps_or_trenches_image") != None else None,
            'Physical_Barriers_Photo_URL__c': f'{url_string}{bp_string.get("erosion_control", {}).get("physical_barriers_image")}' if bp_string.get("erosion_control", {}).get("physical_barriers_image") != None else None,
            'Terraces_Photo_URL__c': f'{url_string}{bp_string.get("erosion_control", {}).get("terraces_image")}' if bp_string.get("erosion_control", {}).get("terraces_image") != None else None,
            'Contour_Planting_Photo_URL__c': f'{url_string}{bp_string.get("erosion_control", {}).get("contour_planting_image")}' if bp_string.get("erosion_control", {}).get("contour_planting_image") != None else None,
            'Cover_Crop_Photo_URL__c': f'{url_string}{bp_string.get("erosion_control", {}).get("bean_or_arachis_cover_crop_image")}' if bp_string.get("erosion_control", {}).get("bean_or_arachis_cover_crop_image") != None else None
        })
    
    if farm_visit_type == 'Farm Visit Full - ET':
        best_practice_fields.update({
            
            # 1. Weeding
            'has_coffee_field_been_dug__c': (
                'Yes' if bp_string.get('weeding', {}).get('look_has_the_coffee_field_been_dug') == 1 else
                'No' if bp_string.get('weeding', {}).get('look_has_the_coffee_field_been_dug') == 0 else
                None
                ),
            
            # 2. Stumping (Needs Revision)
            'stumping_method_on_majority_of_trees__c': (
                'Yes' if bp_string.get('stumping', {}).get('stumping_methods_used_on_majority_of_trees') == 1 else
                'No' if bp_string.get('stumping', {}).get('stumping_methods_used_on_majority_of_trees') == 0 else
                None
            ),
            'year_stumping__c': '', # This is confusing
            'number_of_trees_stumped__c': bp_string.get('stumping', {}).get('total_stumped_trees'),
            'main_stems_in_majority_coffee_trees__c':  bp_string.get('stumping', {}).get('look_on_average_how_many_main_stems_are_on_the_stumped_trees_note_to_traine'),
            'photos_of_stumped_coffee_trees__c': f'{url_string}{bp_string.get("stumping", {}).get("photos_of_stumped_coffee_trees")}' if bp_string.get('stumping', {}).get('stumping_methods_used_on_majority_of_trees') == 1 else None,
            
            # 3. Beekeeping (Only HWG)
            'do_you_keep_bees__c': data.get('form', {}).get('question1', {}).get('ask_do_you_keep_bees_if_yes_ask_can_you_show_me_were_you_keep_your_bees') or None,
            'number_of_bee_hives__c': data.get('form', {}).get('question1', {}).get('look_and_ask_how_many_hives_do_you_have_of_each_type') or None,
            'number_of_bee_hives_transitional__c': data.get('form', {}).get('question1', {}).get('look_and_ask_how_many_hives_do_you_have_of_each_type_transitional') or None,
            'number_of_bee_hives_traditional__c': data.get('form', {}).get('question1', {}).get('look_and_ask_how_many_hives_do_you_have_of_each_type_traditional') or None,
            'take_a_photo_showing_the_modern_hives__c': f"{url_string}{data.get('form', {}).get('question1', {}).get('take_a_photo_showing_the_modern_hives_note_to_trainer_dont_get_too_close_to')}" if data.get('form', {}).get('question1', {}).get('look_and_ask_how_many_hives_do_you_have_of_each_type') > 0 else None,
            'take_a_photo_of_the_transitional_hive__c': f"{url_string}{data.get('form', {}).get('question1', {}).get('take_a_photo_showing_the_transitional_hives__note_to_trainer_dont_get_too_c')}" if data.get('form', {}).get('question1', {}).get('look_and_ask_how_many_hives_do_you_have_of_each_type_transitional') > 0 else None,
            'take_a_photo_of_the_traditional_hive__c': f"{url_string}{data.get('form', {}).get('question1', {}).get('take_a_photo_showing_the_traditional_hives__note_to_trainer_dont_get_too_c')}" if data.get('form', {}).get('question1', {}).get('look_and_ask_how_many_hives_do_you_have_of_each_type_traditional') > 0 else None,
            'how_many_years_have_you_been_beekeeping__c': data.get('form', {}).get('question1', {}).get('ask_how_many_years_have_you_been_beekeeping') or None,
            'did_you_start_beekeeping_before_feb_2023__c': data.get('form', {}).get('question1', {}).get('ask_did_you_start_beekeeping_before_february_2023') or None
        })