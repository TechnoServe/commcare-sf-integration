# from app.utils.salesforce_client import SalesforceClient
import asyncio
from utils.salesforce_client import upsert_to_salesforce
from utils.logging_config import logger
# from registration import process_household, process_participant
import os
  
# Process functions for each Salesforce object
def generate_common_farm_visit_fields(data: dict, url_string, gps_coordinates):
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
        "Field_Age__c": data.get('form', {}).get('field_age')
    }

def process_farm_visit(data: dict, sf_connection):
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

def process_best_practices(data: dict, sf_connection):
    farm_visit_type = data.get('form', {}).get('survey_type')
    bp_string = data.get('form', {}) if farm_visit_type == 'Farm Visit Full - ZM' else data.get('form', {}).get('best_practice_questions', {})
    url_string = f'https://www.commcarehq.org/a/{data.get("domain")}/api/form/attachment/{data.get("form", {}).get("meta", {}).get("instanceID")}/'
    
    best_practice_fields = {
        'Name__c': f'FV-{data.get('id')}',
        
        # 1. Nutrition
        'Color_of_coffee_tree_leaves__c': (
            '5% or more (5 or more in 100) of the leaves are yellow, pale green or brown.' if str(bp_string.get('nutrition', {}).get('are_the_leave_green_or_yellow_pale_green')) == '0' else 
            'Nearly all leaves are dark green and less than 5% (less than 5 in 100) are yellow, pale green, or brown.' if str(bp_string.get('nutrition', {}).get('are_the_leave_green_or_yellow_pale_green')) == '0' else 
            None
            ),
        
        # 2. Weeding
        'how_many_weeds_under_canopy_and_how_big__c': {
            '1': "Few small weeds (less than 30cm) under the tree canopy",
            '2': "Many small weeds under the tree canopy (ground is covered with weeds)",
            '3': "Many large weeds under the tree canopy (ground is covered with weeds)"
        }.get(str(bp_string.get('weeding', {}).get('how_many_weeds_under_canopy_and_how_big_are_they'))),
        'Have_herbicides_been_used_on_the_field__c': bp_string.get('weeding', {}).get('used_herbicides'),
        'photo_of_weeds_under_the_canopy__c': f'{url_string}{bp_string.get("weeding", {}).get("weeds_under_the_canopy_photo")}',
        
        # 3. Shade Management
        'level_of_shade_present_on_the_farm__c': {
            '0': "NO shade, less than 5%",
            '1': "Light shade, 5 to 20%",
            '2': "Medium shade, 20 to 40%",
            '3': "Heavy shade, over 40%"
        }.get(str(bp_string.get('shade_control', {}).get('level_of_shade_present_on_the_farm'))),
        'photo_of_level_of_shade_on_the_plot__c': f'{url_string}{bp_string.get("shade_control", {}).get("photo_of_level_of_shade_on_the_plot")}',
        
        # 4. Compost & Manure
        'do_you_have_compost_manure__c': (
            'Yes' if str(bp_string.get('compost', {}).get('do_you_have_compost_manure')) == '1' else
            'No' if str(bp_string.get('compost', {}).get('do_you_have_compost_manure')) == '0' else
            None
            ),
        'photo_of_the_compost_manure__c': f'{url_string}{bp_string.get("compost", {}).get("photo_of_the_compost_manure")}' if str(bp_string.get('compost', {}).get('do_you_have_compost_manure')) == '1' else None,
        
        # 5. Record Keeping
        'do_you_have_a_record_book__c': (
            'Yes' if str(bp_string.get('record_keeping', {}).get('do_you_have_a_record_book')) == '1' else 
            'No' if str(bp_string.get('record_keeping', {}).get('do_you_have_a_record_book')) == '0' else
            None
            ),
        'are_there_records_on_the_record_book__c': (
            'Yes' if str(bp_string.get('record_keeping', {}).get('are_there_records_on_the_record_book')) == '1' else 
            'No' if str(bp_string.get('record_keeping', {}).get('are_there_records_on_the_record_book')) == '0' else
            None
        ),
        'take_a_photo_of_the_record_book__c': f'{url_string}{bp_string.get("record_keeping", {}).get("take_a_photo_of_the_record_book")}' if str(bp_string.get('record_keeping', {}).get('do_you_have_a_record_book')) == '1' else None
        
    }
    
    # Process FV Best Practices for KE exclusive BPs
    if farm_visit_type == 'Farm Visit Full - KE':
        best_practice_fields.update({
            # 1. Pesticide Use
            'used_pesticide__c': 'Yes' if str(bp_string.get('safe_use_of_pesticides', {}).get('used_pesticides')) == '1' else 'No',
            'pesticide_number_of_times__c': bp_string.get('safe_use_of_pesticides', {}).get('pesticide_number_of_times'),
            'pesticide_spray_type__c': (
                'Routine spray' if str(bp_string.get('safe_use_of_pesticides', {}).get('pesticide_spray_type')) == '1' else 
                'After scouting and seeing a pest' if str(bp_string.get('safe_use_of_pesticides', {}).get('pesticide_spray_type')) == '2' else 
                None
                )
        })
    
    # Process FV Best Practices for Zimbabwe (Health of New Planting & Banana Intercrop)
    if farm_visit_type == 'Farm Visit Full - ZM':
        best_practice_fields.update({
            # 1. Health of New Planting
            'health_of_new_planting_choice__c': {
                '1' : 'The majority of trees are green and healthy and have grown well',
                '2' : 'The majority of trees look stressed and growth is slow',
                '3' : 'The majority of trees have dried up or died'
                }.get(str(bp_string.get('health_of_new_planting', {}).get('health_of_new_planting_choice'))) or None,
            
            # 2. Banana Intercrop
            'planted_intercrop_bananas__c': bp_string.get('shade_control', {}).get('planted_intercrop_bananas', '').capitalize(),
            'photograph_intercrop_bananas__c' : f"{url_string}{bp_string.get('shade_control', {}).get('photograph_intercrop_bananas', '')}" if bp_string.get('shade_control', {}).get('planted_intercrop_bananas', '') == 'yes' else ''
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
    
    # Process FV Best Practices for ET exclusive practices
    if farm_visit_type == 'Farm Visit Full - ET':
        best_practice_fields.update({
            
            # 1. Weeding
            'has_coffee_field_been_dug__c': (
                'Yes' if str(bp_string.get('weeding', {}).get('look_has_the_coffee_field_been_dug')) == '1' else
                'No' if str(bp_string.get('weeding', {}).get('look_has_the_coffee_field_been_dug')) == '0' else
                None
                ),
            
            # 2. Stumping (Needs Revision)
            'stumping_method_on_majority_of_trees__c': (
                'Yes' if str(bp_string.get('stumping', {}).get('stumping_methods_used_on_majority_of_trees')) == '1' else
                'No' if str(bp_string.get('stumping', {}).get('stumping_methods_used_on_majority_of_trees')) == '0' else
                None
            ),
            'year_stumping__c': '', # This is confusing
            'number_of_trees_stumped__c': bp_string.get('stumping', {}).get('total_stumped_trees'),
            'main_stems_in_majority_coffee_trees__c':  bp_string.get('stumping', {}).get('look_on_average_how_many_main_stems_are_on_the_stumped_trees_note_to_traine'),
            'photos_of_stumped_coffee_trees__c': f'{url_string}{bp_string.get("stumping", {}).get("photos_of_stumped_coffee_trees")}' if str(bp_string.get('stumping', {}).get('stumping_methods_used_on_majority_of_trees')) == '1' else None,
            
            # 3. Beekeeping (Only HWG)
            'do_you_keep_bees__c': data.get('form', {}).get('question1', {}).get('ask_do_you_keep_bees_if_yes_ask_can_you_show_me_were_you_keep_your_bees') or None,
            'number_of_bee_hives__c': data.get('form', {}).get('question1', {}).get('look_and_ask_how_many_hives_do_you_have_of_each_type') or None,
            'number_of_bee_hives_transitional__c': data.get('form', {}).get('question1', {}).get('look_and_ask_how_many_hives_do_you_have_of_each_type_transitional') or None,
            'number_of_bee_hives_traditional__c': data.get('form', {}).get('question1', {}).get('look_and_ask_how_many_hives_do_you_have_of_each_type_traditional') or None,
            'take_a_photo_showing_the_modern_hives__c': f"{url_string}{data.get('form', {}).get('question1', {}).get('take_a_photo_showing_the_modern_hives_note_to_trainer_dont_get_too_close_to')}" if float(data.get('form', {}).get('question1', {}).get('look_and_ask_how_many_hives_do_you_have_of_each_type')) > 0 else None,
            'take_a_photo_of_the_transitional_hive__c': f"{url_string}{data.get('form', {}).get('question1', {}).get('take_a_photo_showing_the_transitional_hives__note_to_trainer_dont_get_too_c')}" if float(data.get('form', {}).get('question1', {}).get('look_and_ask_how_many_hives_do_you_have_of_each_type_transitional')) > 0 else None,
            'take_a_photo_of_the_traditional_hive__c': f"{url_string}{data.get('form', {}).get('question1', {}).get('take_a_photo_showing_the_traditional_hives__note_to_trainer_dont_get_too_c')}" if float(data.get('form', {}).get('question1', {}).get('look_and_ask_how_many_hives_do_you_have_of_each_type_traditional')) > 0 else None,
            'how_many_years_have_you_been_beekeeping__c': data.get('form', {}).get('question1', {}).get('ask_how_many_years_have_you_been_beekeeping') or None,
            'did_you_start_beekeeping_before_feb_2023__c': data.get('form', {}).get('question1', {}).get('ask_did_you_start_beekeeping_before_february_2023') or None,
            
            # 4. Erosion Control
            'take_a_photo_of_erosion_control__c' : f"{url_string}{bp_string.get('erosion_control', {}).get('photo_of_erosion_control_method')}" if bp_string.get('erosion_control', {}).get('photo_of_erosion_control_method') != None else None
        })
    # Upsert to Salesforce
    upsert_to_salesforce(
        "FV_Best_Practices__c",
        "FV_Submission_ID__c",
        f'FV-{data.get("id")}',
        best_practice_fields,
        sf_connection
    )

# Process Erosion Control Best Practice Result    
def process_best_practice_results_erosion_control(data: dict, sf_connection):
    farm_visit_type = data.get('form', {}).get('survey_type')
    bp_string = data.get('form', {}) if farm_visit_type == 'Farm Visit Full - ZM' else data.get('form', {}).get('best_practice_questions', {})
    results = str(bp_string.get('erosion_control', {}).get('methods_of_erosion_control', '')).split(" ")
    
    for result in results:
        best_practice_result_fields = {
            'FV_Submission_ID__c': f'FV-{data.get('id')}',
            'Best_Practice_Result_Type__c': 'Erosion Control',
            'Best_Practice_Result_Description__c': {
                '1' : 'Grasses such as vetiver planted in rows' if farm_visit_type == 'Farm Visit Full - ET' else 'Stabilizing grasses',
                '2' : 'Mulch',
                '3' : 'Water traps' if farm_visit_type == 'Farm Visit Full - ET' else 'Water traps or trenches',
                '4' : 'Physical barriers. (e.g. rocks)',
                '5' : 'Terraces',
                '6' : 'Contour planting',
                '7' : 'Bean or Arachis cover crop between the rows',
                '0' : 'No erosion control method seen'
            }.get(result, 'Unknown Result')
        }
        
        # Upsert to Salesforce
        upsert_to_salesforce(
            "FV_Best_Practice_Results__c",
            "Best_Practice_Result_Submission_ID__c",
            f'FVBPN-{data.get("id")}_erosion_{result}',
            best_practice_result_fields,
            sf_connection
        )
        
        
# Process Chemicals and Fertilizers Best Practice Result    
def process_best_practice_results_chemicals_and_fertilizers(data: dict, sf_connection):
    farm_visit_type = data.get('form', {}).get('survey_type')
    bp_string = data.get('form', {}) if farm_visit_type == 'Farm Visit Full - ZM' else data.get('form', {}).get('best_practice_questions', {})
    results = str(bp_string.get('nutrition', {}).get('type_chemical_applied_on_coffee_last_12_months', '')).split(" ")
    
    for result in results:
        best_practice_result_fields = {
            'FV_Submission_ID__c': f'FV-{data.get('id')}',
            'Best_Practice_Result_Type__c': 'Chemicals and Fertilizers Applied',
            'Best_Practice_Result_Description__c': {
                '1' : (
                    'Compost, homemade or pulp compost' if farm_visit_type == 'Farm Visit Full - ET' else
                    'NPK 10:10:5-10' if farm_visit_type == 'Farm Visit Full - PR' else
                    'Compost'
                    ),
                '2' : (
                    'NPK 10:10:5-10' if farm_visit_type == 'Farm Visit Full - PR' else
                    'Manure'
                    ),
                '3' : (
                    'NPK 22:6:12' if farm_visit_type == 'Farm Visit Full - KE' else
                    'Lime' if farm_visit_type == 'Farm Visit Full - ZM' else 
                    'NPK 10:5:15-20' if farm_visit_type == 'Farm Visit Full - PR' else
                    None
                    ),
                '4' : (
                    'NPK 17:17:17' if farm_visit_type == 'Farm Visit Full - KE' else
                    'Compound S' if farm_visit_type == 'Farm Visit Full - ZM' else 
                    'NPK 10:5:15-20' if farm_visit_type == 'Farm Visit Full - PR' else
                    None
                    ),
                '5' : (
                    'Other NPK' if farm_visit_type == 'Farm Visit Full - KE' else
                    'Compound J' if farm_visit_type == 'Farm Visit Full - ZM' else 
                    'NPK 10:5:15-20' if farm_visit_type == 'Farm Visit Full - PR' else
                    None
                    ),
                '6' : (
                    'Zinc/Boron Foliar feed' if farm_visit_type == 'Farm Visit Full - KE' else
                    'Single Super Phosphate (SSP)' if farm_visit_type == 'Farm Visit Full - ZM' else 
                    'NPK 10:5:15-20' if farm_visit_type == 'Farm Visit Full - PR' else
                    None
                    ),
                '7' : (
                    'General Foliar feed' if farm_visit_type == 'Farm Visit Full - KE' else
                    'Zinc/Boron Foliar Feed (Tracel)' if farm_visit_type == 'Farm Visit Full - ZM' else 
                    'NPK 10:5:15-20' if farm_visit_type == 'Farm Visit Full - PR' else
                    None
                    ),
                '8' : (
                    'LIME' if farm_visit_type == 'Farm Visit Full - KE' else
                    'Ammonium Nitrate' if farm_visit_type == 'Farm Visit Full - ZM' else 
                    'NPK 15:5:10-19' if farm_visit_type == 'Farm Visit Full - PR' else
                    None
                    ),
                '9' : (
                    'NPK 15:5:10-19' if farm_visit_type == 'Farm Visit Full - PR' else
                    'CAN'
                    ),
                '10' : (
                    'NPK 15:5:10-19' if farm_visit_type == 'Farm Visit Full - PR' else
                    'Urea'
                    ),
                '11' : (
                    'NPK 15:15:15' if farm_visit_type == 'Farm Visit Full - PR' else None
                    ),
                '12' : (
                    'NPK 20:5:10-20' if farm_visit_type == 'Farm Visit Full - PR' else None
                    ),
                '13' : (
                    'NPK 20:5:10-20' if farm_visit_type == 'Farm Visit Full - PR' else None
                    ),
                '14' : (
                    'DAP' if farm_visit_type == 'Farm Visit Full - PR' else None
                    ),
                '15' : (
                    'Urea' if farm_visit_type == 'Farm Visit Full - PR' else None
                    ),
                '16' : (
                    'Compost or Manure' if farm_visit_type == 'Farm Visit Full - PR' else None
                    ),
                '17' : (
                    'Agricultural Lime - Calcium Carbonate' if farm_visit_type == 'Farm Visit Full - PR' else None
                    ),
                '18' : (
                    'Nutrical (cal dolomita)' if farm_visit_type == 'Farm Visit Full - PR' else None
                    ),
                '19' : (
                    'Foliar Zinc or Boron' if farm_visit_type == 'Farm Visit Full - PR' else None
                    ),
                '20' : (
                    'General Foliar Feed (Nurish, Ferquido Ferqan)' if farm_visit_type == 'Farm Visit Full - PR' else None
                    ),
                '0' : (
                    'Did not apply any fertilizer in past 12 months' if farm_visit_type == 'Farm Visit Full - ET' else
                    'Did NOT apply any fertilizer in past 12 months'
                    )
            }.get(result, 'Unknown Result')
        }
        
        # Upsert to Salesforce
        upsert_to_salesforce(
            "FV_Best_Practice_Results__c",
            "Best_Practice_Result_Submission_ID__c",
            f'FVBPN-{data.get("id")}_fertilizer_{result}',
            best_practice_result_fields,
            sf_connection
        )

# Process Coffee Berry Borer Best Practice Results
def process_best_practice_results_cbb(data: dict, sf_connection):
    farm_visit_type = data.get('form', {}).get('survey_type')
    bp_string = data.get('form', {}) if farm_visit_type == 'Farm Visit Full - ZM' else data.get('form', {}).get('best_practice_questions', {})
    results = str(bp_string.get('pest_disease_management', {}).get('methods_of_controlling_white_stem_borer', '')).split(" ") if farm_visit_type == 'Farm Visit Full - ET' else str(bp_string.get('pest_disease_management', {}).get('methods_of_controlling_coffee_berry_borer', '')).split(" ")
    
    for result in results:
        best_practice_result_fields = {
            'FV_Submission_ID__c': f'FV-{data.get('id')}',
            'Best_Practice_Result_Type__c': 'Management of Coffee Berry Borer (CBB)',
            'Best_Practice_Result_Description__c' : {
                '1' : (
                    'Reduce pesticide use and/or encourage natural predators and parasites - beneficial insects.' if farm_visit_type in ['Farm Visit Full - KE', 'Farm Visit Full - ZM'] else
                    'Encourage natural predators and parasites' if farm_visit_type == 'Farm Visit Full - ET' else
                    'Reduce pesticide use and encourage natural predators' if farm_visit_type == 'Farm Visit Full - PR' else
                    None
                    ),
                '2' : (
                    'Strip all berries at the end of harvest, known as crop hygiene' if farm_visit_type in ['Farm Visit Full - KE', 'Farm Visit Full - ZM'] else
                    'Strip all berries at the end of the harvest or collect fallen berries' if farm_visit_type == 'Farm Visit Full - ET' else
                    'Strip all berries at the end of harvest' if farm_visit_type == 'Farm Visit Full - PR' else
                    None
                    ),
                '3' : (
                    'Harvest ripe cherries regularly - to reduce pest and disease levels' if farm_visit_type in ['Farm Visit Full - KE', 'Farm Visit Full - ZM'] else
                    'Harvest ripe cherries regularly' if farm_visit_type in ['Farm Visit Full - ET', 'Farm Visit Full - PR'] else
                    None
                    ),
                '4' : (
                    'Use berry borer traps' if farm_visit_type in ['Farm Visit Full - KE', 'Farm Visit Full - ZM', 'Farm Visit Full - ET'] else
                    'Collect fallen berries' if farm_visit_type == 'Farm Visit Full - PR' else
                    None
                    ),
                '5' : (
                    'Collect fallen berries at the end of the season - crop hygiene' if farm_visit_type in ['Farm Visit Full - KE', 'Farm Visit Full - ZM'] else
                    'Use compost or manure, to keep the tree healthy' if farm_visit_type == 'Farm Visit Full - ET' else
                    'Use berry borer traps' if farm_visit_type == 'Farm Visit Full - PR' else
                    None
                    ),
                '6' : (
                    'Feed the tree well to keep it healthy' if farm_visit_type in ['Farm Visit Full - KE', 'Farm Visit Full - ZM'] else
                    'Use good agricultural practices such as weeding or mulching to reduce stress and keep trees healthy' if farm_visit_type == 'Farm Visit Full - ET' else
                    'Spray pesticides' if farm_visit_type == 'Farm Visit Full - PR' else
                    None
                    ),
                '7' : (
                    'Use good agricultural practices such as weeding or mulching to reduce stress and keep trees healthy' if farm_visit_type in ['Farm Visit Full - KE', 'Farm Visit Full - ZM'] else
                    'Stump old coffee' if farm_visit_type == 'Farm Visit Full - ET' else
                    None
                    ),
                '8' : (
                    'Prune to keep the canopy open' if farm_visit_type in ['Farm Visit Full - KE', 'Farm Visit Full - ZM'] else
                    'Plant disease resistant varieties' if farm_visit_type == 'Farm Visit Full - ET' else
                    None
                    ),
                '9' : (
                    'Renovate (new planting or grafting) or rejuvenate regularly to keep main stems less than 8 years old' if farm_visit_type == 'Farm Visit Full - KE' else
                    'Renovate (new planting) or rejuvenate regularly to keep main stems less than 8 years old' if farm_visit_type == 'Farm Visit Full - ZM' else
                    'Prune to keep the canopy open' if farm_visit_type == 'Farm Visit Full - ET' else
                    None
                    ),
                '10' : (
                    'Plant and grow disease resistant varieties e.g. Ruiru 11 and Batian' if farm_visit_type == 'Farm Visit Full - KE' else
                    'Plant and grow disease resistant varieties' if farm_visit_type == 'Farm Visit Full - ZM' else
                    'Uproot wilt infected coffee trees and burn' if farm_visit_type == 'Farm Visit Full - ET' else
                    None
                    ),
                '11' : (
                    'Smooth the bark to reduce egg laying sites for While Coffee Borer' if farm_visit_type in ['Farm Visit Full - KE', 'Farm Visit Full - ZM'] else
                    'Uproot wilt infected coffee trees and burn' if farm_visit_type == 'Farm Visit Full - ET' else
                    None
                    ),
                '12' : (
                    'Spray regular pesticides' if farm_visit_type in ['Farm Visit Full - KE', 'Farm Visit Full - ZM'] else
                    None
                    ),
                '13' : (
                    'Spray homemade herbal or botanical pesticides' if farm_visit_type in ['Farm Visit Full - KE', 'Farm Visit Full - ZM'] else
                    None
                    ),
                '0' : 'Does not know any methods'
            }.get(result, 'Unknown Result')
        }
        # Upsert to Salesforce
        upsert_to_salesforce(
            "FV_Best_Practice_Results__c",
            "Best_Practice_Result_Submission_ID__c",
            f'FVBPN-{data.get("id")}_cbb_{result}',
            best_practice_result_fields,
            sf_connection
        )

# Process Coffee Leaf Rust Best Practice Results (Only Puerto Rico)       
def process_best_practice_results_clr(data: dict, sf_connection):
    farm_visit_type = data.get('form', {}).get('survey_type')
    bp_string = data.get('form', {}) if farm_visit_type == 'Farm Visit Full - ZM' else data.get('form', {}).get('best_practice_questions', {})
    results = str(bp_string.get('pest_disease_management', {}).get('methods_of_controlling_coffee_leaf_rust', '')).split(" ")
    if farm_visit_type == 'Farm Visit Full - PR':
        for result in results:
            best_practice_result_fields = {
                'FV_Submission_ID__c': f'FV-{data.get('id')}',
                'Best_Practice_Result_Type__c': 'Management of Coffee Leaf Rust (CLR)',
                'Best_Practice_Result_Description__c' : {
                    '1' : 'Feed the tree well to keep it healthy',
                    '2' : 'Use good agricultural practices such as weeding or mulching to reduce stress and keep trees healthy',
                    '3' : 'Prune or keep canopy open',
                    '4' : 'Spray fungicides',
                    '5' : 'Grow resistant varieties',
                    '0' : 'Does not know any methods'
                }.get(result, 'Unknown Result')
            }
            # Upsert to Salesforce
            upsert_to_salesforce(
                "FV_Best_Practice_Results__c",
                "Best_Practice_Result_Submission_ID__c",
                f'FVBPN-{data.get("id")}_clr_{result}',
                best_practice_result_fields,
                sf_connection
            )
        
    else: None

# Process Pruning Best Practice Results    
def process_best_practice_results_pruning(data: dict, sf_connection):
    farm_visit_type = data.get('form', {}).get('survey_type')
    bp_string = data.get('form', {}) if farm_visit_type == 'Farm Visit Full - ZM' else data.get('form', {}).get('best_practice_questions', {})
    results = str(bp_string.get('pruning', {}).get('pruning_method_on_majority_trees', '')).split(" ")
    field_age = float(data.get('form', {}).get('field_age', 0))
    if farm_visit_type in ['Farm Visit Full - PR', 'Farm Visit Full - ZM', 'Farm Visit Full - KE']:
        for result in results:
            best_practice_result_fields = {
                'FV_Submission_ID__c': f'FV-{data.get('id')}',
                'Best_Practice_Result_Type__c': 'Pruning',
                'Best_Practice_Result_Description__c' : (
                    'N/A' if farm_visit_type in ['Farm Visit Full - PR', 'Farm Visit Full - ZM'] and field_age < 3 else 
                    {
                        '1' : 'Centers opened',
                        '2' : 'Unwanted suckers removed',
                        '3' : 'Dead branches removed',
                        '4' : 'Branches touching the ground removed',
                        '5' : 'Broken / unproductive stems and/or branches removed',
                        '0' : 'No pruning methods used'
                    }.get(result, 'Unknown Result')
                )
            }
            # Upsert to Salesforce
            upsert_to_salesforce(
                "FV_Best_Practice_Results__c",
                "Best_Practice_Result_Submission_ID__c",
                f'FVBPN-{data.get("id")}_pruning_{result}',
                best_practice_result_fields,
                sf_connection
            )
        
    else: None
    
# Process Weeding Best Practice Results
def process_best_practice_results_pruning(data: dict, sf_connection):
    farm_visit_type = data.get('form', {}).get('survey_type')
    bp_string = data.get('form', {}) if farm_visit_type == 'Farm Visit Full - ZM' else data.get('form', {}).get('best_practice_questions', {})
    results = str(bp_string.get('weeding', {}).get('which_product_have_you_used', '')).split(" ")
    if farm_visit_type in ['Farm Visit Full - ZM', 'Farm Visit Full - KE', 'Farm Visit Full - PR']:
        for result in results:
            best_practice_result_fields = {
                'FV_Submission_ID__c': f'FV-{data.get('id')}',
                'Best_Practice_Result_Type__c': 'Weeding',
                'Best_Practice_Result_Description__c' : {
                    '1' : 'Glyphosate (Eg Round Up)' if farm_visit_type in ['Farm Visit Full - ZM', 'Farm Visit Full - KE'] else
                    'Glyphosate (Honcho Plus, Round Up Ultra, GlyStar Plus, Credit, Compare and Save, TouchDown)' if farm_visit_type == 'Farm Visit Full - PR' else
                    None,
                    '2' : 'Paraquat (Eg. Gramoxone)' if farm_visit_type in ['Farm Visit Full - ZM', 'Farm Visit Full - KE'] else
                    'Paraquat (Gramaxon, Parazone, Parashot)' if farm_visit_type == 'Farm Visit Full - PR' else
                    None,
                    '3' : bp_string.get('weeding', {}).get('ask_which_other_product_have_you_used') if farm_visit_type in ['Farm Visit Full - ZM', 'Farm Visit Full - KE'] else
                    None,
                }.get(result, 'Unknown Result')
            }
            # Upsert to Salesforce
            upsert_to_salesforce(
                "FV_Best_Practice_Results__c",
                "Best_Practice_Result_Submission_ID__c",
                f'FVBPN-{data.get("id")}_weeding_{result}',
                best_practice_result_fields,
                sf_connection
            )
    else: None
    
# Process Pesticide Use Pest Problems Best Practice Results (For PR Exclusively)
def process_best_practice_results_pesticide_use_pest_problems(data: dict, sf_connection):
    farm_visit_type = data.get('form', {}).get('survey_type')
    bp_string = data.get('form', {}) if farm_visit_type == 'Farm Visit Full - ZM' else data.get('form', {}).get('best_practice_questions', {})
    results = str(bp_string.get('pesticide_use', {}).get('which_pests_cause_you_problems', '')).split(" ")
    if farm_visit_type == 'Farm Visit Full - PR':
        for result in results:
            best_practice_result_fields = {
                'FV_Submission_ID__c': f'FV-{data.get('id')}',
                'Best_Practice_Result_Type__c': 'Pest Problems',
                'Best_Practice_Result_Description__c' : {
                    '1' : 'Leaf miner',
                    '2' : 'Coffee Berry Borer',
                    '3' : 'Scles and Mealy bugs',
                    '4' : 'None pest issue'
                }.get(result, 'Unknown Result')
            }
            # Upsert to Salesforce
            upsert_to_salesforce(
                "FV_Best_Practice_Results__c",
                "Best_Practice_Result_Submission_ID__c",
                f'FVBPN-{data.get("id")}_pesticide_use_problems_{result}',
                best_practice_result_fields,
                sf_connection
            )
    else: None

# Process Pesticide Use: Pesticides Best Practice Results (For PR Exclusively)   
def process_best_practice_results_pesticide_use_sprays(data: dict, sf_connection):
    farm_visit_type = data.get('form', {}).get('survey_type')
    bp_string = data.get('form', {}) if farm_visit_type == 'Farm Visit Full - ZM' else data.get('form', {}).get('best_practice_questions', {})
    results = str(bp_string.get('pesticide_use', {}).get('do_you_spray_any_of_the_following_on_your_farm_for_leaf_miner_or_rust', '')).split(" ")
    if farm_visit_type == 'Farm Visit Full - PR':
        for result in results:
            best_practice_result_fields = {
                'FV_Submission_ID__c': f'FV-{data.get('id')}',
                'Best_Practice_Result_Type__c': 'Pest Sprays',
                'Best_Practice_Result_Description__c' : {
                    '1' : 'Products containing Imidacloprid',
                    '2' : 'Products containing Propiconazole',
                    '3' : 'None of the products used'
                }.get(result, 'Unknown Result')
            }
            # Upsert to Salesforce
            upsert_to_salesforce(
                "FV_Best_Practice_Results__c",
                "Best_Practice_Result_Submission_ID__c",
                f'FVBPN-{data.get("id")}_pesticide_use_sprays_{result}',
                best_practice_result_fields,
                sf_connection
            )
    else: None