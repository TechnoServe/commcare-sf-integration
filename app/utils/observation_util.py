import asyncio
from utils.salesforce_client import upsert_to_salesforce
from utils.logging_config import logger
import os

# Process functions for each Salesforce object

# 1. Process Training Observation Object
def process_training_observation(data: dict, sf_connection):
    url_string = f'https://www.commcarehq.org/a/{data.get("domain")}/api/form/attachment/{data.get("form", {}).get("meta", {}).get("instanceID")}/'
    gps_coordinates = data.get("form", {}).get("meta", {}).get("location", {}).get("#text", "") or data.get("form", {}).get("gps_information", {}).get("gps_location", "")
    
    gps_parts = gps_coordinates.strip().split(" ")
    
    if len(gps_parts) == 4:
        lat, lon, alt, accuracy = gps_parts
    else:
        lat, lon, alt, accuracy = "", "", "", ""
    
    observation_fields = {
        "Observer__c": data.get("form", {}).get("Observer", ""),
        "Trainer__c": data.get("form", {}).get("trainer_salesforce_id", ""),
        "Training_Group__c": data.get("form", {}).get("Which_Group_Is_Farmer_Trainer_Teaching", ""),
        "Training_Session__c": data.get("form", {}).get("selected_session", ""),
        "RecordTypeId": data.get("form", {}).get("Record_Type", ""),
        "Date__c": data.get("form", {}).get("Date", ""),
        "Male_Participants__c": int(data.get("form", {}).get("Current_session_participants", {}).get("Male_Participants_In_Attendance", "")),
        "Female_Participants__c": int(data.get("form", {}).get("Current_session_participants", {}).get("Female_Participants_In_Attendance", "")),
        "Number_of_Participants__c": int(data.get("form", {}).get("Current_session_participants", {}).get("Total_Participants_In_Attendance", "")),
        "Shared_Action_Plan__c": data.get("form", {}).get("Feedback_And_Coaching_With_The_Farmer_Trainer", {}).get("Share_Action_Plan", "") or None,
        "Shared_Action_Plan_Comments__c": data.get("form", {}).get("Feedback_And_Coaching_With_The_Farmer_Trainer", {}).get("Share_Action_Plan_Comments", "") or None,
        "Did_Well__c": data.get("form", {}).get("Current_Session_Review", {}).get("Did_Well", ""),
        "To_Improve__c": data.get("form", {}).get("Current_Session_Review", {}).get("To_Improve", ""),
        "Photo_of_Facilitator_URL__c": url_string + data.get("form", {}).get("Photo", ""),
        "Farmer_Trainer_Signature__c": url_string + data.get("form", {}).get("Farmer_Trainer_Signature_Section", {}).get("Farmer_Trainer_Signature", ""),
        "Observer_Signature__c": url_string + data.get("form", {}).get("Observer_Signature_Section", {}).get("Observer_Signature", ""),
        "Observation_Location__Latitude__s": lat,
        "Observation_Location__Longitude__s": lon,
        "Altitude__c": alt
    }
    upsert_to_salesforce(
        "Observation__c",
        "Submission_ID__c",
        data.get("id"),
        observation_fields,
        sf_connection
    )

# 2. Process Observation Results Object - Participant Feedback
def process_training_observation_results_participant(data: dict, sf_connection):
    participant_feedback_criteria = {
        'coffee_prepare_and_implement_agronomy_practice': 'Prepare_And_Implement_Agronomy_Practice', 
        'coffee_teaching_clarity_and_effectiveness': 'Teaching_Clarity_And_Effectiveness', 
        'coffee_knowledge_of_trainer_on_agronomy': 'Knowledge_Of_Trainer_On_Agronomy'
        }
    for criteria, criteria_string in participant_feedback_criteria.items():
        
        for participant in range(1,4):
            participant_string = ''
            if participant == 1:
                participant_string = "Participant_One_Feedback"
            elif participant == 2:
                participant_string = "Participant_Two_Feedback"
            elif participant == 3:
                participant_string = "Participant_Three_Feedback"
                
            observation_results_fields = {
                "Observation__r": {
                    "Submission_ID__c": data.get("id", "")
                },
                "RecordTypeId": "01224000000gQe6AAE",
                "Observation_Criterion__r": {
                    "Unique_Name__c": criteria
                },
                "Participant_Sex__c": data.get("form", {}).get(participant_string, {}).get("Participant_Gender", ""),
                "Result__c": data.get("form", {}).get(participant_string, {}).get(criteria_string, ""),
                "Comments__c": data.get("form", {}).get(participant_string, {}).get("participant_comments", ""),
            }
            upsert_to_salesforce(
                "Observation_Result__c",
                "Submission_ID__c",
                f'{data.get("id")}{criteria}-p{participant}',
                observation_results_fields,
                sf_connection
            )

# 3. Process Observation Results Object - Observer Feedback
def process_training_observation_results_observer(data: dict, sf_connection):
    observer_feedback_criteria = {
        'coffee_shows_professionalism': 'Shows_Professionalism',
        'coffee_is_prepared_and_organized': 'Is_Prepared_and_Organized',
        'coffee_engages_participants': 'Engages_Participants',
        'coffee_facilitates_openings_and_closings': 'Facilitates_Openings_and_Closings',
        'coffee_leads_activities': 'Leads_Activities',
        'coffee_leads_discussions': 'Leads_Discussions',
        'coffee_follows_lesson_plans': 'Follows_Lesson_Plans',
        'coffee_manages_time': 'Manages_Time'
        }
    
    for criteria, criteria_string in observer_feedback_criteria.items():
        observation_results_fields = {
            'Observation__r': {
                'Submission_ID__c': data.get('id', '')
            },
            'RecordTypeId': '01224000000gQe7AAE',
            'Observation_Criterion__r': {
                'Unique_Name__c': criteria
            },
            'Result__c': data.get('form', {}).get('Ratings_and_Comments', {}).get(criteria_string, ""),
            'Comments__c': data.get('form', {}).get('Ratings_and_Comments', {}).get(f'{criteria_string}_Comments', "")
        }
        upsert_to_salesforce(
            "Observation_Result__c",
            "Submission_ID__c",
            f'{data.get("id", "")}{criteria}',
            observation_results_fields,
            sf_connection
        )
        
# 4. Process Demo Plot Observation
def process_demoplot_observation(data: dict, sf_connection):
    url_string = f'https://www.commcarehq.org/a/{data.get("domain")}/api/form/attachment/{data.get("form", {}).get("meta", {}).get("instanceID")}/'
    gps_coordinates = data.get("form", {}).get("meta", {}).get("location", {}).get("#text", "") or data.get("form", {}).get("gps_information", {}).get("gps_location", "")
    
    gps_parts = gps_coordinates.strip().split(" ")
    
    if len(gps_parts) == 4:
        lat, lon, alt, accuracy = gps_parts
    else:
        lat, lon, alt, accuracy = "", "", "", "" 
    
    observation_fields = {
        "Observer__c": data.get("form", {}).get("observer", ""),
        "Trainer__c": data.get("form", {}).get("trainer", ""),
        "Training_Group__c": data.get("form", {}).get("training_group", ""),
        "RecordTypeId": data.get("form", {}).get("record_type", ""),
        "Date__c": data.get("form", {}).get("date", ""),
        "Comments_1__c": data.get("form", {}).get("visit_comments", ""),
        "Demo_Plot_Photo__c": url_string + data.get("form", {}).get("Demo_Plot_Photo", ""),
        "Observer_Signature__c": url_string + data.get("form", {}).get("agronomy_advisor_signature", ""),
        "Observation_Location__Latitude__s": lat,
        "Observation_Location__Longitude__s": lon,
        "Altitude__c": alt
    }
    upsert_to_salesforce(
        "Observation__c",
        "Submission_ID__c",
        data.get("id"),
        observation_fields,
        sf_connection
    )

# 5. Process Demo Plot Observation Results  
def process_demoplot_observation_results(data: dict, sf_connection):
    url_string = f'https://www.commcarehq.org/a/{data.get("domain")}/api/form/attachment/{data.get("form", {}).get("meta", {}).get("instanceID")}/'
    gps_coordinates = data.get("form", {}).get("meta", {}).get("location", {}).get("#text", "")
    best_practice_string = data.get("form", {}).get("best_practice_questions", {})
    app_id = data.get("app_id", "")
    
    # We'll be using these somewhere to filter out the results. ET projects have different values from the rest of the countries.
    ethiopian_app_ids = [
        "521097abbcfd4fa79668cb6ca3dca28a", # Regrow 2025
        "dd10fc19040d40f0be48a447e1d2727c", # Regrow 2024
        "d63cdcf6b9d849548413ca356871cd3a", # JCP 2023
        "0c9b5791828b4baea6c1eaa4d6979c5a", # CREW 2025
        "f079b0daae1d4d34a89e331dc5a72fbd", # CREW 2024
        "af9f1901f966494792ade0b63e0d8568" # CREW 2025 TFS
    ]

    best_practice_criteria_single = {
        # 1. Compost
        "coffee_global__compost_heap": {
            "Result__c": best_practice_string.get("compost_heap", {}).get("present_compost_heap", "") if app_id in ethiopian_app_ids else
            {
                "1": "Yes, compost or manure heap seen",
                "0": "No compost or manure heap seen"
            }.get(best_practice_string.get("compost_heap", {}).get("present_compost_heap", ""), "") or "",
            "Compost_Photo__c": url_string + best_practice_string.get("compost_heap", {}).get("compost_heap_photo", "") or ""
        },
        
        # 2. Mulch
        "coffee_global__mulch" :{
            "Result__c": best_practice_string.get("mulch", {}).get("mulch_under_the_canopy", "") if app_id in ethiopian_app_ids else
            {
                "1": "Yes, Some mulch seen",
                "0": "No mulch seen"
            }.get(best_practice_string.get("mulch", {}).get("mulch_under_the_canopy", ""), "") or "",
            "Result_Text__c": best_practice_string.get("mulch", {}).get("thickness_of_mulch", "") if app_id in ethiopian_app_ids else
            {
                "1": "Soil can be seen clearly, less than 2cm of mulch",
                "2": "Soil can not be seen, more than 2cm of mulch"
            }.get(best_practice_string.get("mulch", {}).get("thickness_of_mulch", ""), "") or ""
        },
        
        # 3. Shade Management
        "coffee_global__shade_management":{
            "Result__c": best_practice_string.get("shade_management", {}).get("level_of_shade_present", "") if app_id in ethiopian_app_ids else
            {
                "0": "NO shade, less than 5%",
                "1": "Light shade, 5 to 20%",
                "2": "Medium shade, 20 to 40%",
                "3": "Heavy shade, over 40%"
            }.get(best_practice_string.get("shade_management", {}).get("level_of_shade_present", ""), "") or "",
            "Shade_Management_Photo__c": url_string + best_practice_string.get("shade_management", {}).get("shade_management_photo", "") or ""
        },
        
        # 4. Vetiver
        "coffee_global__vetiver_planted":{
            "Result__c": best_practice_string.get("vetiver", {}).get("vetiver_planted", "") if app_id in ethiopian_app_ids else
            {
                "1": "Yes. Row of vetiver planted",
                "0": "No. Vetiver not planted"
            }.get(best_practice_string.get("vetiver", {}).get("vetiver_planted", ""), "") or ""
        },
        
        # 5. Weed Management
        "coffee_global__weed_management":{
            "Result__c": best_practice_string.get("weed_management", {}).get("has_the_demo_plot_been_dug", "") if app_id in ethiopian_app_ids else
            {
                "1": "Yes, field dug",
                "0": "No sign of digging"
            }.get(best_practice_string.get("weed_management", {}).get("has_the_demo_plot_been_dug", ""), "") or "",
            "Result_Text__c": best_practice_string.get("weed_management", {}).get("how_many_weeds_are_under_the_tree_canopy", "") if app_id in ethiopian_app_ids else
            {
                "0": "No weeds under the tree canopy",
                "1": "Few weeds under the tree canopy",
                "2": "Many weeds under the tree canopy"
            }.get(best_practice_string.get("weed_management", {}).get("how_many_weeds_are_under_the_tree_canopy", ""), "") or "",
            "Result_Text_Two__c": best_practice_string.get("weed_management", {}).get("how_big_are_the_weeds", "") if app_id in ethiopian_app_ids else
            {
                "1": "Weeds are less than 30cm tall or 30cm spread for grasses",
                "2": "Weeds are more than 30cm tall or 30cm spread for grasses"
            }.get(best_practice_string.get("weed_management", {}).get("how_big_are_the_weeds", ""), "") or "",
            "Weed_Management_Photo__c": url_string + best_practice_string.get("weed_management", {}).get("weed_management_photo", "") or ""
        },
        
        # 6. Rejuvenation
        "coffee_global__rejuvenation":{
            "Result__c": {
                "1": "Yes. There is a rejuvenated plot",
                "0": "No rejuvenated plot"
            }.get(str(best_practice_string.get("rejuvenation", {}).get("rejuvenation_plot", "")), "") if isinstance(best_practice_string.get("rejuvenation", {}), dict) else "",
            "Result_Text__c": {
                "1": "Yes. Sucker selection is complete",
                "0": "No. Sucker selection has not been done"
            }.get(str(best_practice_string.get("rejuvenation", {}).get("suckers_three", "")), "") if isinstance(best_practice_string.get("rejuvenation", {}), dict) else "",
            "Rejuvenation_Photo__c": url_string + best_practice_string.get("rejuvenation", {}).get("suckers_photo", "") or ""
        },
        
        # 7. Sucker Selection
        "coffee_global__sucker_selection_taken_place":{
            "Result__c": best_practice_string.get("sucker_selection", {}).get("Sucker_Selection_Taken_Place", "") if isinstance(best_practice_string.get("sucker_selection", {}), dict) else "",
            "Result_number__c": best_practice_string.get("sucker_selection", {}).get("number_of_suckers", "") if isinstance(best_practice_string.get("sucker_selection", {}), dict) else ""
        },
        
        # # 8. Kitchen Garden (DRC)
        # "coffee_global__kitchen_garden":{},
        
        # # 9. Woodlots (DRC)
        # "coffee_global__woodlots":{},
        
        # 10. Stumping
        "coffee_global__stumped_trees":{
            "Result__c": best_practice_string.get("stumped", {}).get("stumped_trees", "") if isinstance(best_practice_string.get("stumped", {}), dict) else ""
        }
    }
    
    # Upsert Best Practices for Single Option Answers
    for best_practice, best_practice_results in best_practice_criteria_single.items():
        
        # subset_dict = {key: best_practice_results[key] for key in list(best_practice_results.keys())[1:]}
        
        observation_results_fields = {
            "Observation__r": {
                "Submission_ID__c": data.get("id", "")
            },
            "RecordTypeId": "01224000000gQe5AAE",
            **best_practice_results,
            "Observation_Criterion__r": {
                "Unique_Name__c": best_practice  
            }
        }
        
        if observation_results_fields.get("Result__c", "") not in ["", None, {}]:
            try:
                upsert_to_salesforce(
                    "Observation_Result__c",
                    "Submission_ID__c",
                    f'{data.get("id", "")}{best_practice}',
                    observation_results_fields,
                    sf_connection
                )
            except Exception as e:
                logger.error({
                    "message": "Failed to process observation criterion",
                    "best_practice": best_practice,
                    "error": str(e)
                })
        else: 
            logger.info({
                "message": "Skipping Observation Criterion",
                "best_practice": best_practice
            })
    
    pruning_results = str(best_practice_string.get("pruning", {}).get("pruning_methods", "")).split(" ")
    covercrop_results = str(best_practice_string.get("covercropping", {}).get("covercrop_present", "")).split(" ")
    
    # Handling Pruning
    for result in pruning_results:
        best_practice = "coffee_global__pruning"
        observation_results_fields = {
            "Result__c": {
                "1": "Centers opened",
                "2": "Unwanted suckers removed",
                "3": "Dead branches removed", 
                "4": "Branches touching the ground removed",
                "5": "Broken/unproductive stems and/or branches removed",
                "0": "No pruning methods used"
            }.get(result),
            "Observation__r": {
                "Submission_ID__c": data.get("id", "")
            },
            "RecordTypeId": "01224000000gQe5AAE",
            "Observation_Criterion__r": {
                "Unique_Name__c": best_practice
            }
        }
        
        if observation_results_fields.get("Result__c", "") not in ["", None, {}]:
            try:
                upsert_to_salesforce(
                    "Observation_Result__c",
                    "Submission_ID__c",
                    f'{data.get("id", "")}{best_practice}{result}',
                    observation_results_fields,
                    sf_connection
                )
            except Exception as e:
                logger.error({
                    "message": "Failed to process observation criterion",
                    "best_practice": best_practice,
                    "error": str(e)
                })
        else: 
            logger.info({
                "message": "Skipping Observation Criterion",
                "best_practice": best_practice
            })
            
    # Handling Covercrop
    for result in covercrop_results:
        best_practice = "coffee_global__covercrop_planted"
        observation_results_fields = {
            "Result__c": {
                "1": "Arachis",
                "2": "Beans",
                "3": "Mulch",
                "0": "No Covercropping Practice"
            }.get(result),
            "Observation__r": {
                "Submission_ID__c": data.get("id", "")
            },
            "RecordTypeId": "01224000000gQe5AAE",
            "Observation_Criterion__r": {
                "Unique_Name__c": best_practice
            }
        }
        
        if observation_results_fields.get("Result__c", "") not in ["", None, {}]:
            try:
                upsert_to_salesforce(
                    "Observation_Result__c",
                    "Submission_ID__c",
                    f'{data.get("id", "")}{best_practice}{result}',
                    observation_results_fields,
                    sf_connection
                )
            except Exception as e:
                logger.error({
                    "message": "Failed to process observation criterion",
                    "best_practice": best_practice,
                    "error": str(e)
                })
        else: 
            logger.info({
                "message": "Skipping Observation Criterion",
                "best_practice": best_practice
            })
