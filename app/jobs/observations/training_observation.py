import asyncio
from utils.salesforce_client import upsert_to_salesforce
from utils.attendance_util import process_attendance, process_training_session
from utils.logging_config import logger
import os

# Process functions for each Salesforce object

# 1. Process Training Observation Object
def process_observation(data, sf_connection):
    url_string = 'https://www.commcarehq.org/a/' + data.get("domain") + '/api/form/attachment/' + data.get("form", {}).get("meta", {}).get("instanceID") + '/'
    gps_coordinates = data.get("form", {}).get("meta", {}).get("Location", {}).get("#Text")
    observation_fields = {
        "Observer__c": data.get("form", {}).get("Observer"),
        "Trainer__c": data.get("form", {}).get("trainer_salesforce_id"),
        "Training_Group__c": data.get("form", {}).get("Which_Group_Is_Farmer_Trainer_Teaching"),
        "Training_Session__c": data.get("form", {}).get("selected_session"),
        "RecordTypeId": data.get("form", {}).get("Record_Type"),
        "Date__c": data.get("form", {}).get("Date"),
        "Male_Participants__c": int(data.get("form", {}).get("Current_session_participants", {}).get("Male_Participants_In_Attendance")),
        "Female_Participants__c": int(data.get("form", {}).get("Current_session_participants", {}).get("Female_Participants_In_Attendance")),
        "Number_of_Participants__c": int(data.get("form", {}).get("Current_session_participants", {}).get("Total_Participants_In_Attendance")),
        "Shared_Action_Plan__c": data.get("form", {}).get("Feedback_And_Coaching_With_The_Farmer_Trainer", {}).get("Share_Action_Plan") or None,
        "Shared_Action_Plan_Comments__c": data.get("form", {}).get("Feedback_And_Coaching_With_The_Farmer_Trainer", {}).get("Share_Action_Plan_Comments") or None,
        "Did_Well__c": data.get("form", {}).get("Current_Session_Review", {}).get("Did_Well"),
        "To_Improve__c": data.get("form", {}).get("Current_Session_Review", {}).get("To_Improve"),
        "Photo_of_Facilitator_URL__c": url_string + data.get("form", {}).get("Photo"),
        "Farmer_Trainer_Signature__c": url_string + data.get("form", {}).get("Farmer_Trainer_Signature_Section", {}).get("Farmer_Trainer_Signature"),
        "Observer_Signature__c": url_string + data.get("form", {}).get("Observer_Signature_Section", {}).get("Observer_Signature"),
        "Observation_Location__Latitude__s": gps_coordinates.split(" ")[0],
        "Observation_Location__Longitude__s": gps_coordinates.split(" ")[1],
        "Altitude__c": gps_coordinates.split(" ")[2]
    }
    upsert_to_salesforce(
        "Observation__c",
        "Submission_ID__c",
        data.get("id"),
        observation_fields,
        sf_connection
    )

# 2. Process Observation Results Object - Participant Feedback
# Will be using one single country's (Ethiopia) criteria since we can't filter by country in the TO CommCare form
def process_observation_results_participant(data, sf_connection):
    participant_feedback_criteria = {
        'coffeeet_prepare_and_implement_agronomy_practice': 'Prepare_And_Implement_Agronomy_Practice', 
        'coffeeet_teaching_clarity_and_effectiveness': 'Teaching_Clarity_And_Effectiveness', 
        'coffeeet_knowledge_of_trainer_on_agronomy': 'Knowledge_Of_Trainer_On_Agronomy'
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
                    "Submission_ID__c": data.get("id")
                },
                "RecordTypeId": "01224000000gQe6AAE",
                "Observation_Criterion__r": {
                    "Unique_Name__c": criteria
                },
                "Participant_Sex__c": data.get("form", {}).get(participant_string, {}).get("Participant_Gender"),
                "Result__c": data.get("form", {}).get(participant_string, {}).get(criteria_string),
                "Comments__c": data.get("form", {}).get(participant_string, {}).get("participant_comments"),
            }
            upsert_to_salesforce(
                "Observation_Result__c",
                "Submission_ID__c",
                f'{data.get("id")}{criteria}-p{participant}',
                observation_results_fields,
                sf_connection
            )

# 3. Process Observation Results Object - Observer Feedback
def process_observation_results_observer(data, sf_connection):
    observer_feedback_criteria = {
        'coffeeet_shows_professionalism': 'Shows_Professionalism',
        'coffeeet_is_prepared_and_organized': 'Is_Prepared_and_Organized',
        'coffeeet_engages_participants': 'Engages_Participants',
        'coffeeet_facilitates_openings_and_closings': 'Facilitates_Openings_and_Closings',
        'coffeeet_leads_activities': 'Leads_Activities',
        'coffeeet_leads_discussions': 'Leads_Discussions',
        'coffeeet_follows_lesson_plans': 'Follows_Lesson_Plans',
        'coffeeet_manages_time': 'Manages_Time'
        }
    
    for criteria, criteria_string in observer_feedback_criteria.items():
        observation_results_fields = {
            'Observation__r': {
                'Submission_ID__c': data.get('id')
            },
            'RecordTypeId': '01224000000gQe7AAE',
            'Observation_Criterion__r': {
                'Unique_Name__c': criteria
            },
            'Result__c': data.get('form', {}).get('Ratings_and_Comments').get(criteria_string),
            'Comments__c': data.get('form', {}).get('Ratings_and_Comments').get(f'{criteria_string}_Comments')
        }
        upsert_to_salesforce(
            "Observation_Result__c",
            "Submission_ID__c",
            f'{data.get("id")}{criteria}',
            observation_results_fields,
            sf_connection
        )