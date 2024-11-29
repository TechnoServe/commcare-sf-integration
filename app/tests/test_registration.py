import pytest
from unittest.mock import patch, MagicMock
from jobs.registration import (
    process_training_group,
    process_household,
    process_participant,
    process_participant_deactivation,
    send_to_salesforce,
)

# Tests for process_training_group
@patch("jobs.registration.upsert_to_salesforce")
def test_process_training_group_new_farmer(mock_upsert):
    """
    Test process_training_group when survey_detail is "New Farmer New Household".
    Verify upsert_to_salesforce is called with correct parameters.
    """
    mock_sf_connection = MagicMock()
    data = {
        "form": {
            "survey_detail": "New Farmer New Household",
            "Training_Group_Id": "TG123",
            "Update_Household_Counter": 10
        }
    }

    process_training_group(data, mock_sf_connection)

    mock_upsert.assert_called_once_with(
        "Training_Group__c",
        "CommCare_Case_Id__c",
        "TG123",
        {"Household_Counter__c": 10},
        mock_sf_connection
    )


@pytest.mark.parametrize("survey_detail, expected_call", [
    ("New Farmer New Household", True),
    ("Existing Farmer Change in FFG", True),
    ("Invalid Survey", False)
])
@patch("jobs.registration.upsert_to_salesforce")
def test_process_training_group_parameterized(mock_upsert, survey_detail, expected_call):
    """
    Parameterized test for process_training_group with multiple survey_detail values.
    """
    mock_sf_connection = MagicMock()
    data = {
        "form": {
            "survey_detail": survey_detail,
            "Training_Group_Id": "TG123",
            "Update_Household_Counter": 10
        }
    }

    process_training_group(data, mock_sf_connection)

    if expected_call:
        mock_upsert.assert_called_once()
    else:
        mock_upsert.assert_not_called()

# Tests for process_household
@patch("jobs.registration.upsert_to_salesforce")
def test_process_household_new_farmer(mock_upsert):
    mock_sf_connection = MagicMock()
    data = {
        "form": {
            "survey_detail": "New Farmer New Household",
            "Household_Id": "H123",
            "Household_Number": "HN001",
            "Training_Group_Id": "TG123",
            "Number_of_Trees": 50,
            "Number_of_Plots": 2,
            "Primary_Household_Member": "Yes"
        }
    }

    process_household(data, mock_sf_connection)

    mock_upsert.assert_called_once_with(
        "Household__c",
        "Household_ID__c",
        "H123",
        {
            "Name": "HN001",
            "Training_Group__c": "TG123",
            "Farm_Size__c": 50,
            "Number_of_Coffee_Plots__c": 2
        },
        mock_sf_connection
    )

# Tests for process_participant
@patch("jobs.registration.upsert_to_salesforce")
def test_process_participant_new_farmer(mock_upsert):
    mock_sf_connection = MagicMock()
    data = {
        "form": {
            "survey_detail": "New Farmer New Household",
            "Farmer_Id": "KCBSTIMBAR1907022",
            "Household_Id": "KCBSTIMBAR190702",
            "Training_Group_Id": "TG123",
            "First_Name": "Eunice",
            "Last_Name": "Karea",
            "Middle_Name": "Yves",
            "Phone_Number": "0114340628",
            "registration_date": "2024-11-01",
        }
    }

    process_participant(data, mock_sf_connection)

    mock_upsert.assert_called_once_with(
        "Participant__c",
        "CommCare_Case_Id__c",
        None,  
        {
            "TNS_Id__c": "KCBSTIMBAR1907022",
            "Household__r": {"Household_ID__c": "KCBSTIMBAR190702"},
            "Training_Group__c": "TG123",
            "Name": "Eunice",
            "Middle_Name__c": "Yves",
            "Last_Name__c": "Karea",
            "Phone_Number__c": "0114340628",
            "Sent_to_OpenFn_Status__c": "Complete",
            "Create_In_CommCare__c": False,
            "Registration_Date__c": "2024-11-01",
            "Other_Id_Number__c": None,  
            "Farm_Size__c": None, 
            "Age__c": None, 
            "Gender__c": None,  
            "Status__c": None,  
            "Primary_Household_Member__c": None,  
            "Number_of_Coffee_Plots__c": None, 
        },
        mock_sf_connection
    )

"""
Implement test to deactivate a farmer

"""


