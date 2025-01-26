import datetime
import requests
from utils.logging_config import logger  # Import the centralized logger
from main import authenticate_commcare
from utils.generate_xml import generate_xml

async def process_participant(data):
    project_unique_id = data.get("uniqueProjectKey", None)
    case_type = f"{project_unique_id}_participant"
    participants = data.get("participants", [])

    print(f'Participants to iterate over: {len(participants)}')
    
    n_participants = 0
    
    # Process each participant
    try:
        for participant in participants:
            
            n_participants += 1
            
            xml_string = generate_xml('participant', participant, project_unique_id)
            
            try:
                send_to_commcare(xml_string)
                logger.info(f"Processed participant #{n_participants}")
            except Exception as e:
                logger.error(f"Failed to process participant #{n_participants}: {str(e)}")
                continue
        return True, None
    except Exception as e:
        return False, str(e)
        
def send_to_commcare(data):
    try:
        url, headers = authenticate_commcare()
        response = requests.post(url=url, data=data, headers=headers)
        if response.status_code == 201:
            logger.info(f"Form submitted successfully! HTTP Status: {response.status_code}")
            logger.info(f"Response: {response.text}")
        else:
            logger.error(f"Failed to submit form. HTTP Status: {response.status_code}")
            logger.error(f"Response: {response.text}")
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")