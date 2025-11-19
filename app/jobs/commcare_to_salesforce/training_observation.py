import asyncio
from utils.observation_util import process_training_observation, process_training_observation_results_observer, process_training_observation_results_participant
from utils.participant_check_util import process_participant_check_training_observation
from utils.logging_config import logger

async def send_to_salesforce(data: dict, sf_connection):
    request_id = data.get("id")

    # Process training observation object
    try:
        logger.info({
            "message": "Processing training observation",
            "request_id": request_id
        })
        process_training_observation(data, sf_connection)
    except Exception as e:
        logger.error({
            "message": "Error processing training observation",
            "request_id": request_id,
            "error": str(e)
        })
        return False, str(e)

    # Process observation results - Participant Feedback
    try:
        logger.info({
            "message": "Processing observation results: Participant feedback",
            "request_id": request_id
        })
        process_training_observation_results_participant(data, sf_connection)
    except Exception as e:
        logger.error({
            "message": "Error processing observation results: Participant feedback",
            "request_id": request_id,
            "error": str(e)
        })
        return False, str(e)
    
    # Process observation results - Observer Feedback
    try:
        logger.info({
            "message": "Processing observation results: Observer feedback",
            "request_id": request_id
        })
        process_training_observation_results_observer(data, sf_connection)
    except Exception as e:
        logger.error({
            "message": "Error processing observation results: Observer feedback",
            "request_id": request_id,
            "error": str(e)
        })
        return False, str(e)
    
    # Process participant attendance check results
    try:
        logger.info({
            "message": "Processing participant attendance check results",
            "request_id": request_id
        })
        process_participant_check_training_observation(data, sf_connection)
    except Exception as e:
        logger.error({
            "message": "Error processing participant attendance check results",
            "request_id": request_id,
            "error": str(e)
        })
        return False, str(e)

    # Completion log
    logger.info({
        "message": "Training observation processing completed",
        "request_id": request_id
    })

    return True, None