import asyncio
from utils.observation_util import process_demoplot_observation, process_demoplot_observation_results
from utils.logging_config import logger

async def send_to_salesforce(data: dict, sf_connection):
    request_id = data.get("id")

    # Process demoplot observation object
    try:
        logger.info({
            "message": "Processing demoplot observation",
            "request_id": request_id
        })
        process_demoplot_observation(data, sf_connection)
    except Exception as e:
        logger.error({
            "message": "Error processing demoplot observation",
            "request_id": request_id,
            "error": str(e)
        })
        return False, str(e)

    # Process observation results - Participant Feedback
    try:
        logger.info({
            "message": "Processing observation results",
            "request_id": request_id
        })
        process_demoplot_observation_results(data, sf_connection)
    except Exception as e:
        logger.error({
            "message": "Error processing observation results",
            "request_id": request_id,
            "error": str(e)
        })
        return False, str(e)

    # Completion log
    logger.info({
        "message": "Demoplot observation processing completed",
        "request_id": request_id
    })

    return True, None