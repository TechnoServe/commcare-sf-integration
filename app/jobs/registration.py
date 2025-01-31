# from app.utils.salesforce_client import SalesforceClient
import asyncio
from utils.attendance_util import process_attendance, process_training_session
from utils.registration_util import process_training_group, process_household, process_participant, process_participant_deactivation
from utils.logging_config import logger
import os
  
environment = os.getenv("SALESFORCE_ENV")

async def send_to_salesforce(data: dict, sf_connection):
    request_id = data.get("id")

    try:
        # Step 1: Process training group
        try:
            logger.info({
                "message": "Processing training group",
                "request_id": request_id,
            })
            process_training_group(data, sf_connection)
        except Exception as e:
            logger.error({
                "request_id": request_id,
                "error": str(e)
            })
            return False, str(e)

        # Step 2: Process household
        try:
            logger.info({
                "message": "Processing household",
                "request_id": request_id
            })
            process_household(data, sf_connection)
        except Exception as e:
            logger.error({
                "request_id": request_id,
                "error": str(e)
            })
            return False, str(e)

        # Step 3: Process participant
        try:
            logger.info({
                "message": "Processing participant",
                "request_id": request_id
            })
            process_participant(data, sf_connection)
        except Exception as e:
            logger.error({
                "request_id": request_id,
                "error": str(e)
            })
            return False, str(e)

        # Step 4: Process participant deactivation
        try:
            logger.info({
                "message": "Processing participant deactivation",
                "request_id": request_id
            })
            process_participant_deactivation(data, sf_connection)
        except Exception as e:
            logger.error({
                "request_id": request_id,
                "error": str(e)
            })
            return False, str(e)

        # Step 5: Process training session
        try:
            logger.info({
                "message": "Processing training session",
                "request_id": request_id
            })
            process_training_session(data, sf_connection)
        except Exception as e:
            logger.error({
                "request_id": request_id,
                "error": str(e)
            })
            return False, str(e)

        # Step 6: Process attendance
        try:
            logger.info({
                "message": "Processing attendance",
                "request_id": request_id
            })
            if os.getenv("SALESFORCE_ENV", "sandbox") != "sandbox": 
                process_attendance(data, sf_connection)
        except Exception as e:
            logger.error({
                "request_id": request_id,
                "error": str(e)
            })
            return False, str(e)

        return True, None

    except Exception as e:
        logger.error({
            "request_id": request_id,
            "error": str(e)
        })
        return False, str(e)
    
