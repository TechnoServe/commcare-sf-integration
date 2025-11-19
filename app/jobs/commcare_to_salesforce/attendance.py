import asyncio
from utils.attendance_util import process_attendance, process_training_session
from utils.logging_config import logger

async def send_to_salesforce(data: dict, sf_connection):
    request_id = data.get("id")

    # Process training session
    try:
        logger.info({
            "message": "Processing training session",
            "request_id": request_id
        })
        process_training_session(data, sf_connection)
    except Exception as e:
        logger.error({
            "message": "Error processing training session",
            "request_id": request_id,
            "error": str(e)
        })
        return False, str(e)

    # Process attendance
    try:
        logger.info({
            "message": "Processing attendance",
            "request_id": request_id
        })
        process_attendance(data, sf_connection)
    except Exception as e:
        logger.error({
            "message": "Error processing attendance",
            "request_id": request_id,
            "error": str(e)
        })
        return False, str(e)

    # Completion log
    logger.info({
        "message": "Attendance processing completed",
        "request_id": request_id
    })

    return True, None