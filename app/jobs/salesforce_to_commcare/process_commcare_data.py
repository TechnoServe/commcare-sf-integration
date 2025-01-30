import asyncio
import aiohttp
from datetime import datetime
import requests
from utils.logging_config import logger
from main import authenticate_commcare
from utils.generate_xml import generate_xml

async def process_record(job_name, record, project_unique_id, record_number, processed_counter, session, url, headers, semaphore):
    async with semaphore:
        try:
            xml_string = generate_xml(job_name, record, project_unique_id)
            
            success, error = await send_to_commcare(xml_string, session, url, headers)
            if success:
                processed_counter[0] += 1
            log_message = f"Processed {job_name} record #{record_number}"
            log_message += f" with errors: {error}" if error else " successfully"
            logger.info(log_message)
            return success, error
        except Exception as e:
            logger.error(f"Failed to process {job_name} #{record_number}: {str(e)}")
            return False, str(e)

async def process_records_parallel(data, job_name):
    project_unique_id = data.get("uniqueProjectKey", None)
    
    records_name = {
        "Participant": "participants",
        "Project Role": "projectRoles",
        "Training Session": "trainingSessions",
        "Training Group": "trainingGroups",
        "Household Sampling": "households"
    }.get(job_name, None)
    
    records = data.get(records_name, [])
    processed_counter = [0]
    start_time = datetime.now()
    url, headers = authenticate_commcare()
    semaphore = asyncio.Semaphore(50)  # Concurrency limit

    async with aiohttp.ClientSession() as session:
        tasks = []
        for idx, record in enumerate(records, start=1):
            task = process_record(
                job_name, record, project_unique_id, idx, processed_counter, session, url, headers, semaphore
            )
            tasks.append(task)
        
        results = await asyncio.gather(*tasks)
    
    end_time = datetime.now()
    total_duration = (end_time - start_time).total_seconds()
    logger.info(f"Total {job_name} records processed successfully: {processed_counter[0]}/{len(records)}")
    logger.info(f"Total time taken: {total_duration:.2f} seconds")
    
    if all(result[0] for result in results):
        return True, None
    else:
        errors = [result[1] for result in results if result[1] is not None]
        return False, "; ".join(errors)

async def send_to_commcare(data, session, url, headers):
    try:
        async with session.post(url=url, data=data, headers=headers) as response:
            response_text = await response.text()
            if response.status == 201:
                logger.info(f"Form submitted successfully! HTTP Status: {response.status}")
                logger.info(f"Response: {response_text}")
                return True, None
            else:
                logger.error(f"Failed to submit form. HTTP Status: {response.status}")
                logger.error(f"Response: {response_text}")
                return False, f'HTTP Status: {response.status}'
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        return False, f"An error occurred: {str(e)}"
        
        

