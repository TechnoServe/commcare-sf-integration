import asyncio
import aiohttp
from datetime import datetime
import requests
from utils.logging_config import logger
from main import authenticate_commcare
from utils.generate_xml import generate_xml
import xml.etree.ElementTree as ET

async def process_record(job_name, job_id, record, project_unique_id, record_number, processed_counter, session, url, headers, semaphore):
    async with semaphore:
        try:
            xml_string = generate_xml(job_name, job_id, record, project_unique_id)
            
            success, error = await send_to_commcare(xml_string, session, url, headers)
            if success:
                processed_counter[0] += 1
            log_message = f"Processed {job_name} record #{record_number} with case ID: {record.get("commCareCaseId")}"
            log_message += f" with errors: {error}" if error else " successfully"
            logger.info(log_message)
            return success, error
        except Exception as e:
            logger.error(f"Failed to process {job_name} #{record_number}: {str(e)}")
            return False, str(e)

async def process_records_parallel(data, job_name):
    project_unique_id = data.get("uniqueProjectKey", "").lower()
    
    records_name = {
        "Participant": "participants",
        "Project Role": "projectRoles",
        "Training Session": "trainingSessions",
        "Training Group": "trainingGroups",
        "Household Sampling": "households"
    }.get(job_name, None)
    
    records = data.get(records_name, [])
    job_id = data.get("id", "")
    processed_counter = [0]
    start_time = datetime.now()
    url, headers = authenticate_commcare()
    semaphore = asyncio.Semaphore(50)  # Concurrency limit

    async with aiohttp.ClientSession() as session:
        tasks = []
        for idx, record in enumerate(records, start=1):
            task = process_record(
                job_name, job_id, record, project_unique_id, idx, processed_counter, session, url, headers, semaphore
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
            response_nature, response_message = extract_xml_response(response_text)
            
            if response.status == 201 and response_nature == "submit_success":
                logger.info(f"Form submitted successfully! HTTP Status: {response.status}")
                logger.info(f"Response nature: '{response_nature}', message: '{response_message}'")
                return True, None
            else:
                logger.error(f"Failed to submit form. HTTP Status: {response.status}")
                logger.error(f"Response nature: '{response_nature}', message: '{response_message}'")
                return False, f"HTTP Status: {response.status}, message: '{response_message}'"
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        return False, f"An error occurred: {str(e)}"
    
def extract_xml_response(xml_response):
    try:
        # Parse the XML response
        root = ET.fromstring(xml_response)

        # Extract namespace from the root tag
        if '}' in root.tag:
            namespace_uri = root.tag.split('}')[0].strip('{')
            ns = {'ns': namespace_uri}
        else:
            ns = {}  # No namespace fallback

        # Find the 'message' element with namespace (if present)
        message_element = root.find('.//ns:message', ns) if ns else root.find('.//message')

        if message_element is None:
            return None, "No <message> element found"

        # Extract 'nature' attribute and clean message text
        message_nature = message_element.attrib.get('nature', 'unknown')
        message = (message_element.text or "").strip()  # Strip leading/trailing whitespace

        return message_nature, message

    except ET.ParseError:
        return None, "Invalid XML response"

        
        

