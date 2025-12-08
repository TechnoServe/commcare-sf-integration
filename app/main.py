from flask import Flask, request, jsonify
import asyncio
from google.cloud import firestore
from google.cloud.firestore import FieldFilter
from google.api_core.retry import Retry
from jobs.commcare_to_salesforce import registration, attendance, training_observation, demoplot_observation, farm_visit
from jobs.commcare_to_postrgresql import wetmill_registration, wetmill_visit
from utils.firestore_client import save_to_firestore, update_firestore_status
import os
from simple_salesforce import Salesforce
from dotenv import load_dotenv
from utils.logging_config import logger  # Import the centralized logger
from datetime import datetime, timezone
import requests
from jobs.salesforce_to_commcare import process_commcare_data
import httpx

from utils.postgres import init_db

def main():
    init_db()
    logger.info({
        "message": "Database initialized!",
    })

load_dotenv()  # Load environment variables

app = Flask(__name__)
db = firestore.Client()

migrated_form_types = [
    "Farmer Registration", "Attendance Full - Current Module", 
    'Edit Farmer Details', 'Training Observation', 
    "Attendance Light - Current Module", 'Participant', 
    "Training Group", "Training Session", "Project Role", 
    "Household Sampling", "Demo Plot Observation", "Farm Visit Full",
    "Farm Visit - AA", "Field Day Farmer Registration", "Field Day Attendance Full",
    "Wet Mill Registration Form", "Wet Mill Visit"
    ]

# Salesforce Authentication
def authenticate_salesforce():
    environment = os.getenv("SALESFORCE_ENV", "sandbox")
    password = os.getenv("SF_PROD_PASSWORD")
    security_token = os.getenv("SF_PROD_SECURITY_TOKEN")

    if environment == "sandbox":
        username = os.getenv("SF_TEST_USERNAME")
        domain = "test"  # Salesforce sandbox domain
    else:
        username = os.getenv("SF_PROD_USERNAME")
        domain = "login"  # Salesforce production domain

    try:
        sf = Salesforce(username=username, password=password, security_token=security_token, domain=domain)
        logger.info({
            "message": "Authenticated with Salesforce",
            "environment": environment
        })
        return sf
    except Exception as e:
        logger.error({
            "message": "Error authenticating with Salesforce",
            "environment": environment,
            "error": str(e)
        })
        return None

def authenticate_commcare():
    domain = os.getenv("CC_DOMAIN")
    apikey = os.getenv("CC_API_KEY")
    username = os.getenv("CC_USERNAME")
    url = f'https://www.commcarehq.org/a/{domain}/receiver/GCP_Forms/'
    headers = {'Authorization': 'ApiKey ' + f'{username}:{apikey}',
               "Content-Type": "text/xml"}
    return url, headers

# Initialize Salesforce connection
sf_connection = authenticate_salesforce()

@app.route('/process-data-<origin_url_parameter>', methods=['POST'])
def process_data(origin_url_parameter):
    
    # The Collection is named after the origin of the data
    mapping = {
        'cc': {'destination': 'Salesforce','origin': 'CommCare' , 'collection': 'commcare_collection', 'destination_url_parameter': 'sf'},
        'sf': {'destination': 'CommCare', 'origin': 'Salesforce', 'collection': 'salesforce_collection','destination_url_parameter': 'cc'}
    }
    
    # Use the mapping to set destination and origin, defaulting to None if the collection is not found
    destination, origin, collection, destination_url_parameter = mapping.get(origin_url_parameter.lower().strip(), {
        'destination': None, 
        'origin': None, 
        'collection': None,
        'destination_url_parameter': None
        }).values()
    
    data = request.get_json()

    # Ensure data is provided
    if not data:
        return jsonify({"error": "Invalid payload"}), 400

    # Parse job type and request ID from the payload
    job_name = data.get("form", {}).get("@name") if origin == 'CommCare' else data.get("jobType") if origin == "Salesforce" else None
    request_id = data.get("id")
    
    # Check for cases where survey_type is Attendance Light but job_name is followup
    if job_name == "Followup" and data.get("form", {}).get("survey_type", "") == "Attendance Light":
        job_name = "Attendance Light - Current Module"

    if not job_name:
        logger.warning({
            "message": "Job name not provided in payload",
            "request_id": request_id
        })
        return jsonify({"error": "Job name not provided in payload"}), 422
    try:
        logger.info({
            "message": "Storing data in Firestore",
            "request_id": request_id,
            "job_name": job_name
        })
        
        if job_name in migrated_form_types:

            # PIMA Sustainability forwarded to PostgreSQL
            # if job_name in ["Wet Mill Registration Form", "Wet Mill Visit"]:
            #     if job_name == "Wet Mill Registration Form":
            #         success, error = wetmill_registration.save_wetmill_registration(data, sf_connection)
            #     elif job_name == "Wet Mill Visit":
            #         success, error = wetmill_visit.save_form_visit(data)
                    
            #     if success:
            #         logger.info({
            #             "message": f"Processed successfully record with Request ID: '{request_id}' to PostgreSQL",
            #             "request_id": request_id,
            #         })
            #     else:
            #         logger.error({
            #             "message": f"Failed to process record with Request ID: '{request_id}' to PostgreSQL",
            #             "request_id": request_id,
            #             "error": error
            #         })
            #         # Save to Firestore for retry
            #         # doc_id = save_to_firestore(data, job_name, "failed", collection)
            #         # logger.info({
            #         #     "message": "Data stored in Firestore for retry",
            #         #     "request_id": request_id,
            #         #     "doc_id": doc_id
            #         # })

            #         return jsonify({"error": f"Failed to save data to Postgres"}), 500 # Nice early exit!
            
            # PIMA Agronomy saved to firestore and later forwarded to Salesforce     
            # else:
            doc_id = save_to_firestore(data, job_name, "new", collection)
            logger.info({
                "message": "Data stored in Firestore",
                "request_id": request_id,
                "doc_id": doc_id
            })
            
        else:
            logger.warning({
                "message": "Skipping saving in firestore.",
                "request_id": request_id,
                "job_name": job_name
            })
            
    except Exception as e:
        print(e)
        logger.error({
            "message": "Failed to save data to Firestore",
            "request_id": request_id,
            "error": str(e)
        })
        return jsonify({"error": f"Failed to save data to Firestore: {str(e)}"}), 500

    # Respond immediately with status 200, processing to be done asynchronously
    return jsonify({"status": "Data stored, processing deferred"}), 200

# Function to check the size of records from Salesforce to be able to process the record immediately

async def process_firestore_records(collection):
    # Query records to send to SF. 10 Records at a time
    
    query_size = {"salesforce_collection": 1, 
                  "commcare_collection": 10
                  }
    
    docs = db.collection(collection).where(
        filter=FieldFilter("status", "==", "new")).where(
        filter=FieldFilter("job_name", "in", migrated_form_types)
        ).limit(query_size.get(collection, 0)).get()
    
    destination = 'CommCare' if collection == 'salesforce_collection' else "Salesforce" if collection == 'commcare_collection' else None

    processed_records = []
    for doc in docs:
        doc_id = doc.id
        data = doc.to_dict()  # Extract data from Firestore
        request_id = data.get("data", {}).get("id")  # Request ID: Used to track the record in logs
        job_name = data.get("job_name")
        
        destination = "PostgreSQL" if job_name in ["Wet Mill Registration Form", "Wet Mill Visit"] else destination

        try:
            logger.info({
                "message": f"Started sending record with Request ID: '{request_id}' to {destination}",
                "request_id": request_id,
                "doc_id": doc_id
            })

            update_firestore_status(doc_id, "processing", collection)  # Set status to "processing" before handling the record

            # 1. Farmer Registration and Update
            if job_name in ["Farmer Registration", "Edit Farmer Details", "Field Day Farmer Registration"]:
                success, error = await registration.send_to_salesforce(data.get("data"), sf_connection)

            # 2. Attendance Light and Full
            elif job_name in ["Attendance Full - Current Module", "Attendance Light - Current Module", "Field Day Attendance Full"]:
                success, error = await attendance.send_to_salesforce(data.get("data"), sf_connection)
            
            # 3. Training Observation    
            elif job_name == "Training Observation":
                success, error = await training_observation.send_to_salesforce(data.get("data"), sf_connection)
            
            # 4. Salesforce -> CommCare    
            elif job_name in ["Participant", "Training Group", "Training Session", "Project Role", "Household Sampling"]:
                success, error = await process_commcare_data.process_records_parallel(data.get("data"), job_name)  # Use the new parallel processing function
                
            # 5. Demo Plot Observation    
            elif job_name == "Demo Plot Observation":
                # success, error = await demoplot_observation.send_to_salesforce(data.get("data"), sf_connection)
                success, error = await demoplot_observation.send_to_salesforce(data.get("data"), sf_connection)
            
            # 6. Farm Visit
            elif job_name in ["Farm Visit Full", "Farm Visit - AA"]:
                success, error = await farm_visit.send_to_salesforce(data.get("data"), sf_connection)
                
            # 7. Wet Mill Registration and Visit to PostgreSQL
            elif job_name in ["Wet Mill Registration Form", "Wet Mill Visit"]:
                if job_name == "Wet Mill Registration Form":
                    success, error = wetmill_registration.save_wetmill_registration(data.get("data"), sf_connection)
                elif job_name == "Wet Mill Visit":
                    success, error = wetmill_visit.save_form_visit(data.get("data"))


            if success:
                # If processing is successful, mark as completed
                update_firestore_status(doc_id, "completed", collection)
                processed_records.append(doc_id)
                logger.info({
                    "message": f"Processed successfully record with Request ID: '{request_id}' to {destination}",
                    "request_id": request_id,
                    "doc_id": doc_id
                })
            else:
                # If failed, mark record as failed with the error
                update_firestore_status(doc_id, "failed", collection, {"error": error})
                logger.error({
                    "message": f"Failed to process record with Request ID: '{request_id}' to {destination}",
                    "request_id": request_id,
                    "doc_id": doc_id,
                    "error": error
                })

        except Exception as e:
            # In case of error, mark as failed and log the error
            update_firestore_status(doc_id, "failed", collection, {"error": str(e)})
            logger.error({
                "message": f"Error processing record with Request ID: '{request_id}' to {destination}",
                "request_id": request_id,
                "doc_id": doc_id,
                "error": str(e)
            })

    return processed_records

@app.route('/process-firestore-to-<destination_url_parameter>', methods=['POST'])
async def process_firestore(destination_url_parameter):
    
    mapping = {
        'sf': {'destination': 'Salesforce','origin': 'CommCare' , 'collection': 'commcare_collection'},
        'cc': {'destination': 'CommCare', 'origin': 'Salesforce', 'collection': 'salesforce_collection'}
    }
    
    # Use the mapping to set destination and origin, defaulting to None if the collection is not found
    destination, origin, collection = mapping.get(destination_url_parameter.lower().strip(), {'destination': None, 'origin': None, 'collection': None}).values()
    
    try:
        timeStart = datetime.now()
        logger.info({
            "message": "Batch processing started from scheduler",
            "timeStart": str(timeStart)
        })
        
        # Assuming process_firestore_records is asynchronous and processes records in batches
        processed_records = await process_firestore_records(collection)

        timeEnd = datetime.now()
        
        logger.info({
            "message": "Batch processing completed",
            "processed_records": processed_records,
            "timeEnd": str(timeEnd)
        })
        
        totalTime = ((timeEnd - timeStart).total_seconds())/60

        return jsonify({"status": "Processing completed", "processed_records": processed_records, "time_taken": f'{str(totalTime)} minutes'}), 200

    except Exception as e:
        logger.error({
            "message": "Error in processing batch",
            "error": str(e)
        })
        return jsonify({"error": f"Error processing Firestore records: {str(e)}"}), 500

async def process_failed_records(collection):
    # Query records to send to SF. 10 Records at a time
    
    query_size = {"salesforce_collection": 1, 
                  "commcare_collection": 10
                  }
    
    docs = db.collection(collection).where(
        filter=FieldFilter("status", "==", "failed")).where( 
        filter=FieldFilter("job_name", "in", migrated_form_types)).where( 
        filter=FieldFilter("run_retries", "<", 3)
        ).limit(query_size.get(collection, 0)).get()
    
    destination = 'CommCare' if collection == 'salesforce_collection' else "Salesforce" if collection == 'commcare_collection' else None

    processed_records = []
    for doc in docs:
        doc_id = doc.id
        data = doc.to_dict()  # Extract data from Firestore
        request_id = data.get("data", {}).get("id")  # Request ID: Used to track the record in logs
        job_name = data.get("job_name")
        
        destination = "PostgreSQL" if job_name in ["Wet Mill Registration Form", "Wet Mill Visit"] else destination

        try:
            logger.info({
                "message": f"Started sending record with Request ID: '{request_id}' to {destination}",
                "request_id": request_id,
                "doc_id": doc_id
            })

            update_firestore_status(doc_id, "processing", collection)  # Set status to "processing" before handling the record

            # 1. Farmer Registration and Update
            if job_name in ["Farmer Registration", "Edit Farmer Details", "Field Day Farmer Registration"]:
                success, error = await registration.send_to_salesforce(data.get("data"), sf_connection)
            
            # 2. Attendance Light).where( Full    
            elif job_name in ["Attendance Full - Current Module", "Attendance Light - Current Module", "Field Day Attendance Full"]:
                success, error = await attendance.send_to_salesforce(data.get("data"), sf_connection)
            
            # 3. Training Observation    
            elif job_name == "Training Observation":
                success, error = await training_observation.send_to_salesforce(data.get("data"), sf_connection)
            
            # 4. Salesforce -> CommCare    
            elif job_name in ["Participant", "Training Group", "Training Session", "Project Role", "Household Sampling"]:
                success, error = await process_commcare_data.process_records_parallel(data.get("data"), job_name)  # Use the new parallel processing function 
            
            # 5. Demo Plot Observation    
            elif job_name == "Demo Plot Observation":
                success, error = await demoplot_observation.send_to_salesforce(data.get("data"), sf_connection)
                
            # 6. Farm Visit Full
            elif job_name in ["Farm Visit Full", "Farm Visit - AA"]:
                success, error = await farm_visit.send_to_salesforce(data.get("data"), sf_connection)
                
            # 7. Wet Mill Registration and Visit to PostgreSQL
            elif job_name in ["Wet Mill Registration Form", "Wet Mill Visit"]:
                if job_name == "Wet Mill Registration Form":
                    success, error = wetmill_registration.save_wetmill_registration(data.get("data"), sf_connection)
                elif job_name == "Wet Mill Visit":
                    success, error = wetmill_visit.save_form_visit(data.get("data"))

            if success:
                # If processing is successful, mark as completed
                update_firestore_status(doc_id, "completed", collection)
                processed_records.append(doc_id)
                logger.info({
                    "message": f"Processed successfully record with Request ID: '{request_id}' to {destination}",
                    "request_id": request_id,
                    "doc_id": doc_id
                })
            else:
                # If failed, mark record as failed with the error
                update_firestore_status(doc_id, "failed", collection, {"error": error, "run_retries": data.get("run_retries", 0) + 1})
                logger.error({
                    "message": f"Failed to process record with Request ID: '{request_id}' to {destination}",
                    "request_id": request_id,
                    "doc_id": doc_id,
                    "error": error
                })

        except Exception as e:
            # In case of error, mark as failed and log the error
            update_firestore_status(doc_id, "failed", collection, {"error": str(e), "run_retries": data.get("run_retries", 0) + 1})
            logger.error({
                "message": f"Error processing record with Request ID: '{request_id}' to {destination}",
                "request_id": request_id,
                "doc_id": doc_id,
                "error": str(e)
            })

    return processed_records

@app.route('/auto-retry-firestore-to-<destination_url_parameter>', methods=['POST'])
async def retry_firestore_records(destination_url_parameter):
    
    mapping = {
        'sf': {'destination': 'Salesforce','origin': 'CommCare' , 'collection': 'commcare_collection'},
        'cc': {'destination': 'CommCare', 'origin': 'Salesforce', 'collection': 'salesforce_collection'}
    }
    
    # Use the mapping to set destination and origin, defaulting to None if the collection is not found
    destination, origin, collection = mapping.get(destination_url_parameter.lower().strip(), {'destination': None, 'origin': None, 'collection': None}).values()
    
    try:
        logger.info({
            "message": "Auto Retrying started from scheduler"
        })
        
        # Assuming process_firestore_records is asynchronous and processes records in batches
        processed_records = await process_failed_records(collection)

        logger.info({
            "message": "Auto Retrying processing completed",
            "processed_records": processed_records
        })

        return jsonify({"status": "Processing completed", "processed_records": processed_records, "processed_records_count": len(processed_records)}), 200

    except Exception as e:
        logger.error({
            "message": "Error in processing Auto Retrying",
            "error": str(e), 
        })
        return jsonify({"error": f"Error retrying Firestore records: {str(e)}"}), 500

@app.route('/record/<collection>/<id>', methods=['GET'])
def get_record(collection, id):

    try:
        docs = db.collection(collection).where(filter=FieldFilter("data.id", "==", id)).get()

        if not docs:
            return jsonify({"message": f"No records found in {collection}", "id": id}), 404

        records = []
        for doc in docs:
            records.append({"id": doc.id, "data": doc.to_dict()})

        return jsonify(records), 200
    except Exception as e:
        logger.error({
            "message": "Error fetching record",
            "id": id,
            "error": str(e)
        })
        return jsonify({"error": "Failed to fetch record", "details": str(e)}), 500
    
@app.route('/retry/<destination_url_parameter>/<id>', methods=['GET']) 
async def retry_record(destination_url_parameter, id):
    
    mapping = {
        'sf': {'destination': 'Salesforce','origin': 'CommCare' , 'collection': 'commcare_collection'},
        'cc': {'destination': 'CommCare', 'origin': 'Salesforce', 'collection': 'salesforce_collection'}
    }
    
    # Use the mapping to set destination and origin, defaulting to None if the collection is not found
    destination, origin, collection = mapping.get(destination_url_parameter.lower().strip(), {'destination': None, 'origin': None, 'collection': None}).values()
    
    try:
        # Query Firestore for documents matching the given ID
        docs = db.collection(collection).where(filter=FieldFilter("data.id", "==", id)).get()

        if not docs:
            logger.info({
                "message": "No records found for retry",
                "id": id
            })
            return jsonify({"message": f"No records found in {collection}", "id": id}), 404

        # Process each document retrieved
    
        for doc in docs:
            doc_id = doc.id
            data = doc.to_dict()
            request_id = data.get("data", {}).get("id")
            job_name = data.get("job_name")

            destination = "PostgreSQL" if job_name in ["Wet Mill Registration Form", "Wet Mill Visit"] else destination
            
            try:
                logger.info({
                    "message": f"Retrying record with Request ID: '{request_id}' to {destination}",
                    "request_id": request_id,
                    "job_name": job_name
                })

                # Check the job type and call the appropriate processing function
                
                # 1. Participant Registration
                if job_name in ["Farmer Registration", "Edit Farmer Details", "Field Day Farmer Registration"]:
                    success, error = await registration.send_to_salesforce(data.get("data"), sf_connection)
                    logger.info(f'Process was successful: {success}')
                
                # 2. Participant Send to CommCare  
                elif job_name in ["Participant", "Training Group", "Training Session", "Project Role", "Household Sampling"]:
                    success, error = await process_commcare_data.process_records_parallel(data.get("data"), job_name)  # Use the new parallel processing function
                    logger.info(f'Process was successful: {success}')
                    
                # 3. Attendance Full).where( Light
                elif job_name in ["Attendance Full - Current Module", "Attendance Light - Current Module", "Field Day Attendance Full"]:
                    success, error = await attendance.send_to_salesforce(data.get("data"), sf_connection)
                    logger.info(f'Process was successful: {success}')
                
                # 4. Training Observation
                elif job_name == "Training Observation":
                    success, error = await training_observation.send_to_salesforce(data.get("data"), sf_connection)
                    
                # 5. Demo Plot Observation    
                elif job_name == "Demo Plot Observation":
                    success, error = await demoplot_observation.send_to_salesforce(data.get("data"), sf_connection)
                    
                # 6. Farm Visit Full
                elif job_name in ["Farm Visit Full", "Farm Visit - AA"]:
                    success, error = await farm_visit.send_to_salesforce(data.get("data"), sf_connection)
                    
                # 7. Wet Mill Registration and Visit to PostgreSQL
                elif job_name in ["Wet Mill Registration Form", "Wet Mill Visit"]:
                    if job_name == "Wet Mill Registration Form":
                        success, error = wetmill_registration.save_wetmill_registration(data.get("data"), sf_connection)
                    elif job_name == "Wet Mill Visit":
                        success, error = wetmill_visit.save_form_visit(data.get("data"))

                if success:
                    # If successful, update Firestore status to completed
                    update_firestore_status(doc_id, "completed", collection)
                    logger.info({
                        "message": f"Processed successfully record with Request ID: '{request_id}' to {destination}",
                        "request_id": request_id,
                        "doc_id": doc_id,
                        "run_retries": data.get("run_retries", 0) + 1,
                        "last_retried_at": firestore.SERVER_TIMESTAMP
                    })
                else:
                    # If failed, update Firestore status to failed with error
                    update_firestore_status(doc_id, "failed", collection, {
                        "error": error,
                        "run_retries": data.get("run_retries", 0) + 1,
                        "last_retried_at": firestore.SERVER_TIMESTAMP
                    })
                    logger.error({
                        "message": f"Failed to process record with Request ID: '{request_id}' to {destination}",
                        "request_id": request_id,
                        "error": error
                    })

            except Exception as e:
                # Handle any exceptions during processing
                update_firestore_status(doc_id, "failed", collection, {
                    "error": str(e),
                    "run_retries": data.get("run_retries", 0) + 1,
                    "last_retried_at": firestore.SERVER_TIMESTAMP
                })
                logger.error({
                    "message": "Error processing record",
                    "request_id": request_id,
                    "job_name": job_name,
                    "error": str(e)
                })

        # Return a success message once all records have been processed
        return jsonify({"message": "Retry completed", "id": id}), 200

    except Exception as e:
        # Catch any errors in the retry operation
        logger.error({
            "message": "Error during retry operation",
            "id": id,
            "error": str(e)
        })
        return jsonify({"error": "Failed to retry records", "details": str(e)}), 500

@app.route('/batch_retry/<destination_url_parameter>/', methods=['POST'])
async def batch_retry(destination_url_parameter):
    data = request.get_json()
    ids_list = data.get('ids', [])

    sem = asyncio.Semaphore(100)  # max 20 retries at once

    async def limited_retry(idx, record_id):
        async with sem:
            try:
                await retry_record(destination_url_parameter, record_id)
                logger.info({
                    "message": f"Retry number {idx+1} completed",
                    "request_id": record_id,
                })
            except Exception as e:
                logger.error({
                    "message": f"Failed to complete retry number {idx+1}",
                    "request_id": record_id,
                    "error": str(e)
                })

    try:
        await asyncio.gather(
            *(limited_retry(idx, record_id) for idx, record_id in enumerate(ids_list))
        )
        return jsonify({"message": "Retry completed"}), 200
    except Exception as e:
        logger.error({
            "message": "Failed to complete batch retry",
            "error": str(e)
        })
        return jsonify({"error": "Failed to retry records", "details": str(e)}), 500

@app.route('/bulk_update/<collection>/', methods=['POST'])
def bulk_update(collection):
    data = request.get_json()
    ids_list = data.get('ids', [])
    status = data.get('status', "new")
    

    try:
        batch_size = 200
        for i in range(0, len(ids_list), batch_size):
            batch = db.batch()
            chunk = ids_list[i:i+batch_size]
            for id in chunk:
                docs = db.collection(collection).where(filter=FieldFilter("data.id", "==", id)).get()
                if not docs:
                    logger.info({"message": "No records found for editing", "id": id})
                    continue
                for doc in docs:
                    logger.info({
                        "message": f"Parsing document {str(doc.id)}"
                    })
                    doc_ref = db.collection(collection).document(doc.id)
                    update_data = {
                        "status": status,
                        "updated_at": str(datetime.now(timezone.utc)),
                        "run_retries": 0
                    }
                    batch.update(doc_ref, update_data)
            batch.commit()
        
        return jsonify({"message": "Update completed"}), 200
    except Exception as e:
        return jsonify({"error": "Failed to update records", "details": str(e)}), 500

@app.route('/failed/<destination_url_parameter>', methods=['GET'])
def get_failed_jobs(destination_url_parameter):
    """
    Endpoint to retrieve jobs with 'failed' status from the Firestore database.
    """
    
    mapping = {
        'sf': {'destination': 'Salesforce','origin': 'CommCare' , 'collection': 'commcare_collection'},
        'cc': {'destination': 'CommCare', 'origin': 'Salesforce', 'collection': 'salesforce_collection'}
    }
    
    # Use the mapping to set destination and origin, defaulting to None if the collection is not found
    destination, origin, collection = mapping.get(destination_url_parameter.lower().strip(), {'destination': None, 'origin': None, 'collection': None}).values()
    
    try:
        # Query Firestore for documents with status 'failed', limited to 5
        processing_docs = db.collection(collection).where(filter=FieldFilter("status", "==", "failed")).get()

        # Extract relevant fields from Firestore documents
        failed_jobs = [
            {
                "id": doc.id, # Firestore document ID
                "collection": collection,
                **doc.to_dict()  # Document fields
            }
            for doc in processing_docs
        ]

        # Return the list of failed jobs
        return jsonify({"failed_jobs": failed_jobs}), 200

    except Exception as e:
        # Log the error with additional context
        logger.error({
            "message": "Error retrieving failed jobs",
            "endpoint": "/failed",
            "error": str(e)
        })
        # Return an error response
        return jsonify({"error": f"Error retrieving failed jobs: {str(e)}"}), 500

@app.route('/status-count/<collection>', methods=['GET'])
def status_count(collection):
    
    try:
        status_count_dict = {}
        statuses = ["new", "processing", "failed", "completed"]

        for status in statuses:
            query = (
                db.collection(collection)
                .where(filter=FieldFilter("status", "==", status))
            )

            result = query.count().get(retry=Retry(deadline=120))
            status_count_dict[status] = result[0][0].value

        return jsonify({"status_counts": status_count_dict}), 200

    except Exception as e:
        logger.error({
            "message": "Error retrieving status count",
            "error": str(e)
        })
        return jsonify({"error": f"Error retrieving status count: {str(e)}"}), 500
    
if __name__ == "__main__":
    main()
    print("app running on port 8080")
    app.run(host="0.0.0.0", port=8080, debug=True)
