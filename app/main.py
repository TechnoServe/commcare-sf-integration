from flask import Flask, request, jsonify
import asyncio
from google.cloud import firestore
from jobs import registration, attendance  # Import job modules
from utils.firestore_client import save_to_firestore, update_firestore_status
import os
from simple_salesforce import Salesforce
from dotenv import load_dotenv
from utils.logging_config import logger  # Import the centralized logger
from datetime import datetime
import requests
from jobs.salesforce_to_commcare import process_commcare_data

load_dotenv()  # Load environment variables

app = Flask(__name__)
db = firestore.Client()

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
        'cc': {'destination': 'Salesforce','origin': 'CommCare' , 'collection': 'commcare_collection'},
        'sf': {'destination': 'CommCare', 'origin': 'Salesforce', 'collection': 'salesforce_collection'}
    }
    
    # Use the mapping to set destination and origin, defaulting to None if the collection is not found
    destination, origin, collection = mapping.get(origin_url_parameter.lower().strip(), {'destination': None, 'origin': None, 'collection': None}).values()
    
    data = request.get_json()

    # Ensure data is provided
    if not data:
        return jsonify({"error": "Invalid payload"}), 400

    # Parse job type and request ID from the payload
    job_name = data.get("form", {}).get("@name") if origin == 'CommCare' else data.get("jobType") if origin == "Salesforce" else None
    request_id = data.get("id")

    if not job_name:
        logger.warning({
            "message": "Job name not provided in payload",
            "request_id": request_id
        })
        return jsonify({"error": "Job name not provided in payload"}), 200
    try:
        logger.info({
            "message": "Storing data in Firestore",
            "request_id": request_id,
            "job_name": job_name
        })
        
        if job_name in ["Farmer Registration", "Attendance Full - Current Module", 'Participant', 'Edit Farmer Details', 
                        "Training Group", "Training Session", "Project Role", "Household Sampling"]:
            doc_id = save_to_firestore(data, job_name, "new", collection)
            logger.info({
                "message": "Data stored in Firestore",
                "request_id": request_id,
                "doc_id": doc_id
            })
        else:
            logger.info({
                "message": "Skipping saving in firestore.",
                "request_id": request_id
            })
            
    except Exception as e:
        logger.error({
            "message": "Failed to save data to Firestore",
            "request_id": request_id,
            "error": str(e)
        })
        return jsonify({"error": f"Failed to save data to Firestore: {str(e)}"}), 500

    # Respond immediately with status 200, processing to be done asynchronously
    return jsonify({"status": "Data stored, processing deferred"}), 200

async def process_firestore_records(collection):
    # Query records to send to SF. 10 Records at a time
    
    query_size = {"salesforce_collection": 1, 
                  "commcare_collection": 10
                  }
    
    docs = db.collection(collection).where(
        "status", "==", "new"
    ).where(
        "job_name", "in", ["Farmer Registration", "Attendance Full - Current Module", 'Participant', 'Edit Farmer Details', 
                           "Training Group", "Training Session", "Project Role", "Household Sampling"]
    ).limit(query_size.get(collection, 0)).get()
    
    destination = 'CommCare' if collection == 'salesforce_collection' else "Salesforce" if collection == 'commcare_collection' else None

    processed_records = []
    for doc in docs:
        doc_id = doc.id
        data = doc.to_dict()  # Extract data from Firestore
        request_id = data.get("data", {}).get("id")  # Request ID: Used to track the record in logs
        job_name = data.get("job_name")

        try:
            logger.info({
                "message": f"Started sending record with Request ID: '{request_id}' to {destination}",
                "request_id": request_id,
                "doc_id": doc_id
            })

            update_firestore_status(doc_id, "processing", collection)  # Set status to "processing" before handling the record

            # Check if the job is Farmer Registration
            if job_name in ["Farmer Registration", "Edit Farmer Details"]:
                success, error = await registration.send_to_salesforce(data.get("data"), sf_connection)

            elif job_name == "Attendance Full - Current Module":
                success, error = await attendance.send_to_salesforce(data.get("data"), sf_connection)
                
            elif job_name in ["Participant", "Training Group", "Training Session", "Project Role", "Household Sampling"]:
                success, error = await process_commcare_data.process_records_parallel(data.get("data"), job_name)  # Use the new parallel processing function

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
        "status", "==", "failed"
    ).where(
        "job_name", "in", ["Farmer Registration", "Attendance Full - Current Module", "Participant", 'Edit Farmer Details', 
                           "Training Group", "Training Session", "Project Role", "Household Sampling"]
    ).where(
        "run_retries", "<", 3
    ).limit(query_size.get(collection, 0)).get()
    
    destination = 'CommCare' if collection == 'salesforce_collection' else "Salesforce" if collection == 'commcare_collection' else None

    processed_records = []
    for doc in docs:
        doc_id = doc.id
        data = doc.to_dict()  # Extract data from Firestore
        request_id = data.get("data", {}).get("id")  # Request ID: Used to track the record in logs
        job_name = data.get("job_name")

        try:
            logger.info({
                "message": f"Started sending record with Request ID: '{request_id}' to {destination}",
                "request_id": request_id,
                "doc_id": doc_id
            })

            update_firestore_status(doc_id, "processing", collection)  # Set status to "processing" before handling the record

            # Check if the job is Farmer Registration
            if job_name in ["Farmer Registration", "Edit Farmer Details"]:
                success, error = await registration.send_to_salesforce(data.get("data"), sf_connection)
            elif job_name == "Attendance Full - Current Module":
                success, error = await attendance.send_to_salesforce(data.get("data"), sf_connection)
            elif job_name in ["Participant", "Training Group", "Training Session", "Project Role", "Household Sampling"]:
                success, error = await process_commcare_data.process_records_parallel(data.get("data"), job_name)  # Use the new parallel processing function 

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
        docs = db.collection(collection).where("data.id", "==", id).get()

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
        docs = db.collection(collection).where("data.id", "==", id).get()

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

            try:
                logger.info({
                    "message": f"Retrying record with Request ID: '{request_id}' to {destination}",
                    "request_id": request_id,
                    "job_name": job_name
                })

                # Check the job type and call the appropriate processing function
                
                # 1. Participant Registration
                if job_name in ["Farmer Registration", "Edit Farmer Details"]:
                    
                    success, error = await registration.send_to_salesforce(data.get("data"), sf_connection)
                    logger.info(f'Process was successful: {success}')
                
                # 2. Participant Send to CommCare  
                elif job_name in ["Participant", "Training Group", "Training Session", "Project Role", "Household Sampling"]:
                    success, error = await process_commcare_data.process_records_parallel(data.get("data"), job_name)  # Use the new parallel processing function
                    logger.info(f'Process was successful: {success}')
                    
                # 3. Attendance Full
                elif job_name == "Attendance Full - Current Module":
                    success, error = await attendance.send_to_salesforce(data.get("data"), sf_connection)
                    logger.info(f'Process was successful: {success}')

                if success:
                    # If successful, update Firestore status to completed
                    update_firestore_status(doc_id, "completed", collection)
                    logger.info({
                        "message": f"Processed successfully record with Request ID: '{request_id}' to {destination}",
                        "request_id": request_id,
                        "doc_id": doc_id,
                        "run_retries": data.get("run_retries", 0) + 1
                    })
                else:
                    # If failed, update Firestore status to failed with error
                    update_firestore_status(doc_id, "failed", collection, {
                        "error": error,
                        "run_retries": data.get("run_retries", 0) + 1
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
                    "run_retries": data.get("run_retries", 0) + 1
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
        processing_docs = db.collection(collection).where("status", "==", "failed").limit(5).get()

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

        new_docs = db.collection(collection).where("status", "==", "new").where("job_name", "in", [
            "Farmer Registration", "Attendance Full - Current Module", 'Participant', 'Edit Farmer Details', 
            "Training Group", "Training Session", "Project Role", "Household Sampling"]).count()

        completed_docs = db.collection(collection).where("status", "==", "completed").where("job_name", "in", [
            "Farmer Registration", "Attendance Full - Current Module", 'Participant', 'Edit Farmer Details',
            "Training Group", "Training Session", "Project Role", "Household Sampling"]).count()

        status_count_dict["completed"] = completed_docs.get()[0][0].value
        status_count_dict["new"] = new_docs.get()[0][0].value

        # Query for "processing" status
        processing_docs = db.collection(collection).where("status", "==", "processing").where("job_name", "in", [
            "Farmer Registration", "Attendance Full - Current Module", 'Participant', 'Edit Farmer Details', 
            "Training Group", "Training Session", "Project Role", "Household Sampling"]).count()
        status_count_dict["processing"] = processing_docs.get()[0][0].value

        # Query for "failed" status
        failed_docs = db.collection(collection).where("status", "==", "failed").where("job_name", "in", [
            "Farmer Registration", "Attendance Full - Current Module", 'Participant', 'Edit Farmer Details', 
            "Training Group", "Training Session", "Project Role", "Household Sampling"]).count()
        status_count_dict["failed"] = failed_docs.get()[0][0].value

        return jsonify({"status_counts": status_count_dict}), 200

    except Exception as e:
        logger.error({
            "message": "Error retrieving status count",
            "error": str(e)
        })
        return jsonify({"error": f"Error retrieving status count: {str(e)}"}), 500
    
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)
