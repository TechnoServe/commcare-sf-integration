from flask import Flask, request, jsonify
import asyncio
from google.cloud import firestore
from jobs import registration, attendance  # Import job modules
from utils.firestore_client import save_to_firestore, update_firestore_status
import os
from simple_salesforce import Salesforce
from dotenv import load_dotenv
from utils.logging_config import logger  # Import the centralized logger
import datetime
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

@app.route('/process-data', methods=['POST'])
def process_data():
    data = request.get_json()

    # Ensure data is provided
    if not data:
        return jsonify({"error": "Invalid payload"}), 400

    # Parse job type and request ID from the payload
    job_name = data.get("form", {}).get("@name")
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
        doc_id = save_to_firestore(data, job_name, "new", 'CommCare')
        logger.info({
            "message": "Data stored in Firestore",
            "request_id": request_id,
            "doc_id": doc_id
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

@app.route('/process-data-salesforce', methods=['POST'])
def process_data_salesforce():
    data = request.get_json()

    # Ensure data is provided
    if not data:
        return jsonify({"error": "Invalid payload"}), 400

    # Parse job type and request ID from the payload
    job_name = data.get("jobType")
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
        doc_id = save_to_firestore(data, job_name, "new", 'Salesforce')
        logger.info({
            "message": "Data stored in Firestore",
            "request_id": request_id,
            "doc_id": doc_id
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

async def process_firestore_records():
    # Query records to send to SF. 10 Records at a time
    docs = db.collection("job_statuses").where(
        "status", "==", "new"
    ).where(
        "job_name", "in", ["Farmer Registration", "Attendance Full - Current Module"]
    ).limit(10).get()


    processed_records = []
    for doc in docs:
        doc_id = doc.id
        data = doc.to_dict()  # Extract data from Firestore
        request_id = data.get("data", {}).get("id")  # Request ID: Used to track the record in logs
        job_name = data.get("job_name")

        try:
            logger.info({
                "message": f"Started sending record with Request ID {request_id} to Salesforce",
                "request_id": request_id,
                "doc_id": doc_id
            })

            update_firestore_status(doc_id, "processing", "CommCare")  # Set status to "processing" before handling the record

            # Check if the job is Farmer Registration
            if job_name == "Farmer Registration":
                success, error = await registration.send_to_salesforce(data.get("data"), sf_connection)

            elif job_name == "Attendance Full - Current Module":
                success, error = await attendance.send_to_salesforce(data.get("data"), sf_connection)

            if success:
                # If processing is successful, mark as completed
                update_firestore_status(doc_id, "completed", "CommCare")
                processed_records.append(doc_id)
                logger.info({
                    "message": f"Processed successfully record with Request ID {request_id} to Salesforce",
                    "request_id": request_id,
                    "doc_id": doc_id
                })
            else:
                # If failed, mark record as failed with the error
                update_firestore_status(doc_id, "failed", "CommCare", {"error": error})
                logger.error({
                    "message": f"Failed to process record with Request ID {request_id} to Salesforce",
                    "request_id": request_id,
                    "doc_id": doc_id,
                    "error": error
                })

        except Exception as e:
            # In case of error, mark as failed and log the error
            update_firestore_status(doc_id, "failed", "CommCare", {"error": str(e)})
            logger.error({
                "message": f"Error processing record with Request ID {request_id} to Salesforce",
                "request_id": request_id,
                "doc_id": doc_id,
                "error": str(e)
            })

    return processed_records

@app.route('/process-firestore-to-sf', methods=['POST'])
async def process_firestore():
    try:
        logger.info({
            "message": "Batch processing started from scheduler"
        })
        
        # Assuming process_firestore_records is asynchronous and processes records in batches
        processed_records = await process_firestore_records()

        logger.info({
            "message": "Batch processing completed",
            "processed_records": processed_records
        })

        return jsonify({"status": "Processing completed", "processed_records": processed_records}), 200

    except Exception as e:
        logger.error({
            "message": "Error in processing batch",
            "error": str(e)
        })
        return jsonify({"error": f"Error processing Firestore records: {str(e)}"}), 500

async def process_failed_records():
    # Query records to send to SF. 10 Records at a time
    docs = db.collection("job_statuses").where(
        "status", "==", "failed"
    ).where(
        "job_name", "in", ["Farmer Registration", "Attendance Full - Current Module"]
    ).where(
        "run_retries", "<", 10
    ).limit(10).get()

    processed_records = []
    for doc in docs:
        doc_id = doc.id
        data = doc.to_dict()  # Extract data from Firestore
        request_id = data.get("data", {}).get("id")  # Request ID: Used to track the record in logs
        job_name = data.get("job_name")

        try:
            logger.info({
                "message": f"Started sending record with Request ID {request_id} to Salesforce",
                "request_id": request_id,
                "doc_id": doc_id
            })

            update_firestore_status(doc_id, "processing", "CommCare")  # Set status to "processing" before handling the record

            # Check if the job is Farmer Registration
            if job_name == "Farmer Registration":
                success, error = await registration.send_to_salesforce(data.get("data"), sf_connection)
            elif job_name == "Attendance Full - Current Module":
                success, error = await attendance.send_to_salesforce(data.get("data"), sf_connection)

            if success:
                # If processing is successful, mark as completed
                update_firestore_status(doc_id, "completed", "CommCare")
                processed_records.append(doc_id)
                logger.info({
                    "message": f"Processed successfully record with Request ID {request_id} to Salesforce",
                    "request_id": request_id,
                    "doc_id": doc_id
                })
            else:
                # If failed, mark record as failed with the error
                update_firestore_status(doc_id, "failed", "CommCare", {"error": error, "run_retries": data.get("run_retries", 0) + 1})
                logger.error({
                    "message": f"Failed to process record with Request ID {request_id} to Salesforce",
                    "request_id": request_id,
                    "doc_id": doc_id,
                    "error": error
                })

        except Exception as e:
            # In case of error, mark as failed and log the error
            update_firestore_status(doc_id, "failed", "CommCare", {"error": str(e), "run_retries": data.get("run_retries", 0) + 1})
            logger.error({
                "message": f"Error processing record with Request ID {request_id} to Salesforce",
                "request_id": request_id,
                "doc_id": doc_id,
                "error": str(e)
            })

    return processed_records

@app.route('/auto-retry', methods=['POST'])
async def retry_firestore_records():
    try:
        logger.info({
            "message": "Auto Retrying started from scheduler"
        })
        
        # Assuming process_firestore_records is asynchronous and processes records in batches
        processed_records = await process_failed_records()

        logger.info({
            "message": "Auto Retrying processing completed",
            "processed_records": processed_records
        })

        return jsonify({"status": "Processing completed", "processed_records": processed_records, "processed_records": len(processed_records)}), 200

    except Exception as e:
        logger.error({
            "message": "Error in processing Auto Retrying",
            "error": str(e), 
        })
        return jsonify({"error": f"Error retrying Firestore records: {str(e)}"}), 500

@app.route('/record/<id>', methods=['GET'])
def get_record(id):
    try:
        docs = db.collection("job_statuses").where("data.id", "==", id).get()

        if not docs:
            return jsonify({"message": "No records found", "id": id}), 404

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
    
@app.route('/retry/<collection>/<id>', methods=['GET']) 
async def retry_record(collection, id):
    
    collection_mapping = {
        'job_statuses': {'destination': 'Salesforce', 'origin': 'CommCare'},
        'salesforce_collection': {'destination': 'CommCare', 'origin': 'Salesforce'}
    }
    
    # Use the mapping to set destination and origin, defaulting to None if the collection is not found
    destination, origin = collection_mapping.get(collection, {'destination': None, 'origin': None}).values()
    
    try:
        # Query Firestore for documents matching the given ID
        docs = db.collection(collection).where("data.id", "==", id).get()

        if not docs:
            logger.info({
                "message": "No records found for retry",
                "id": id
            })
            return jsonify({"message": "No records found", "id": id}), 404

        # Process each document retrieved
    
        for doc in docs:
            doc_id = doc.id
            data = doc.to_dict()
            request_id = data.get("data", {}).get("id")
            job_name = data.get("job_name")

            try:
                logger.info({
                    "message": f"Retrying record with Request ID {request_id} to {destination}",
                    "request_id": request_id,
                    "job_name": job_name
                })

                # Check the job type and call the appropriate processing function
                
                # 1. Participant Registration
                if job_name == "Farmer Registration":
                    success, error = await registration.send_to_salesforce(data.get("data"), sf_connection)
                    logger.info(f'Process was successful: {success}')
                
                # 2. Participant Send to CommCare  
                elif job_name == "Participant":
                    success, error = await process_commcare_data.process_participant(data.get("data"))
                    logger.info(f'Process was successful: {success}')

                if success:
                    # If successful, update Firestore status to completed
                    update_firestore_status(doc_id, "completed", origin)
                    logger.info({
                        "message": f"Processed successfully record with Request ID {request_id} to {destination}",
                        "request_id": request_id,
                        "doc_id": doc_id,
                        "run_retries": data.get("run_retries", 0) + 1
                    })
                else:
                    # If failed, update Firestore status to failed with error
                    update_firestore_status(doc_id, "failed", origin, {
                        "error": error,
                        "run_retries": data.get("run_retries", 0) + 1
                    })
                    logger.error({
                        "message": f"Failed to process record with Request ID {request_id} to {destination}",
                        "request_id": request_id,
                        "error": error
                    })

            except Exception as e:
                # Handle any exceptions during processing
                update_firestore_status(doc_id, "failed", origin, {
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

@app.route('/failed', methods=['GET'])
def get_failed_jobs():
    """
    Endpoint to retrieve jobs with 'failed' status from the Firestore database.
    """
    try:
        # Query Firestore for documents with status 'failed', limited to 5
        processing_docs = db.collection("job_statuses").where("status", "==", "failed").limit(5).get()

        # Extract relevant fields from Firestore documents
        failed_jobs = [
            {
                "id": doc.id,  # Firestore document ID
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

@app.route('/status-count', methods=['GET'])
def status_count():
    try:
        status_count_dict = {}

        new_docs = db.collection("job_statuses").where("status", "==", "new").where("job_name", "==", "Attendance Full - Current Module").count()

        completed_docs = db.collection("job_statuses").where("status", "==", "completed").where("job_name", "==", "Attendance Full - Current Module").count()

        status_count_dict["completed"] = completed_docs.get()[0][0].value
        status_count_dict["new"] = new_docs.get()[0][0].value

        # Query for "processing" status
        processing_docs = db.collection("job_statuses").where("status", "==", "processing").where("job_name", "==", "Attendance Full - Current Module").count()
        status_count_dict["processing"] = processing_docs.get()[0][0].value

        # Query for "failed" status
        failed_docs = db.collection("job_statuses").where("status", "==", "failed").where("job_name", "==", "Attendance Full - Current Module").count()
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
