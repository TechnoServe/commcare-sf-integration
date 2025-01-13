from flask import Flask, request, jsonify
import asyncio
from google.cloud import firestore
from jobs import registration  # Import job modules
from utils.firestore_client import save_to_firestore, update_firestore_status
import os
from simple_salesforce import Salesforce
from dotenv import load_dotenv
from utils.logging_config import logger  # Import the centralized logger

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
        doc_id = save_to_firestore(data, job_name, "new")
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
    docs = db.collection("job_statuses").where("status", "==", "completed").where("job_name", "==", "Farmer Registration").limit(10).get()

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

            update_firestore_status(doc_id, "processing")  # Set status to "processing" before handling the record

            # Check if the job is Farmer Registration
            if job_name == "Farmer Registration":
                success, error = await registration.send_to_salesforce(data.get("data"), sf_connection)

            if success:
                # If processing is successful, mark as completed
                update_firestore_status(doc_id, "completed")
                processed_records.append(doc_id)
                logger.info({
                    "message": f"Processed successfully record with Request ID {request_id} to Salesforce",
                    "request_id": request_id,
                    "doc_id": doc_id
                })
            else:
                # If failed, mark record as failed with the error
                update_firestore_status(doc_id, "failed", {"error": error})
                logger.error({
                    "message": f"Failed to process record with Request ID {request_id} to Salesforce",
                    "request_id": request_id,
                    "doc_id": doc_id,
                    "error": error
                })

        except Exception as e:
            # In case of error, mark as failed and log the error
            update_firestore_status(doc_id, "failed", {"error": str(e)})
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
    
@app.route('/retry/<id>', methods=['GET']) 
async def retry_record(id):
    try:
        # Query Firestore for documents matching the given ID
        docs = db.collection("job_statuses").where("data.id", "==", id).get()

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
                    "message": f"Retrying record with Request ID {request_id} to Salesforce",
                    "request_id": request_id,
                    "job_name": job_name
                })

                # Check the job type and call the appropriate processing function
                if job_name == "Farmer Registration":
                    success, error = await registration.send_to_salesforce(data.get("data"), sf_connection)
                    logger.info(f'Process was successful: {success}')
                    logger.info(f'Testing this: {data.get("run_retries", 0) + 1}')

                if success:
                    # If successful, update Firestore status to completed
                    update_firestore_status(doc_id, "completed")
                    logger.info({
                        "message": f"Processed successfully record with Request ID {request_id} to Salesforce",
                        "request_id": request_id,
                        "doc_id": doc_id,
                        "run_retries": data.get("run_retries", 0) + 1
                    })
                else:
                    # If failed, update Firestore status to failed with error
                    update_firestore_status(doc_id, "failed", {
                        "error": error,
                        "run_retries": data.get("run_retries", 0) + 1
                    })
                    logger.error({
                        "message": f"Failed to process record with Request ID {request_id} to Salesforce",
                        "request_id": request_id,
                        "error": error
                    })

            except Exception as e:
                # Handle any exceptions during processing
                update_firestore_status(doc_id, "failed", {
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

@app.route('/status-count', methods=['GET'])
def status_count():
    try:
        status_count_dict = {}

        # completed_docs = db.collection("job_statuses").where("status", "==", "completed").stream()
        # status_count_dict["completed"] = sum(1 for _ in completed_docs)

        # Query for "processing" status
        processing_docs = db.collection("job_statuses").where("status", "==", "processing").stream()
        status_count_dict["processing"] = sum(1 for _ in processing_docs)

        # Query for "failed" status
        failed_docs = db.collection("job_statuses").where("status", "==", "failed").stream()
        status_count_dict["failed"] = sum(1 for _ in failed_docs)

        return jsonify({"status_counts": status_count_dict}), 200

    except Exception as e:
        logger.error({
            "message": "Error retrieving status count",
            "error": str(e)
        })
        return jsonify({"error": f"Error retrieving status count: {str(e)}"}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)
