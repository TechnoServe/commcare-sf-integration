from flask import Flask, request, jsonify
import asyncio
from google.cloud import firestore, logging as cloud_logging
from jobs import registration  # Import job modules
from utils.firestore_client import save_to_firestore, update_firestore_status
import os
from simple_salesforce import Salesforce
from dotenv import load_dotenv
import logging


load_dotenv()  # Load environment variables

app = Flask(__name__)
db = firestore.Client()

# Logging
cloud_logging_client = cloud_logging.Client()
cloud_logging_client.setup_logging(log_level=logging.INFO)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler()]  # Local logs to console
)

# Salesforce Authentication
def authenticate_salesforce():
    environment = os.getenv('SALESFORCE_ENV', 'sandbox')
    print("Environment is" + environment)
    password = os.getenv('SF_PROD_PASSWORD')
    security_token = os.getenv('SF_PROD_SECURITY_TOKEN')
    
    if environment == 'sandbox':
        username = os.getenv('SF_TEST_USERNAME')
        domain = 'test'  # Salesforce sandbox domain
    else:
        username = os.getenv('SF_PROD_USERNAME') 
        domain = 'login'  # Salesforce production domain

    try:
        sf = Salesforce(username=username, password=password, security_token=security_token, domain=domain)
        logging.info(f"Authenticated with Salesforce ({environment})")
        return sf
    except Exception as e:
        logging.error(f"Error authenticating with Salesforce ({environment}): {e}")
        return None

# Initialize Salesforce connection
sf_connection = authenticate_salesforce()

@app.route('/process-data', methods=['POST'])
def process_data():
    data = request.get_json()

    # Ensure data is provided
    if not data:
        return jsonify({"error": "Invalid payload"}), 400

    # Parse job type from the payload
    job_name = data.get("form", {}).get("@name")

    request_id = data.get("id")

    if not job_name:
        return jsonify({"error": "Job name not provided in payload"}), 200

    try:
        logging.info("Storing data in Firestore", extra={"request_id": request_id, "job_name": job_name})
        doc_id = save_to_firestore(data, job_name, "new")
        logging.info("Data stored in Firestore", extra={"request_id": request_id, "doc_id": doc_id})
    except Exception as e:
        logging.error("Failed to save data to Firestore", extra={"request_id": request_id, "error": str(e)})
        return jsonify({"error": f"Failed to save data to Firestore: {str(e)}"}), 500

    # Respond immediately with status 200, processing to be done asynchronously
    return jsonify({"status": "Data stored, processing deferred"}), 200

async def process_firestore_records():
    # Query Firestore for new records, limit to a manageable batch size
    docs = db.collection('job_statuses').where("status", "==", "completed").where("job_name", "==", "Farmer Registration").limit(10).get()

    processed_records = []
    for doc in docs: 

        print("processing for " + doc.id)
        doc_id = doc.id
        data = doc.to_dict()

        request_id = data.get('data').get('id')
        try:
            logging.info("Processing Firestore document", extra={"request_id": request_id})
            # Set status to "processing" before handling the record
            update_firestore_status(doc_id, "processing")

            # Await the send_to_salesforce function
            if data.get('job_name') == 'Farmer Registration':
                await registration.send_to_salesforce(data.get('data'), sf_connection)  

            # Mark record as "completed"
            update_firestore_status(doc_id, "completed")
            processed_records.append(doc_id)
            logging.info("Firestore document processed successfully", extra={"request_id": request_id})
        except Exception as e:
            # Mark document as "failed" on error
            update_firestore_status(doc_id, "failed")
            logging.error("Error processing Firestore document", extra={"request_id": request_id, "error": str(e)})


    return processed_records


@app.route('/process-firestore-to-sf', methods=['POST'])
def process_firestore():
    try:
        logging.info("Batch processing begin from scheduler", extra={"processed_records": processed_records})
        processed_records = asyncio.run(process_firestore_records())
        logging.info("Batch processing done from scheduler", extra={"processed_records": processed_records})
        return jsonify({"status": "Processing completed", "processed_records": processed_records}), 200
    except Exception as e:
        logging.error("Error in processing batch", extra={"error": str(e)})
        return jsonify({"error": f"Error processing Firestore records: {str(e)}"}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
