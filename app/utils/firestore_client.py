from jobs import registration
from google.cloud import firestore

db = firestore.Client()

def save_to_firestore(data, job_name, status):
    doc_ref = db.collection("job_statuses").add({
        "data": data,
        "job_name": job_name,
        "job_id": data.get("id"),
        "status": status,
        "run_retries": 0,
        "last_retried_at": "",
        "response_from_sf": "",
        "created_at": firestore.SERVER_TIMESTAMP,
        "updated_at": firestore.SERVER_TIMESTAMP,
    })
    return doc_ref[1].id  # Return document ID for status updates

def update_firestore_status(doc_id, status):
    db.collection("job_statuses").document(doc_id).update({"status": status})

def retry_failed_jobs():
    # Query Firestore for all records with 'failed' status
    failed_jobs = db.collection("job_statuses").where("status", "==", "failed").stream()

    for job in failed_jobs:
        doc_id = job.id
        data = job.to_dict()["data"]
        job_name = job.to_dict()["job_name"]

        # Route job to correct processing function
        if job_name == "Farmer Registration":
            success = registration.process_registration(data)
        # Add other job processing as needed

        # Update Firestore status based on retry outcome
        new_status = "processed" if success else "failed"
        update_firestore_status(doc_id, new_status)
