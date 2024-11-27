from google.cloud import firestore
from utils.logging_config import logger

def get_firestore_client():
    return firestore.Client()

def save_to_firestore(data, job_name, status, db=None):
    if db is None:
        db = get_firestore_client()
    doc_ref = db.collection("job_statuses").add({
        "data": data,
        "job_name": job_name,
        "status": status,
        "run_retries": 0,
        "last_retried_at": "",
        "response_from_sf": "",
        "last_step": "",
        "created_at": firestore.SERVER_TIMESTAMP,
        "updated_at": firestore.SERVER_TIMESTAMP,
    })
    return doc_ref[1].id

def update_firestore_status(doc_id, status, fields=None, db=None):
    if db is None:
        db = get_firestore_client()
    try:
        update_data = {"status": status}
        if fields:
            update_data.update(fields)
        
        db.collection("job_statuses").document(doc_id).update(update_data)
        logger.info({
            "message": "Successfully updated Firestore document",
            "doc_id": doc_id,
            "status": status,
            "fields": fields
        })
        return True
    except Exception as e:
        logger.error({
            "message": "Failed to update Firestore document",
            "doc_id": doc_id,
            "status": status,
            "fields": fields,
            "error": str(e)
        })
        return False
