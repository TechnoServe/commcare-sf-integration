from google.cloud import firestore
from utils.logging_config import logger
from datetime import datetime
import functools
import asyncio

def get_firestore_client():
    return firestore.Client()

def save_to_firestore(data, job_name, status, collection, db=None):
    if db is None:
        db = get_firestore_client()
        
    doc_ref = db.collection(collection).add({
        "data": data,
        "job_name": job_name,
        "job_id": data.get("id"),
        "status": status,
        "run_retries": 0,
        "last_retried_at": "",
        "last_step": "",
        "created_at": firestore.SERVER_TIMESTAMP,
        "updated_at": firestore.SERVER_TIMESTAMP,
    })
    
    return doc_ref[1].id

def update_firestore_status(doc_id, status, collection, fields=None, db=None):
    if db is None:
        db = get_firestore_client()
    try:
        update_data = {"status": status}
        if fields:
            update_data.update(fields)
        db.collection(collection).document(doc_id).update(update_data)
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

# async def save_to_firestore(data, job_name, status, collection, db=None):
#     """Run Firestore save operation asynchronously using a thread pool."""
#     loop = asyncio.get_running_loop()
#     return await loop.run_in_executor(None, functools.partial(save_to_firestore_sync, data, job_name, status, collection, db))

# async def update_firestore_status(doc_id, status, collection, fields=None, db=None):
#     """Run Firestore update operation asynchronously using a thread pool."""
#     loop = asyncio.get_running_loop()
#     return await loop.run_in_executor(None, functools.partial(update_firestore_status_sync, doc_id, status, collection, fields, db))