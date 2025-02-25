from unittest.mock import MagicMock, patch
from utils.firestore_client import save_to_firestore, update_firestore_status
from google.cloud import firestore

@patch("utils.firestore_client.get_firestore_client")
def test_save_to_firestore(mock_get_firestore_client):
    # Mock Firestore setup
    mock_db = MagicMock()
    mock_collection = MagicMock()
    mock_doc_ref = MagicMock()
    mock_get_firestore_client.return_value = mock_db
    mock_db.collection.return_value = mock_collection
    mock_collection.add.return_value = [None, MagicMock(id="mock_doc_id")]

    # Test data
    data = {"test": "data"}
    job_name = "Test Job"
    status = "new"

    # Call the function
    doc_id = save_to_firestore(data, job_name, status, db=mock_db)

    # Assertions
    mock_collection.add.assert_called_once_with({
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
    assert doc_id == "mock_doc_id"

@patch("utils.firestore_client.get_firestore_client")
def test_update_firestore_status(mock_get_firestore_client):
    # Mock Firestore setup
    mock_db = MagicMock()
    mock_collection = MagicMock()
    mock_doc = MagicMock()
    mock_get_firestore_client.return_value = mock_db
    mock_db.collection.return_value = mock_collection
    mock_collection.document.return_value = mock_doc

    # Test data
    doc_id = "mock_doc_id"
    status = "completed"
    fields = {"last_step": "step1"}

    # Call the function
    success = update_firestore_status(doc_id, status, fields, db=mock_db)

    # Assertions
    mock_collection.document.assert_called_once_with(doc_id)
    mock_doc.update.assert_called_once_with({
        "status": status,
        "last_step": "step1",
    })
    assert success is True
