def upsert_to_salesforce(object_name, external_id_field, external_id, record_data, sf_connection):
    try:
        result = sf_connection.__getattr__(object_name).upsert(f"{external_id_field}/{external_id}", record_data)
        print(f"Upserted {object_name}: {result} with external ID {external_id_field}:{external_id}")
        return result
    except Exception as e:
        raise Exception(f"Error upserting {object_name}: {e} data {record_data}") 
 