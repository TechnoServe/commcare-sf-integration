def training_group_exists(sf_connection, training_group_id):
        """Check if the Training Group exists in Salesforce."""
        try:
            response = sf_connection.Training_Group__c.get(training_group_id)
            return response is not None
        except Exception:
            return False