FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

COPY . .

COPY service-account-key.json /app/application_default_credentials.json

# Install dependencies
RUN pip install -r requirements.txt

# Expose port 8080
EXPOSE 8080

# Set Google credentials environment variables
ENV GOOGLE_APPLICATION_CREDENTIALS="/app/application_default_credentials.json"
ENV GOOGLE_CLOUD_PROJECT="tns-coffee-data"

# SF Production credentials
ENV SF_PROD_USERNAME="ymugenga@pima.org"
ENV SF_PROD_PASSWORD="microsoft19?."
ENV SF_PROD_SECURITY_TOKEN="oV2rkDdQV2MTup929V4dIppF"
ENV SF_TEST_USERNAME="ymugenga@pima.org.satellite"


# Change this to sandbox if you want to use SF **sandbox** for deployment of **production** for live data
ENV SALESFORCE_ENV="sandbox"

# Run the Flask app
CMD ["python", "app/main.py"]

