# commcare-sf-integration

# Run Tests: 
pytest --maxfail=1 --disable-warnings -vv
# Or use this command: 
PYTHONPATH=app pytest --maxfail=1 --disable-warnings -vv
# Use this to build and submit: 
gcloud builds submit --tag gcr.io/tns-coffee-data/commcare-processor
# Use this to run the deploy: 
gcloud run deploy commcare-processor --image gcr.io/tns-coffee-data/commcare-processor --platform managed --allow-unauthenticated


# Activate virtual environment
source venv/bin/activate

# Start server 
python3 app/main.py


# Setting up Postgres

## Install PostGIS 
1. Install Postgres
2. Install PostGIS (if ubuntu, windows download in installer)

 - sudo apt update
 - sudo apt install postgresql-14-postgis-3


## Create tables in database

CREATE DB IF NOT EXISTS my_pima_db


## Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS postgis;

### 1. wetmills
CREATE TABLE wetmills (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  wet_mill_unique_id VARCHAR NOT NULL,
  commcare_case_id VARCHAR UNIQUE,
  name VARCHAR,
  mill_status VARCHAR,
  exporting_status VARCHAR,
  programe VARCHAR,
  country VARCHAR,
  manager_name VARCHAR,
  manager_role VARCHAR,
  comments TEXT,
  wetmill_counter INTEGER,
  ba_signature VARCHAR,
  manager_signature VARCHAR,
  tor_page_picture VARCHAR,
  registration_date DATE,
  created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT now(),
  updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT now()
);

### 2. wetmill_visits
CREATE TABLE wetmill_visits (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  wetmill_id UUID REFERENCES wetmills(id) ON DELETE CASCADE,
  user_id UUID,
  form_name VARCHAR NOT NULL,
  visit_date TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  entrance_photograph VARCHAR,
  geo_location geometry(Point,4326),
  created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT now()
);

### 3. survey_responses
CREATE TABLE survey_responses (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  form_visit_id UUID NOT NULL REFERENCES wetmill_visits(id) ON DELETE CASCADE,
  survey_type VARCHAR NOT NULL,
  completed_date TIMESTAMP WITHOUT TIME ZONE,
  general_feedback TEXT,
  created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT now()
);

### 4. survey_question_responses
CREATE TABLE survey_question_responses (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  survey_response_id UUID NOT NULL REFERENCES survey_responses(id) ON DELETE CASCADE,
  section_name VARCHAR,
  question_name VARCHAR NOT NULL,
  field_type VARCHAR NOT NULL,
  value_text TEXT,
  value_number DOUBLE PRECISION,
  value_boolean BOOLEAN,
  value_date TIMESTAMP WITHOUT TIME ZONE,
  value_gps geometry(Point,4326)
);

### 5. Create Users
CREATE TABLE tbl_users (
    user_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    sf_user_id VARCHAR(50),
    user_name VARCHAR(255),
    user_password VARCHAR(255) NOT NULL,
    user_email VARCHAR(255),
    mobile_no VARCHAR(50),
    role_id UUID NOT NULL REFERENCES tbl_roles(role_id),
    account_status VARCHAR DEFAULT 'active',
    "createdAt" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

### Create Project Roles 
CREATE TABLE tbl_project_role (
    pr_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES tbl_users(user_id),
    project_id UUID NOT NULL REFERENCES tbl_projects(project_id),
    role_id UUID NOT NULL REFERENCES tbl_roles(role_id),
    "createdAt" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE wetmills
ADD COLUMN user_id UUID REFERENCES tbl_users(user_id);

