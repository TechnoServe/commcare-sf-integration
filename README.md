# CommCare Salesforce Integration Platform

> A comprehensive data integration and processing platform that orchestrates bi-directional data synchronization between CommCare mobile applications, Salesforce CRM, and PostgreSQL databases for TechnoServe's coffee portfolio programs.

[![Python Version](https://img.shields.io/badge/python-3.8%2B-blue)]()
[![Flask](https://img.shields.io/badge/flask-2.0%2B-green)]()
[![Cloud Run](https://img.shields.io/badge/Google%20Cloud-Run-orange)]()
[![License](https://img.shields.io/badge/license-MIT-green)]()

## 🌟 Overview

This integration platform serves as the central data processing hub for TechnoServe's coffee portfolio programs, handling complex data workflows between CommCare mobile data collection, Salesforce CRM management, and PostgreSQL analytics databases. The system processes multiple form types including farmer registrations, training sessions, farm visits, wet mill operations, and attendance tracking across multiple countries.

### Key Capabilities

- **🔄 Bi-directional Sync**: CommCare ↔ Salesforce ↔ PostgreSQL data synchronization
- **📊 Multi-Program Support**: Handles PIMA Agronomy and PIMA Sustainability programs
- **🏭 Asynchronous Processing**: Firestore-based queue system for reliable data processing
- **🌍 Multi-Country Operations**: Supports coffee programs across different regions
- **📱 Mobile-First**: Optimized for CommCare mobile data collection workflows
- **🔍 Comprehensive Logging**: Structured logging with request tracking and error handling
- **♻️ Auto-Retry Logic**: Intelligent retry mechanisms for failed processing jobs
- **📈 Real-time Monitoring**: Status tracking and processing analytics

## 🏗️ System Architecture

```
┌─────────────────┐    ┌──────────────────────┐    ┌─────────────────┐
│   CommCare      │────│  Flask Web Service   │────│   Salesforce    │
│   Mobile Forms  │    │  (Cloud Run)         │    │     CRM         │
│                 │    │                      │    │                 │
│ • Registrations │    │  ┌─────────────────┐ │    │ • Participants  │
│ • Farm Visits   │────│──│  Firestore      │ │────│ • Training Data │
│ • Training Obs. │    │  │  Queue System   │ │    │ • Farm Records  │
│ • Attendance    │    │  └─────────────────┘ │    │ • Observations  │
└─────────────────┘    │                      │    └─────────────────┘
                       │  ┌─────────────────┐ │
                       │  │  PostgreSQL     │ │
                       │  │  (PostGIS)      │ │
                       │  │                 │ │
                       │  │ • Wet Mills     │ │
                       │  │ • Survey Data   │ │
                       │  │ • Geospatial    │ │
                       │  └─────────────────┘ │
                       └──────────────────────┘
```

## 🚀 Supported Form Types

### PIMA Agronomy (CommCare → Salesforce)
- **Farmer Registration** - New farmer onboarding and profile creation
- **Edit Farmer Details** - Farmer profile updates and modifications
- **Field Day Farmer Registration** - Event-specific farmer registration
- **Attendance Full/Light** - Training session participation tracking
- **Field Day Attendance** - Event attendance management
- **Training Observation** - Training quality assessment and feedback
- **Demo Plot Observation** - Agricultural demonstration tracking
- **Farm Visit Full/AA** - Comprehensive farm assessment visits

### PIMA Sustainability (CommCare → PostgreSQL)
- **Wet Mill Registration** - Coffee processing facility registration
- **Wet Mill Visit** - Facility inspection and survey data collection

### Salesforce → CommCare Sync
- **Participant Data** - Farmer and participant information sync
- **Training Groups** - Training cohort management
- **Training Sessions** - Session scheduling and details
- **Project Roles** - Staff and role assignments
- **Household Sampling** - Household survey data

## 📁 Project Structure

```
app/
├── jobs/
│   ├── commcare_to_postgresql/
│   │   ├── wetmill_registration.py    # Wet mill facility processing
│   │   └── wetmill_visit.py           # Survey data and geospatial processing
│   ├── commcare_to_salesforce/
│   │   ├── registration.py            # Farmer registration workflows
│   │   ├── attendance.py              # Training attendance processing
│   │   ├── training_observation.py    # Training quality assessments
│   │   ├── demoplot_observation.py    # Demo plot data processing
│   │   └── farm_visit.py              # Comprehensive farm visit processing
│   └── salesforce_to_commcare/
│       └── process_commcare_data.py   # Parallel data sync to CommCare
├── utils/
│   ├── models.py                      # SQLAlchemy database models
│   ├── postgres.py                    # PostgreSQL connection management
│   ├── mappings.py                    # Data transformation mappings
│   ├── logging_config.py              # Centralized logging configuration
│   └── [various utility modules]     # Processing utilities
└── main.py                           # Flask application and routing
```

## 🛠️ Installation & Setup

### Prerequisites

- Python 3.8+
- PostgreSQL 14+ with PostGIS extension
- Google Cloud SDK
- CommCare API credentials
- Salesforce API credentials
- Google Firestore database

### Local Development Setup

1. **Clone and set up environment**
   ```bash
   git clone https://github.com/TechnoServe/commcare-sf-integration.git
   cd commcare-sf-integration
   python3 -m venv venv
   source venv/bin/activate  # Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

2. **Configure environment variables**
   ```bash
   cp .env.example .env
   # Edit .env with your credentials
   ```

3. **Set up PostgreSQL with PostGIS**
   ```bash
   # Ubuntu/Debian
   sudo apt update
   sudo apt install postgresql-14-postgis-3
   
   # Create database and extensions
   createdb commcare_integration
   psql -d commcare_integration -c "CREATE EXTENSION IF NOT EXISTS postgis;"
   psql -d commcare_integration -c "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";"
   ```

4. **Initialize database**
   ```bash
   python3 -c "from utils.postgres import init_db; init_db()"
   ```

5. **Run locally**
   ```bash
   python3 app/main.py
   ```

## 🌐 API Endpoints

### Data Processing Endpoints

#### Process CommCare Data
```http
POST /process-data-cc
Content-Type: application/json

# Processes incoming CommCare form submissions
# Routes to appropriate handler based on form type
# Stores in Firestore queue for async processing
```

#### Process Salesforce Data
```http
POST /process-data-sf
Content-Type: application/json

# Processes incoming Salesforce data
# Handles participant, training, and project data
# Queues for CommCare synchronization
```

### Batch Processing Endpoints

#### Process Firestore Queue to Salesforce
```http
POST /process-firestore-to-sf

# Processes batched CommCare data to Salesforce
# Handles up to 10 records per batch
# Updates Firestore status tracking
```

#### Process Firestore Queue to CommCare
```http
POST /process-firestore-to-cc

# Processes Salesforce data to CommCare
# Handles 1 record per batch (due to complexity)
# XML generation and parallel processing
```

### Monitoring & Management Endpoints

#### Auto-Retry Failed Records
```http
POST /auto-retry-firestore-to-sf
POST /auto-retry-firestore-to-cc

# Automatically retries failed records
# Up to 3 retry attempts per record
# Incremental backoff strategy
```

#### Manual Record Retry
```http
GET /retry/sf/{record_id}
GET /retry/cc/{record_id}

# Manual retry for specific failed records
# Updates retry counters and timestamps
# Useful for debugging and manual intervention
```

#### Get Record Details
```http
GET /record/{collection}/{id}

# Retrieves specific record from Firestore
# Returns processing history and current status
# Useful for debugging and tracking
```

#### Failed Jobs Dashboard
```http
GET /failed/sf
GET /failed/cc

# Returns all failed jobs with error details
# Includes retry counts and failure reasons
# Essential for monitoring system health
```

#### Status Counts
```http
GET /status-count/{collection}

# Returns processing statistics
# Counts: new, processing, completed, failed
# Real-time system health monitoring
```

## 🔧 Configuration

### Environment Variables

```bash
# Salesforce Configuration
SALESFORCE_ENV=sandbox  # or production
SF_TEST_USERNAME=your_sandbox_username
SF_PROD_USERNAME=your_production_username
SF_PROD_PASSWORD=your_password
SF_PROD_SECURITY_TOKEN=your_security_token

# CommCare Configuration
CC_DOMAIN=your_commcare_domain
CC_API_KEY=your_api_key
CC_USERNAME=your_username

# Database Configuration
DATABASE_URL=postgresql://user:password@host:port/database

# Google Cloud
GOOGLE_APPLICATION_CREDENTIALS=path/to/service-account.json
GOOGLE_CLOUD_PROJECT=your-project-id
```

### Form Type Configuration

The system processes these migrated form types:
```python
migrated_form_types = [
    "Farmer Registration", "Attendance Full - Current Module", 
    'Edit Farmer Details', 'Training Observation', 
    "Attendance Light - Current Module", 'Participant', 
    "Training Group", "Training Session", "Project Role", 
    "Household Sampling", "Demo Plot Observation", "Farm Visit Full",
    "Farm Visit - AA", "Field Day Farmer Registration", "Field Day Attendance Full",
    "Wet Mill Registration Form", "Wet Mill Visit"
]
```

## 🗄️ Database Schema

### PostgreSQL Schema (PIMA Sustainability)

#### Wet Mills Table
```sql
CREATE TABLE wetmills (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    wet_mill_unique_id VARCHAR NOT NULL,
    commcare_case_id VARCHAR UNIQUE,
    name VARCHAR,
    mill_status VARCHAR,
    exporting_status VARCHAR,
    vertical_integration VARCHAR,
    manager_name VARCHAR,
    manager_role VARCHAR,
    programme VARCHAR,
    country VARCHAR,
    comments TEXT,
    ba_signature VARCHAR,
    manager_signature VARCHAR,
    tor_page_picture VARCHAR,
    office_entrance_picture VARCHAR,
    registration_date DATE,
    date_ba_signature DATE,
    office_gps geometry(Point,4326),
    user_id UUID REFERENCES tbl_users(user_id),
    created_at TIMESTAMP DEFAULT now(),
    updated_at TIMESTAMP DEFAULT now()
);
```

#### Form Visits Table
```sql
CREATE TABLE form_visits (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    submission_id VARCHAR UNIQUE NOT NULL,
    wetmill_id UUID REFERENCES wetmills(id) ON DELETE CASCADE,
    user_id UUID,
    form_name VARCHAR NOT NULL,
    visit_date TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    entrance_photograph VARCHAR,
    geo_location geometry(Point,4326),
    created_at TIMESTAMP DEFAULT now()
);
```

#### Survey Responses Table
```sql
CREATE TABLE survey_responses (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    submission_id VARCHAR UNIQUE NOT NULL,
    form_visit_id UUID REFERENCES form_visits(id) ON DELETE CASCADE,
    survey_type VARCHAR NOT NULL,
    completed_date TIMESTAMP WITHOUT TIME ZONE,
    general_feedback TEXT,
    created_at TIMESTAMP DEFAULT now()
);
```

#### Survey Question Responses Table
```sql
CREATE TABLE survey_question_responses (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    submission_id VARCHAR UNIQUE NOT NULL,
    survey_response_id UUID REFERENCES survey_responses(id) ON DELETE CASCADE,
    section_name VARCHAR,
    question_name VARCHAR NOT NULL,
    field_type VARCHAR NOT NULL,
    value_text TEXT,
    value_number DOUBLE PRECISION,
    value_boolean BOOLEAN,
    value_date TIMESTAMP WITHOUT TIME ZONE,
    value_gps geometry(Point,4326)
);
```

### Firestore Collections

#### CommCare Collection
- **Purpose**: Queue CommCare form submissions for Salesforce processing
- **Fields**: `data`, `job_name`, `status`, `timestamp`, `run_retries`, `error`
- **Statuses**: `new`, `processing`, `completed`, `failed`

#### Salesforce Collection
- **Purpose**: Queue Salesforce data for CommCare synchronization
- **Fields**: `data`, `job_name`, `status`, `timestamp`, `run_retries`, `error`
- **Processing**: Generates XML for CommCare case creation/updates

## 🔄 Data Processing Workflows

### CommCare → Salesforce Workflow

1. **Form Submission**: CommCare sends webhook to `/process-data-cc`
2. **Immediate Storage**: Data stored in Firestore with `new` status
3. **Batch Processing**: Scheduler triggers `/process-firestore-to-sf`
4. **Data Transformation**: Form data mapped to Salesforce objects
5. **Multi-Step Processing**: Complex forms processed in sequential steps
6. **Status Update**: Firestore status updated to `completed` or `failed`
7. **Error Handling**: Failed records eligible for auto-retry

### Salesforce → CommCare Workflow

1. **Data Trigger**: Salesforce sends data to `/process-data-sf`
2. **Queue Storage**: Data stored in Firestore for processing
3. **XML Generation**: Salesforce data converted to CommCare XML format
4. **Parallel Processing**: Multiple records processed concurrently
5. **Case Management**: Creates/updates CommCare cases via API
6. **Status Tracking**: Processing results tracked in Firestore

### PostgreSQL Direct Processing

1. **Wet Mill Forms**: Processed immediately without queuing
2. **Spatial Data**: GPS coordinates stored as PostGIS geometry
3. **Survey Processing**: Dynamic survey responses with flexible schema
4. **Upsert Logic**: Updates existing records or creates new ones
5. **Data Validation**: Type inference and validation for survey responses

## 🚀 Cloud Run Deployment

### Build and Deploy

```bash
# Set up Google Cloud project
gcloud config set project your-project-id

# Enable required services
gcloud services enable cloudbuild.googleapis.com
gcloud services enable run.googleapis.com
gcloud services enable firestore.googleapis.com

# Build container
gcloud builds submit --tag gcr.io/your-project/cc-sf-integration-app

# Deploy to Cloud Run
gcloud run deploy cc-sf-integration-app \
  --image gcr.io/your-project/cc-sf-integration-app \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated \
  --memory 2Gi \
  --cpu 2 \
  --timeout 3600 \
  --concurrency 10 \
  --max-instances 50
```

### Environment Configuration

```bash
# Set environment variables
gcloud run services update cc-sf-integration-app \
  --set-env-vars "SALESFORCE_ENV=production,DATABASE_URL=your_db_url"
```

### Scheduler Setup

```bash
# Create scheduler jobs for batch processing
gcloud scheduler jobs create http sf-batch-processor \
  --schedule="*/5 * * * *" \
  --uri="https://your-service-url/process-firestore-to-sf" \
  --http-method=POST

gcloud scheduler jobs create http cc-batch-processor \
  --schedule="*/2 * * * *" \
  --uri="https://your-service-url/process-firestore-to-cc" \
  --http-method=POST

# Auto-retry failed records
gcloud scheduler jobs create http auto-retry-sf \
  --schedule="0 */2 * * *" \
  --uri="https://your-service-url/auto-retry-firestore-to-sf" \
  --http-method=POST
```

## 📊 Monitoring & Observability

### Logging

The system uses structured logging with request tracking:

```python
logger.info({
    "message": "Processing farm visit",
    "request_id": request_id,
    "job_name": job_name,
    "processing_step": "best_practices"
})
```

### Key Metrics to Monitor

- **Processing Rate**: Records processed per minute
- **Queue Depth**: Number of pending records in Firestore
- **Error Rate**: Failed processing percentage
- **Retry Success**: Auto-retry effectiveness
- **Processing Time**: Average time per record type

### Cloud Logging Queries

```bash
# View processing logs
gcloud logging read "resource.type=cloud_run_revision AND resource.labels.service_name=cc-sf-integration-app" --limit=100

# Monitor errors
gcloud logging read "resource.type=cloud_run_revision AND severity>=ERROR" --limit=50

# Track specific request
gcloud logging read "jsonPayload.request_id=\"your-request-id\"" --limit=20
```

## 🛠️ Troubleshooting

### Common Issues

1. **Database Connection Errors**
   ```bash
   # Check PostgreSQL connectivity
   psql $DATABASE_URL -c "SELECT version();"
   
   # Verify PostGIS extension
   psql $DATABASE_URL -c "SELECT PostGIS_version();"
   ```

2. **Salesforce Authentication Issues**
   ```bash
   # Test Salesforce connection
   python3 -c "from main import authenticate_salesforce; print(authenticate_salesforce())"
   ```

3. **CommCare API Issues**
   ```bash
   # Verify CommCare credentials
   curl -H "Authorization: ApiKey username:apikey" \
        https://www.commcarehq.org/a/domain/api/v0.5/user/
   ```

4. **Firestore Queue Backlog**
   ```bash
   # Check queue status
   curl https://your-service-url/status-count/commcare_collection
   ```

### Performance Optimization

- **Batch Size Tuning**: Adjust query limits in `process_firestore_records()`
- **Concurrency Limits**: Modify semaphore limits for parallel processing
- **Database Indexing**: Add indexes for frequently queried fields
- **Memory Allocation**: Increase Cloud Run memory for large batches

## 🤝 Contributing

### Development Guidelines

1. **Code Style**: Follow PEP 8 standards
2. **Error Handling**: Always include try-except blocks with logging
3. **Documentation**: Update README for new form types or endpoints
4. **Testing**: Add unit tests for new processing functions

### Adding New Form Types

1. **Update Form List**: Add to `migrated_form_types` in `main.py`
2. **Create Processor**: Add handler in appropriate `jobs/` directory
3. **Add Routing**: Update processing logic in main Flask routes
4. **Test Thoroughly**: Verify end-to-end processing

## 📞 Support & Contact

- **Technical Issues**: Create GitHub issues with detailed error logs
- **TechnoServe PIMA Team**: Internal Slack `#pima`
- **Production Issues**: Follow incident response procedures

## 📜 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

<div align="center">
  <strong>🌍 Empowering Agricultural Development Through Data Integration</strong><br>
  <em>Built by TechnoServe for creating business solutions to poverty</em>
</div>