import uuid
import datetime

from geoalchemy2.shape import from_shape
from shapely.geometry import Point
from sqlalchemy.exc import SQLAlchemyError
from utils.postgres import SessionLocal
from utils.models import FormVisit, Wetmill, SurveyResponse, SurveyQuestionResponse, User
from utils.mappings import SURVEY_TRANSFORMATIONS
from utils.logging_config import logger

ALLOWED_SURVEYS = [
    "wet_mill_training",
    "routine_visit",
    "manager_needs_assessment",
    "infrastructure",
    "waste_water_management",
    "cpqi",
    "cherry_weekly_price",
    "kpis",
    "water_and_energy_use",
    "financials",
    "employees",
    "gender_equitable_business_practices"
    ]

def save_form_visit(data, wetmill_id=None, user_id=None):
    session = SessionLocal()
    # try:
    form = data.get("form", {})
    case = form.get("case", {})
    case_id = case.get("@case_id")

    # Lookup/create Wetmill
    # if case_id:
    wetmill = session.query(Wetmill).filter_by(commcare_case_id=case_id).first()
    if not wetmill:
    #     wetmill = Wetmill(commcare_case_id=case_id, wet_mill_unique_id="", name="Placeholder")
    #     session.add(wetmill)
    #     session.flush()
        logger.error({
            "message": "Wetmill not found for case_id",
            "case_id": case_id,
            "request_id": data.get("id")
        })

        return False, f"Wetmill not found for case_id: {case_id}" # return early if wetmill not found
    form_wetmill_id = wetmill.id
    # else:
    #     form_wetmill_id = wetmill_id

    visit_date_str = form.get("date")
    visit_date = datetime.datetime.strptime(visit_date_str, "%Y-%m-%d") if visit_date_str else None
    form_name = form.get("survey_type")
    form_id = data.get("id")
    loc_str = form.get("introduction", {}).get("gps", "")
    point = extract_location_string(loc_str)

    entrance_photograph = f'https://www.commcarehq.org/a/{data.get("domain")}/api/form/attachment/{data.get("form", {}).get("meta", {}).get("instanceID")}/{data.get("form", {}).get("introduction", {}).get("wetmill_entrance_photograph")}' if data.get("form", {}).get("introduction", {}).get("wetmill_entrance_photograph") else None
    
    form_visit = session.query(FormVisit).filter_by(
            submission_id=f'WV-{form_id}'
        ).first()
    
    if form_visit:
        form_visit.wetmill_id = form_wetmill_id
        form_visit.user_id = user_id
        form_visit.form_name = form_name
        form_visit.visit_date = visit_date
        form_visit.entrance_photograph = entrance_photograph
        form_visit.geo_location = from_shape(point, srid=4326) if point else None
        logger.info({
            "message": "Updated existing Wet mill visit",
            "submission_id": f'WV-{form_id}',
            "wetmill_id": form_wetmill_id
        })
    else:
        form_visit = FormVisit(
            submission_id=f'WV-{form_id}',
            wetmill_id=form_wetmill_id,
            user_id=wetmill.user_id,
            form_name=form_name,
            visit_date=visit_date,
            entrance_photograph=entrance_photograph,
            geo_location=from_shape(point, srid=4326) if point else None,
        )
        session.add(form_visit)
        logger.info({
            "message": "Added new Wet mill visit",
            "submission_id": f'WV-{form_id}',
            "wetmill_id": form_wetmill_id
        })

    surveys = form.get("surveys", {})
    completed = form.get("completed_surveys", {})
    url_string = f'https://www.commcarehq.org/a/{data.get("domain")}/api/form/attachment/{data.get("form", {}).get("meta", {}).get("instanceID")}'
    for survey_name, content in surveys.items():
        if survey_name not in ALLOWED_SURVEYS:
            print('skipping survey: ' + survey_name)
            continue
        if not isinstance(content, dict): # Skip non survey 
            continue

        # Apply transformation functions (ETL) for each survey
        transform_func = SURVEY_TRANSFORMATIONS.get(survey_name)

        if transform_func:
            content = transform_func(content, url_string, form)

        # Determine completed date
        date_key = f"{survey_name}_date"
        completed_date = None
        date_str = completed.get(date_key)
        if date_str:
            completed_date = datetime.datetime.strptime(date_str, "%Y-%m-%d")

        general_feedback = content.get("general_feedback")

        survey_response = session.query(SurveyResponse).filter_by(
            submission_id=f'SR-{form_id}-{survey_name}'
        ).first()

        if survey_response:
            survey_response.form_visit = form_visit
            survey_response.survey_type = survey_name
            survey_response.completed_date = completed_date
            survey_response.general_feedback = general_feedback
            logger.info({
                "message": "Updated existing survey response",
                "submission_id": f'SR-{form_id}-{survey_name}',
                "survey_type": survey_name,
                "completed_date": completed_date,
                "survey_response": survey_response.id
            })
        else:
            survey_response = SurveyResponse(
                submission_id=f'SR-{form_id}-{survey_name}',
                form_visit=form_visit,
                survey_type=survey_name,
                completed_date=completed_date,
                general_feedback=general_feedback
            )
            session.add(survey_response)
            logger.info({
                "message": "Added new survey response",
                "submission_id": f'SR-{form_id}-{survey_name}',
                "survey_type": survey_name,
                "completed_date": completed_date,
                "general_feedback": general_feedback
            })

        # Insert question responses
        for section, sec_content in content.items():
            # handle multiple answers questions on top level
            if isinstance(sec_content, list):
                for item in sec_content:
                    item_index = str(sec_content.index(item) + 1)  # 1-based index
                    submission_id = f'SQR-{form_id}-{survey_name}-{section}-{item_index}'
                    add_question_response(session, survey_response, section, section, item, submission_id)
            # Nested questions
            elif isinstance(sec_content, dict):
                for q_name, ans in sec_content.items():
                    # handle multiple answers questions in nested
                    if isinstance(ans, list):
                        for item in ans:
                            item_index = str(ans.index(item) + 1)  # 1-based index
                            submission_id = f'SQR-{form_id}-{survey_name}-{section}-{q_name}-{item_index}'
                            add_question_response(session, survey_response, section, q_name, item, submission_id)
                    else:
                        # Skip label fields
                        if q_name.endswith('_label'):
                            continue
                        submission_id = f'SQR-{form_id}-{survey_name}-{section}-{q_name}'
                        add_question_response(session, survey_response, section, q_name, ans, submission_id)
            # Single value questions or flags
            else:
                # Skip top-level label or survey keys
                if section.endswith('_label') or section.startswith('survey_'):
                    continue
                submission_id = f'SQR-{form_id}-{survey_name}-{section}'
                add_question_response(session, survey_response, None, section, sec_content, submission_id)
    session.commit()
    return True, None
#except SQLAlchemyError as e:
#    session.rollback()
#    return False, str(e)
# except Exception as e:
#   session.rollback()
#    return False, str(e)
#finally:
#    session.close()

def extract_location_string(location_string):
    """
    Parse a GPS string "lat lon ..." into a Shapely Point (lon, lat).
    """
    try:
        lat, lon, *_ = map(float, location_string.split())
        return Point(lon, lat)
    except:
        return None

def infer_field_type(value):
    """
    Infer the data type of the answer and return (field_type, parsed_value).
    - Booleans: 'TRUE', 'FALSE', '1', '0'
    - Dates: ISO format 'YYYY-MM-DD'
    - Numbers: integers or floats (e.g., '50', '50.0')
    - Fallback to text
    """
    # Handle string inputs
    if isinstance(value, str):
        val = value.strip()
        # Detect booleans
        if val.upper() in ("TRUE", "FALSE"):
            return "boolean", (val.upper() == "TRUE")
        if val in ("1", "0"):
            return "boolean", (val == "1")
        # Detect dates
        try:
            dt = datetime.datetime.strptime(val, "%Y-%m-%d")
            return "date", dt
        except:
            pass
        # Detect numbers
        try:
            num = float(val)
            return "number", num
        except:
            pass
        # Otherwise text
        return "text", val or None
    # Non-strings
    elif isinstance(value, bool):
        return "boolean", value
    elif isinstance(value, (int, float)):
        return "number", float(value)
    # Turn unknown types to text
    return "text", str(value) if value is not None else None


def add_question_response(session, survey_response, section_name, question_name, answer, submission_id):
    """
    Create and add a SurveyQuestionResponse for a given answer.
    """
    field_type, field_value = infer_field_type(answer)

    question_response = session.query(SurveyQuestionResponse).filter_by(
        submission_id=submission_id
    ).first()

    if question_response:
        question_response.survey_response = survey_response
        question_response.section_name = section_name
        question_response.question_name = question_name
        question_response.field_type = field_type
        question_response.value_text = field_value if field_type == "text" else None
        question_response.value_number = field_value if field_type == "number" else None
        question_response.value_boolean = field_value if field_type == "boolean" else None
        question_response.value_date = field_value if field_type == "date" else None
        question_response.value_gps = from_shape(field_value, srid=4326) if field_type == "gps" else None
        logger.info({
            "message": "Updated existing question response",
            "submission_id": submission_id,
            "question_name": question_name,
            "field_type": field_type,
            "value": field_value
        })
    else:
        question_response = SurveyQuestionResponse(
            submission_id=submission_id,
            survey_response=survey_response,
            section_name=section_name,
            question_name=question_name,
            field_type=field_type,
            value_text=field_value if field_type == "text" else None,
            value_number=field_value if field_type == "number" else None,
            value_boolean=field_value if field_type == "boolean" else None,
            value_date=field_value if field_type == "date" else None,
            value_gps=from_shape(field_value, srid=4326) if field_type == "gps" else None,   
        )
        session.add(question_response)
        logger.info({
            "message": "Added new question response",
            "submission_id": submission_id,
            "question_name": question_name,
            "field_type": field_type,
            "value": field_value
        })