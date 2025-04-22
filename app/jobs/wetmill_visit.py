import uuid
import datetime

from geoalchemy2.shape import from_shape
from shapely.geometry import Point
from sqlalchemy.exc import SQLAlchemyError
from utils.postgres import SessionLocal

from utils.models import FormVisit, Wetmill, SurveyResponse, SurveyQuestionResponse

from utils.mappings import SURVEY_TRANSFORMATIONS

ALLOWED_SURVEYS = [
    "cpqi",
    "employees",
    "financials",
    "infrastructure",
    "kpis",
    "manager_needs_assessment",
    "training_attendance_recording",
    "waste_water_management"
]


def save_form_visit(data, wetmill_id=None, user_id=None):
    session = SessionLocal()
    try:
        form = data.get("form", {})
        case = form.get("case", {})
        case_id = case.get("@case_id")

        # Lookup/create Wetmill
        if case_id:
            wetmill = session.query(Wetmill).filter_by(commcare_case_id=case_id).first()
            if not wetmill:
                wetmill = Wetmill(commcare_case_id=case_id, wet_mill_unique_id="", name="Placeholder")
                session.add(wetmill)
                session.flush()
            form_wetmill_id = wetmill.id
        else:
            form_wetmill_id = wetmill_id

        visit_date_str = form.get("date")
        visit_date = datetime.datetime.strptime(visit_date_str, "%Y-%m-%d") if visit_date_str else None
        form_name = form.get("survey_type")
        loc_str = form.get("introduction", {}).get("gps", "")
        point = extract_location_string(loc_str)

        form_visit = FormVisit(
            wetmill_id=form_wetmill_id,
            user_id=user_id,

            form_name=form_name,
            visit_date=visit_date,
            entrance_photograph=f'https://www.commcarehq.org/a/{data.get("domain")}/api/form/attachment/'
                                f'{data.get("form", {}).get("meta", {}).get("instanceID")}/'
                                f'{data.get("form", {}).get("introduction", {}).get("wetmill_entrance_photograph")}/',
            geo_location=from_shape(point, srid=4326) if point else None,
        )
        session.add(form_visit)

        surveys = form.get("surveys", {})
        completed = form.get("completed_surveys", {})
        for survey_name, content in surveys.items():
            if survey_name not in ALLOWED_SURVEYS:
                print('skipping survey: ' + survey_name)
                continue
            if not isinstance(content, dict):
                continue

            # Apply survey-specific transformation where applicable
            transform_func = SURVEY_TRANSFORMATIONS.get(survey_name)
            if transform_func:
                content = transform_func(content)

            # Determine completed date
            date_key = f"{survey_name}_date"
            completed_date = None
            date_str = completed.get(date_key)
            if date_str:
                completed_date = datetime.datetime.strptime(date_str, "%Y-%m-%d")

            general_feedback = content.get("general_feedback")

            survey_response = SurveyResponse(
                form_visit=form_visit,
                survey_type=survey_name,
                completed_date=completed_date,
                general_feedback=general_feedback
            )
            session.add(survey_response)
            print("we here")
            # Insert question responses
            for section, sec_content in content.items():
                # handle multiple answers questions on top level
                if isinstance(sec_content, list):
                    for item in sec_content:
                        add_question_response(session, survey_response, section, section, item)
                # Nested questions
                elif isinstance(sec_content, dict):
                    for q_name, ans in sec_content.items():
                        # handle multiple answers questions in nested
                        if isinstance(ans, list):
                            for item in ans:
                                add_question_response(session, survey_response, section, q_name, item)
                        else:
                            # Skip label fields
                            if q_name.endswith('_label'):
                                continue
                            add_question_response(session, survey_response, section, q_name, ans)
                # Single value questions or flags
                else:
                    # Skip top-level label or survey keys
                    if section.endswith('_label') or section.startswith('survey_'):
                        continue
                    add_question_response(session, survey_response, None, section, sec_content)
        session.commit()
        return True, None
    except SQLAlchemyError as e:
        session.rollback()
        return False, str(e)
    except Exception as e:
        session.rollback()
        return False, str(e)
    finally:
        session.close()

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
    - GPS: handled separately if needed
    - Fallback to text
    """
    # Handle string inputs
    if isinstance(value, str):
        val = value.strip()
        # Boolean detection first
        if val.upper() in ("TRUE", "FALSE"):
            return "boolean", (val.upper() == "TRUE")
        if val in ("1", "0"):
            # Ambiguous 1/0 often represent boolean
            return "boolean", (val == "1")
        # Date detection
        try:
            dt = datetime.datetime.strptime(val, "%Y-%m-%d")
            return "date", dt
        except:
            pass
        # Numeric detection (int or float)
        try:
            num = float(val)
            return "number", num
        except:
            pass
        # Otherwise fallback to text
        return "text", val or None
    # Non-string primitives
    elif isinstance(value, bool):
        return "boolean", value
    elif isinstance(value, (int, float)):
        return "number", float(value)
    # Unknown types fallback to text
    return "text", str(value) if value is not None else None


def add_question_response(session, survey_response, section_name, question_name, answer):
    """
    Create and add a SurveyQuestionResponse for a given answer.
    """
    field_type, field_value = infer_field_type(answer)
    question_response = SurveyQuestionResponse(
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