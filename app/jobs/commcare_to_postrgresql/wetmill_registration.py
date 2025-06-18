from utils.models import Wetmill, User
from utils.postgres import SessionLocal
from sqlalchemy.exc import SQLAlchemyError
from geoalchemy2.shape import from_shape
from shapely.geometry import Point

from utils.mappings import map_status, EXPORTING_STATUS_MAP, MANAGER_ROLE_MAP, WET_MILL_STATUS_MAP
from simple_salesforce import Salesforce # I don't know if this is needed, but I left it in for now


def extract_location_string(location_string):
    try:
        lat, lon, *_ = map(float, location_string.split())
        return Point(lon, lat)
    except Exception:
        return None


def map_manager_role(value, role_map, role_other, survey_type, default="undefined"): # changed default to "undefined". To make sure it is actually working.
    return role_other if value == "99" else role_map.get(survey_type, {}).get(value, default)


def save_wetmill_registration(data, sf):
    session = SessionLocal()

    try:
        form = data["form"]
        meta = form.get("meta", {})
        subcase = form.get("subcase_0", {}).get("case", {})
        update = subcase.get("update", {})
        wetmill_details = form.get("wet_mill_details", {})

        location = meta.get("location", {}).get("#text", "")
        location_2 = wetmill_details.get("office_gps", None)
        point = extract_location_string(location)
        point_2 = extract_location_string(location_2)

        exporting_status = map_status(
            wetmill_details.get("exporting_status"), EXPORTING_STATUS_MAP
        )
        wet_mill_status = map_status(
            wetmill_details.get("mill_status"), WET_MILL_STATUS_MAP
        )
        manager_role = map_manager_role(
            wetmill_details.get("manager_role"),
            MANAGER_ROLE_MAP,
            wetmill_details.get("manager_role_other"),
            form.get("survey_type")
        )

        wet_mill_unique_id = form.get("wetmill_tns_id")
        if not wet_mill_unique_id:
            raise ValueError("Missing wetmill_tns_id")
        
        commcare_case_id = form.get("subcase_0", {}).get("case", {}).get("@case_id")

        url_string = (
            f'https://www.commcarehq.org/a/{data.get("domain")}/api/form/attachment/{form.get("meta", {}).get("instanceID")}/'
        )

        # Get user from Salesforce via Project_Role__c > Staff__c > sf_user_id
        try:
            project_role_id = form.get("subcase_0", {}).get("case", {}).get("index", {}).get("parent", {}).get("#text")
        
            sf_result = sf.query(
                f"SELECT Id, Staff__c FROM Project_Role__c WHERE Id = '{project_role_id}'"
            )

            sf_records = sf_result.get("records", [])
            sf_user_id = sf_records[0].get("Staff__c") if sf_records else None
            
            # Get MYPIMA user ID
            user = session.query(User).filter_by(sf_user_id=sf_user_id).first() if sf_user_id else None
            user_id = user.user_id if user else None
        except Exception:
            user_id = None

        # Upsert wetmill by wet_mill_unique_id
        
        # Changing this from wet_mill_unique_id to commcare_case_id
        
        wetmill = session.query(Wetmill).filter_by(
            commcare_case_id=commcare_case_id
        ).first()

        if wetmill:
            wetmill.name = wetmill_details.get("mill_registered_name")
            wetmill.wet_mill_unique_id=form.get("wetmill_tns_id")
            wetmill.mill_status = wet_mill_status
            wetmill.exporting_status = exporting_status
            wetmill.manager_name = wetmill_details.get("manager_name")
            wetmill.comments = wetmill_details.get("comments")
            # wetmill.wetmill_counter = int(wetmill_details.get("wetmill_counter") or 0) # Bouncing this -- Only update for BA.
            wetmill.ba_signature = f'{url_string}{wetmill_details.get("ba_signature")}'
            wetmill.manager_signature = f'{url_string}{wetmill_details.get("manager_signature")}'
            wetmill.tor_page_picture = (
                f'{url_string}{wetmill_details.get("tor_page_picture")}'
                if wetmill_details.get("tor_page_picture") else ''
            )
            wetmill.manager_role = manager_role
            wetmill.programme = form.get("programme")
            wetmill.country = form.get("country")
            wetmill.registration_date = form.get("registration_date")
            wetmill.user_id = user_id
            wetmill.date_ba_signature = form.get("date_ba_signature")
            wetmill.office_entrance_picture = (
                f'{url_string}{wetmill_details.get("office_entrance_picture")}'
                if wetmill_details.get("office_entrance_picture") else ''
            )
            wetmill.office_gps = (
                from_shape(point_2, srid=4326) if point_2 else None
            )
        else:
            wetmill = Wetmill(
                wet_mill_unique_id=wet_mill_unique_id,
                commcare_case_id=form["subcase_0"]["case"]["@case_id"],
                name=wetmill_details.get("mill_registered_name"),
                mill_status=wet_mill_status,
                exporting_status=exporting_status,
                manager_name=wetmill_details.get("manager_name"),
                comments=wetmill_details.get("comments"),
                # wetmill_counter=int(wetmill_details.get("wetmill_counter") or 0), Bouncing this -- Only update for BA.
                ba_signature=f'{url_string}{wetmill_details.get("ba_signature")}',
                manager_signature=f'{url_string}{wetmill_details.get("manager_signature")}',
                tor_page_picture=(
                    f'{url_string}{wetmill_details.get("tor_page_picture")}'
                    if wetmill_details.get("tor_page_picture") else ''
                ),
                manager_role=manager_role,
                programme=form.get("programme"),
                country=form.get("country"),
                registration_date=form.get("registration_date"),
                user_id=user_id
            )
            session.add(wetmill)

        session.commit()
        return True, None

    except SQLAlchemyError as e:
        session.rollback()
        return False, str(e)

    except Exception as e:
        print(e)
        return False, str(e)

    finally:
        session.close()
