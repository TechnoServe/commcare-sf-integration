from utils.models import Wetmill
from utils.postgres import SessionLocal
from sqlalchemy.exc import SQLAlchemyError
from geoalchemy2.shape import from_shape
from shapely.geometry import Point

from utils.mappings import map_status, EXPORTING_STATUS_MAP, MANAGER_ROLE_MAP, WET_MILL_STATUS_MAP


def extract_location_string(location_string):
    try:
        lat, lon, *_ = map(float, location_string.split())
        return Point(lon, lat)  # GeoJSON order is (lon, lat)
    except:
        return None

def save_wetmill_registration(data):
    session = SessionLocal()
    try:
        form = data["form"]
        meta = form.get("meta", {})
        subcase = form.get("subcase_0", {}).get("case", {})
        update = subcase.get("update", {})
        wetmill_details = form.get("wet_mill_details", {})

        location = meta.get("location", {}).get("#text", "")
        point = extract_location_string(location)

        exporting_status=map_status(wetmill_details.get("exporting_status"), EXPORTING_STATUS_MAP)
        wet_mill_status=map_status(wetmill_details.get("mill_status"), WET_MILL_STATUS_MAP)

        manager_role = map_manager_role(
            wetmill_details.get("manager_role"),
            MANAGER_ROLE_MAP,
            wetmill_details.get("manager_role_other")
        )

        url_string = f'https://www.commcarehq.org/a/{data.get("domain")}/api/form/attachment/{data.get("form", {}).get("meta", {}).get("instanceID")}/'
        


        wetmill = Wetmill(
            wet_mill_unique_id=form.get("wetmill_tns_id"),
            name=wetmill_details.get("mill_registered_name"),
            mill_status=wet_mill_status,
            exporting_status=exporting_status,
            manager_name=wetmill_details.get("manager_name"),
            comments=wetmill_details.get("comments"),
            wetmill_counter=int(wetmill_details.get("wetmill_counter", 0)),
            ba_signature=wetmill_details.get("ba_signature"), # TODO: Work on extracting image URL
            manager_signature=wetmill_details.get("manager_signature"),
            tor_page_picture=(f'{url_string}{wetmill_details.get("tor_page_picture")}' if wetmill_details.get("tor_page_picture") else ''),
            manager_role=manager_role,
            programe=form.get("programe"),
            country=form.get("country"),
            registration_date=form.get("registration_date"),
            # certified=False, # Remove
            # geo_location=from_shape(point, srid=4326) if point else None,
        )


        session.add(wetmill)
        session.commit()
        return True, None

    except SQLAlchemyError as e:
        session.rollback()
        return False, str(e)
    except Exception as e:
        return False, str(e)
    finally:
        session.close()


def map_manager_role(value, role_map, role_other, default="Wet Mill Manager"):
    return role_other if value == 99 else role_map.get(value, default)

