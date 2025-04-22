from sqlalchemy import Column, String, Integer, Boolean, DateTime, ForeignKey, Float, Text, Date
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship, declarative_base
from geoalchemy2 import Geometry
import uuid
import datetime

Base = declarative_base()

class Wetmill(Base):
    __tablename__ = "wetmills"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    wet_mill_unique_id = Column(String, nullable=False)
    commcare_case_id = Column(String, unique=True)
    name = Column(String)
    mill_status = Column(String)
    exporting_status = Column(String)
    programe = Column(String)
    country = Column(String)
    manager_name = Column(String, nullable=True)
    manager_role = Column(String, nullable=True)
    comments = Column(Text, nullable=True)
    wetmill_counter = Column(Integer, nullable=True)
    ba_signature = Column(String, nullable=True)
    manager_signature = Column(String, nullable=True)
    tor_page_picture = Column(String, nullable=True)
    registration_date = Column(Date, nullable=True)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)

    # Relationship to visits
    visits = relationship("FormVisit", back_populates="wetmill", cascade="all, delete-orphan")

class FormVisit(Base):
    __tablename__ = "wetmill_visits"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    wetmill_id = Column(UUID(as_uuid=True), ForeignKey("wetmills.id"), nullable=True)
    user_id = Column(UUID(as_uuid=True), nullable=True)
    form_name = Column(String, nullable=False)
    visit_date = Column(DateTime, nullable=False)
    entrance_photograph=Column(String, nullable=True)
    geo_location = Column(Geometry("POINT", srid=4326), nullable=True)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

    # Relationship to wetmill and surveys
    wetmill = relationship("Wetmill", back_populates="visits")
    surveys = relationship("SurveyResponse", back_populates="form_visit", cascade="all, delete-orphan")

class SurveyResponse(Base):
    __tablename__ = "survey_responses"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    form_visit_id = Column(UUID(as_uuid=True), ForeignKey("wetmill_visits.id"), nullable=False)
    survey_type = Column(String, nullable=False)
    completed_date = Column(DateTime, nullable=True)
    general_feedback = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

    form_visit = relationship("FormVisit", back_populates="surveys")
    question_responses = relationship(
        "SurveyQuestionResponse",
        back_populates="survey_response",
        cascade="all, delete-orphan"
    )

class SurveyQuestionResponse(Base):
    __tablename__ = "survey_question_responses"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    survey_response_id = Column(UUID(as_uuid=True), ForeignKey("survey_responses.id"), nullable=False)
    section_name = Column(String, nullable=True)
    question_name = Column(String, nullable=False)
    field_type = Column(String, nullable=False)

    value_text = Column(Text, nullable=True)
    value_number = Column(Float, nullable=True)
    value_boolean = Column(Boolean, nullable=True)
    value_date = Column(DateTime, nullable=True)
    value_gps = Column(Geometry("POINT", srid=4326), nullable=True)

    survey_response = relationship("SurveyResponse", back_populates="question_responses")
