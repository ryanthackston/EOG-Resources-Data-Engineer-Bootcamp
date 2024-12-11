# models.py
from pydantic import BaseModel, Field, ValidationError, field_validator
from datetime import datetime
from typing import List, Optional, Annotated

class Employee(BaseModel):
    employee_id: Optional[int] = None
    first_name: Optional[str] = "Unknown"
    last_name: Optional[str] = "Unknown"
    email: Optional[str] = "N/A"
    phone: Optional[str] = "N/A"
    hire_date: Optional[datetime] = None  # Change type to datetime and set alias
    manager_id: Optional[int] = None
    job_title: Optional[str] = "Not Specified"
    is_active_fl: Optional[int] = Field(default=1, ge=0, le=1)

    class Config:
        allow_population_by_field_name = True

    @field_validator("hire_date", mode="before")
    def parse_hire_date(cls, value):
        if value in (None, ""):  # Handle None and empty strings
            return None
        if isinstance(value, datetime):
            return value
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            raise ValueError(f"Invalid hire_date format: {value}")