"""
Pydantic schemas for shift allowance, employee, and client summary responses.

This module defines request and response models used across shift allowance,
employee details, summaries, dashboards, and Excel correction workflows.
All schemas are designed for FastAPI response validation and serialization.

 Shift types are DYNAMIC:
- No hardcoded keys like ANZ / PST_MST / SG / US_INDIA in schemas.
- Works with any shift key coming from config (e.g., PST_MST, EMEA_NIGHT, etc.)
"""
# pylint: disable=too-few-public-methods,missing-class-docstring

from typing import Optional, List, Dict, Union
from datetime import datetime, date
from pydantic import BaseModel, Field



class ShiftAllowancesResponse(BaseModel):
    """
    Response model for shift allowance summary per employee.
    """
    id: int
    emp_id: str
    emp_name: str
    department: str
    payroll_month: str
    client: str
    client_partner: str
    duration_month: str
    shift_types: List[str]
    shift_days: Dict[str, Union[int, float]]

    class Config:
        from_attributes = True


class ClientSummary(BaseModel):
    """
    Aggregated shift summary per client and client partner.

     Dynamic shifts:
      shift_days = {"PST_MST": 12.5, "ANZ": 5, ...}
    """
    client_partner: str
    client: str
    total_employees: int

   
    shift_days: Dict[str, float]

    total_allowances: float

    class Config:
        from_attributes = True


class ShiftMappingResponse(BaseModel):
    """
    Individual shift mapping details.
    """
    shift_type: str
    days: float
    total_allowance: Optional[float]

    class Config:
        from_attributes = True


class EmployeeResponse(BaseModel):
    """
    Detailed employee information including shift mappings.
    """
    id: int
    emp_id: Optional[str]
    emp_name: Optional[str]
    grade: Optional[str]
    department: Optional[str]
    client: Optional[str]
    project: Optional[str]
    project_code: Optional[str]
    client_partner: Optional[str]
    practice_lead: Optional[str]
    delivery_manager: Optional[str]

    duration_month: Optional[date]
    payroll_month: Optional[date]

    billability_status: Optional[str]
    practice_remarks: Optional[str]
    rmg_comments: Optional[str]

    created_at: datetime
    updated_at: datetime

    shift_mappings: List[ShiftMappingResponse] = []

    class Config:
        from_attributes = True


class PaginatedShiftResponse(BaseModel):
    """
    Paginated shift response with selected month context.
    """
    total_records: int
    selected_month: str
    data: List[ShiftAllowancesResponse]

    class Config:
        from_attributes = True

class ShiftUpdateRequest(BaseModel):
    """
    Request payload for updating shift days.

     Dynamic format:
    {
      "shifts": {
        "PST_MST": 3,
        "ANZ": 1.5,
        "SG": 2
      }
    }
    """
    shifts: Dict[str, Union[int, float]] = Field(
        default_factory=dict,
        description="Shift updates as dynamic mapping: {shift_key: days}"
    )


class ShiftDetail(BaseModel):
    """
    Shift-wise breakdown after update.
    """
    shift: str
    days: float
    total: Optional[float] = None


class ShiftUpdateResponse(BaseModel):
    """
    Response returned after successful shift update.
    """
    message: str
    updated_fields: List[str]
    total_days: float
    total_allowance: float
    shift_details: List[ShiftDetail]

class ClientAllowance(BaseModel):
    """
    Client-wise allowance total.
    """
    client: str
    total_allowances: float

    class Config:
        from_attributes = True


class ClientAllowanceList(BaseModel):
    """
    Wrapper for client allowance list responses.
    """
    data: List[ClientAllowance]


class ClientDeptResponse(BaseModel):
    """
    Mapping of client to departments.
    """
    client: str
    departments: List[str]

    class Config:
        from_attributes = True



class CorrectedRow(BaseModel):
    """
    Corrected row model for Excel correction workflows.

     Dynamic shifts:
      shift_days = {"PST_MST": 2, "ANZ": 1.5, ...}
      shift_allowances = {"PST_MST": 1400, "ANZ": 1050, ...}

    This avoids hardcoding shift keys in schema and supports future shift additions.
    """
    emp_id: str
    emp_name: Optional[str] = None
    grade: Optional[str] = None

    current_status: Optional[str] = Field(None, alias="Current Status(e)")
    department: Optional[str] = None
    client: Optional[str] = None
    project: str
    project_code: Optional[str] = None
    client_partner: Optional[str] = None
    practice_lead: Optional[str] = None
    delivery_manager: Optional[Union[int, str]] = None

    duration_month: Optional[str] = None
    payroll_month: Optional[str] = None

    #  Dynamic shifts instead of ANZ/PST_MST/SG/US_INDIA columns
    shift_days: Dict[str, Union[int, float]] = Field(default_factory=dict)
    shift_allowances: Dict[str, Union[int, float]] = Field(default_factory=dict)

    total_days: Optional[Union[int, float]] = 0
    total_days_allowances: Optional[Union[int, float]] = None

    timesheet_billable_days: Optional[Union[int, float]] = None
    timesheet_non_billable_days: Optional[Union[int, float]] = None
    diff: Optional[Union[int, float]] = None
    final_total_days: Optional[Union[int, float]] = None

    billability_status: Optional[str] = None
    practice_remarks: Optional[Union[int, str]] = None
    rmg_comments: Optional[Union[int, str]] = None
    amar_approval: Optional[Union[int, str]] = None

    am_email_attempt: Optional[Union[int, str]] = None
    am_approval_status: Optional[Union[int, str]] = None

    class Config:
        populate_by_name = True
        extra = "ignore"


class CorrectedRowsRequest(BaseModel):
    corrected_rows: List[CorrectedRow]

    class Config:
        extra = "ignore"