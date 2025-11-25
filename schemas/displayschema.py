from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime,date


class ShiftAllowancesResponse(BaseModel):
    id: int
    emp_id: str
    emp_name: str
    department: str
    month: date
    client: str
    project_code: Optional[str]
    shift_category: List[str]
 
    class Config:
        from_attributes = True

class ClientSummary(BaseModel):
    client: str
    total_employees: int
    shift_a_days: int
    shift_b_days: int
    shift_c_days: int
    prime_days: int
    total_allowances: float  

    class Config:
        from_attributes = True


class ShiftMappingResponse(BaseModel):
    shift_type: str
    days: int

    class Config:
        from_attributes = True


class EmployeeResponse(BaseModel):
    id: int
    emp_id: Optional[str]
    emp_name: Optional[str]
    grade: Optional[str]
    department: Optional[str]
    client: Optional[str]
    project: Optional[str]
    project_code: Optional[str]
    account_manager: Optional[str]
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
    total_records: int
    data: List[ShiftAllowancesResponse]

    class Config:
        from_attributes = True

class ShiftUpdateRequest(BaseModel):
    shift_a: int = 0
    shift_b: int = 0
    shift_c: int = 0
    prime: int = 0


class ShiftDetail(BaseModel):
    shift: str
    days: int


class ShiftUpdateResponse(BaseModel):
    message: str
    updated_fields: List[str]
    total_days: int
    total_allowance: float
    shift_details: List[ShiftDetail]
