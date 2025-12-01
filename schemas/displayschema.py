from pydantic import BaseModel
from typing import Optional, List,Dict
from datetime import datetime,date


class ShiftAllowancesResponse(BaseModel):
    id: int
    emp_id: str
    emp_name: str
    department: str
    payroll_month: str
    client: str
    account_manager: str
    duration_month: str
    shift_types: List
    shift_days: Dict
 
    class Config:
        from_attributes = True

class ClientSummary(BaseModel):
    account_manager: str
    client: str
    total_employees: int
    shift_a_days: float
    shift_b_days: float
    shift_c_days: float
    prime_days: float
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
    selected_month: str
    data: List[ShiftAllowancesResponse]

    class Config:
        from_attributes = True

class ShiftUpdateRequest(BaseModel):
    shift_a: Optional[str] = None
    shift_b: Optional[str] = None
    shift_c: Optional[str] = None
    prime: Optional[str] = None
 
class ShiftDetail(BaseModel):
    shift: str
    days: float
 
class ShiftUpdateResponse(BaseModel):
    message: str
    updated_fields: List[str]
    total_days: float
    total_allowance: float
    shift_details: List[ShiftDetail]
