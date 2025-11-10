from pydantic import BaseModel, condecimal
from typing import Optional, Annotated
from datetime import datetime

DecimalType = Annotated[Optional[condecimal(max_digits=10, decimal_places=2)], None]

class ShiftAllowancesResponse(BaseModel):
    id : int
    file_id : int
    emp_id : str
    emp_name : str
    grade : str
    current_status : str
    department : str
    client : str
    class Config:
        from_attributes = True


class EmployeeResponse(BaseModel):
    id : int
    file_id: Optional[int]
    emp_id: Optional[str]
    emp_name: Optional[str]
    grade: Optional[str]
    current_status: Optional[str]
    department: Optional[str]
    client: Optional[str]
    project: Optional[str]
    project_code: Optional[str]
    account_manager: Optional[str]
    practice_lead: Optional[str]
    delivery_manager: Optional[str]
    duration_month: Optional[str]
    payroll_month: Optional[str]
    shift_a_days: Optional[int] = 0
    shift_b_days: Optional[int] = 0
    shift_c_days: Optional[int] = 0
    prime_days: Optional[int] = 0
    total_days: Optional[int] = 0
    billable_days: Optional[int] = 0
    non_billable_days: Optional[int] = 0
    diff: Optional[int] = 0
    final_total_days: Optional[int] = 0
    billability_status: Optional[str]
    practice_remarks: Optional[str]
    rmg_comments: Optional[str]
    amar_approval: Optional[str]
    shift_a_allowance: DecimalType = 0
    shift_b_allowance: DecimalType = 0
    shift_c_allowance: DecimalType = 0
    prime_allowance: DecimalType = 0
    total_days_allowance: DecimalType = 0
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True