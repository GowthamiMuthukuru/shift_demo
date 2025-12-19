from pydantic import BaseModel,Field,field_validator
from typing import List,Optional, Literal,Union,Dict


class PieChartClientShift(BaseModel):
    client_full_name: str
    client_enum: str
    total_employees: int
    shift_a: int
    shift_b: int
    shift_c: int
    prime: int
    total_days: int
    total_allowances: float



class HorizontalBarResponse(BaseModel):
    Name: str
    total_no_of_days: float



class GraphResponse(BaseModel):
    Name: str
    total_allowances: float



class VerticalGraphResponse(BaseModel):
    client_full_name: str
    client_enum: str
    total_days: float
    total_allowances: float


class ClientList(BaseModel):
    clients: List[str]



class DashboardFilterRequest(BaseModel):
    
    clients: Union[
        Literal["ALL"],
        Dict[str, List[str]]
    ]

   
    top: str = Field(
        default="ALL",
        description="ALL or a numeric string like '2', '5', '10'"
    )

    start_month: Optional[str] = None
    end_month: Optional[str] = None

    selected_year: Optional[int] = None
    selected_months: Optional[List[str]] = None
    selected_quarters: Optional[List[Literal["Q1","Q2","Q3","Q4"]]] = None

    @field_validator("top")
    def validate_top(cls, v):
        if v == "ALL":
            return v
        if not v.isdigit() or int(v) <= 0:
            raise ValueError("top must be 'ALL' or a positive number as string")
        return v