"""
Dashboard and analytics request/response schemas.

This module defines Pydantic models used by dashboard-related APIs,
including pie charts, bar charts, graphs, client lists, and dashboard
filter payloads. These schemas standardize request validation and
response structures for analytics endpoints.
"""

from typing import List,Optional, Literal,Union,Dict,Any
from pydantic import BaseModel,Field,field_validator,ConfigDict


class PieChartClientShift(BaseModel):
    """
    Pie chart data structure for client-wise shift distribution.
    """
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
    """
    Horizontal bar chart response model.
    """
    Name: str
    total_no_of_days: float



class GraphResponse(BaseModel):
    """
    Line or bar graph response model.
    """
    Name: str
    total_allowances: float



class VerticalGraphResponse(BaseModel):
    """
    Vertical bar chart response model.
    """
    client_full_name: str
    client_enum: str
    total_days: float
    total_allowances: float


class ClientList(BaseModel):
    """
    Client list response model.
    """
    clients: List[str]

SortBy = Literal["client", "client_partner", "departments", "headcount", "total_allowance"]
SortOrder = Literal["default", "asc", "desc"]


class DashboardFilterRequest(BaseModel):
    """
    Pydantic v2 model.
    """
    model_config = ConfigDict(extra="forbid")

    clients: Union[Literal["ALL"], List[str]] = "ALL"
    departments: Union[Literal["ALL"], List[str]] = "ALL"

    years: Optional[List[int]] = None
    months: Optional[List[int]] = None

    headcounts: Union[Literal["ALL"], List[str]] = "ALL"
    shifts: Union[Literal["ALL"], List[str]] = "ALL"

    top: str = Field(default="ALL")

    sort_by: Optional[SortBy] = "total_allowance"
    sort_order: SortOrder = "default"

    @field_validator("clients", "departments", mode="before")
    def normalize_list_fields(cls, v):
        """
        Accept:
        - "ALL"
        - comma separated string
        - list
        """
        if v == "ALL" or v is None:
            return "ALL"

        if isinstance(v, str):
            items = [x.strip() for x in v.split(",") if x.strip()]
            return items or "ALL"

        if isinstance(v, list):
            cleaned = [str(x).strip() for x in v if str(x).strip()]
            return cleaned or "ALL"

        return "ALL"

    @field_validator("top")
    def validate_top(cls, v: str):
        if v == "ALL":
            return v
        if not v.isdigit() or int(v) <= 0:
            raise ValueError("top must be 'ALL' or positive number string")
        return v

    @field_validator("headcounts", "shifts", mode="before")
    def normalize_multi_fields(cls, v):
        if v == "ALL" or v is None:
            return "ALL"

        if isinstance(v, str):
            items = [x.strip().upper() for x in v.split(",") if x.strip()]
            return items or "ALL"

        if isinstance(v, list):
            cleaned = [str(x).strip().upper() for x in v if str(x).strip()]
            return cleaned or "ALL"

        return "ALL"



class ClientAnalyticsRequest(BaseModel):
    clients: Union[Literal["ALL"], str, List[str]] = "ALL"
    departments: Union[Literal["ALL"], str, List[str]] = "ALL"

    years: Optional[List[int]] = None
    months: Optional[List[int]] = None

    headcounts: Union[Literal["ALL"], str, List[str]] = "ALL"
    shifts: Union[Literal["ALL"], str, List[str]] = "ALL"
    top: Union[Literal["ALL"], str, int] = "ALL"

    sort_by: Optional[SortBy] = "total_allowance"
    sort_order: SortOrder = "default"

    class Config:
        extra = "forbid"


class ClientTotalAllowanceFilter(BaseModel):
    clients: Union[str, List[str]] = "ALL"
    departments: Union[str, List[str]] = "ALL"
    years: List[int] = [0]
    months: List[int] = [0]
    headcounts: Union[str, List[str]] = "ALL"
    shifts: Union[str, List[str]] = "ALL"
    top: str = "ALL"

    # Sorting options: add all supported keys
    sort_by: Literal["total_allowance", "client", "client_partner", "headcount", "departments"] = "total_allowance"
    sort_order: Literal["asc", "desc", "default"] = "default"


class SelectedPeriod(BaseModel):
    year: int
    months: List[int]


class DashboardSummary(BaseModel):
    selected_periods: List[SelectedPeriod]


class ClientStats(BaseModel):
    departments: int
    headcount: int
    total_allowance: float


class DashboardResponse(BaseModel):
    summary: DashboardSummary
    messages: List[str] = []
    dashboard: Dict[str, ClientStats]


class DashboardFilter(BaseModel):
    clients: Union[str, List[str]] = "ALL"
    departments: Union[str, List[str]] = "ALL"
    years: List[int] = [0]
    months: List[int] = [0]
    headcounts: Union[str, List[str]] = "ALL"   
    shifts: Union[str, List[str]] = "ALL"
    top: str = "ALL"
    client_starts_with: Optional[str] = None

   
    sort_by: Literal["total_allowance", "client", "client_partner", "headcount", "departments"] = "total_allowance"
    sort_order: Literal["asc", "desc", "default"] = "default"




class ClientSummaryRequest(BaseModel):
    years: Optional[List[int]] = None
    months: Optional[List[int]] = None

  
    clients: Optional[Union[Literal["ALL"], List[str]]] = "ALL"
    departments: Optional[Union[Literal["ALL"], List[str]]] = "ALL"

    emp_id: Optional[List[str]] = None
    client_partner: Optional[List[str]] = None

    shifts: Optional[Union[Literal["ALL"], List[str]]] = "ALL"
    headcounts: Optional[Union[Literal["ALL"], List[str]]] = "ALL"

    sort_by: Optional[str] = None
    sort_order: Optional[str] = None

    @field_validator("clients", "departments", "shifts", "headcounts")
    def validate_all_or_list(cls, v):
        if v is None:
            return "ALL"

        if v == "ALL":
            return v

        if isinstance(v, list):
            cleaned = [str(x).strip() for x in v if str(x).strip()]
            if not cleaned:
                raise ValueError("List cannot be empty")

            if len(cleaned) == 1 and cleaned[0].upper() == "ALL":
                raise ValueError("'ALL' must be sent as string, not inside list")

            return cleaned

        raise ValueError("Must be 'ALL' or list of strings")

