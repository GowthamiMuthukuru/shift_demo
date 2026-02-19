"""
Dashboard and analytics request/response schemas.

This module defines Pydantic models used by dashboard-related APIs,
including pie charts, bar charts, graphs, client lists, and dashboard
filter payloads. These schemas standardize request validation and
response structures for analytics endpoints.
"""

from typing import List,Optional, Literal,Union,Dict,Any
from pydantic import BaseModel,Field,field_validator,ConfigDict
import re

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

    # FIX: close the Union[...] bracket and set default properly
    shifts: Optional[Union[Literal["ALL"], str]] = "ALL"

    # CHANGE REQUESTED: headcounts must be a single string or "ALL" (no list)
    headcounts: Optional[Union[Literal["ALL"], str]] = "ALL"

    sort_by: Optional[str] = None
    sort_order: Optional[str] = None

    model_config = ConfigDict(extra="forbid")

   
    @field_validator("clients", "departments", "shifts", mode="before")
    def validate_all_or_list_for_multi(cls, v):
        """
        For clients, departments, shifts:
        - Accept "ALL"
        - Accept comma-separated string (converted to list)
        - Accept list[str]
        """
        if v is None or v == "ALL":
            return "ALL"

        if isinstance(v, str):
            # For shifts we still allow a single string like "ALL" or "PST"
            # For clients/departments a CSV becomes a list
            items = [x.strip() for x in v.split(",") if x.strip()]
            # If it's a single item, keep it as a single string (for 'shifts'),
            # but for clients/departments it's fine to keep list. To keep simple,
            # return list (your downstream can handle list).
            return items or "ALL"

        if isinstance(v, list):
            cleaned = [str(x).strip() for x in v if str(x).strip()]
            if not cleaned:
                raise ValueError("List cannot be empty")
            if len(cleaned) == 1 and cleaned[0].upper() == "ALL":
                raise ValueError("'ALL' must be sent as string, not inside list")
            return cleaned

        raise ValueError("Must be 'ALL', CSV string, or list of strings")


    @field_validator("headcounts", mode="before")
    def validate_headcounts_range_str(cls, v):
        """
        Headcounts must be:
        - "ALL"
        - a single numeric string "N" (interpreted as N..N)
        - a single numeric range "N-M" (1 <= N <= M), no lists

        Examples: "1-5", "10", "25-25"
        """
        if v is None or str(v).strip().upper() == "ALL":
            return "ALL"

        if isinstance(v, list):
            raise ValueError("headcounts must be a single string like '1-5' or 'ALL' (lists are not allowed)")

        s = str(v).strip()
        # normalize unicode dashes to '-'
        s = s.replace("–", "-").replace("—", "-").replace("−", "-")

        # Accept either "N" or "N-M"
        single_pat = r"^\s*\d+\s*$"
        range_pat = r"^\s*(\d+)\s*-\s*(\d+)\s*$"

        if re.match(single_pat, s):
            # OK: "5"
            if int(s) <= 0:
                raise ValueError("headcounts value must be a positive integer")
            return s  # keep original string

        m = re.match(range_pat, s)
        if m:
            lo, hi = int(m.group(1)), int(m.group(2))
            if lo <= 0 or hi <= 0 or lo > hi:
                raise ValueError("headcounts range must be positive and min <= max")
            return f"{lo}-{hi}"

        raise ValueError("headcounts must be 'ALL', 'N', or 'N-M' (e.g., '1-5')")

class DeptDashboardFilter(BaseModel):
    """
    Department dashboard filters.
    """
    clients: Union[str, List[str]] = "ALL"
    departments: Union[str, List[str]] = "ALL"
    years: List[int] = [0]
    months: List[int] = [0]
    headcounts: Union[str, List[str]] = "ALL"
    shifts: Union[str, List[str]] = "ALL"
    top: str = "ALL"

    # ONLY department "starts with" filter is supported:
    department_starts_with: Optional[str] = None
    allowance: Optional[Union[str, List[str]]] = None
    # Sorting options for department dashboard
    sort_by: Literal["total_allowance", "department", "clients", "headcount"] = "total_allowance"
    sort_order: Literal["asc", "desc", "default"] = "default"


class DepartmentDashboardItem(BaseModel):
    clients: int
    headcount: int
    total_allowance: float


class DepartmentDashboardResponse(BaseModel):
    summary: Dict[str, Any]
    messages: List[str]
    dashboard: Dict[str, DepartmentDashboardItem]


class DepartmentAnalyticsRequest(BaseModel):
    # Exact-match filters only
    clients: Union[Literal["ALL"], str, List[str]] = "ALL"
    departments: Union[Literal["ALL"], str, List[str]] = "ALL"

    years: Optional[List[int]] = None
    months: Optional[List[int]] = None

    headcounts: Union[Literal["ALL"], str, List[str]] = "ALL"   
    shifts: Union[Literal["ALL"], str, List[str]] = "ALL"
    top: Union[Literal["ALL"], str, int] = "ALL"

    # Sorting (department view)
    # Allowed: department | clients | headcount | total_allowance
    sort_by: Optional[str] = "total_allowance"
    sort_order: str = "default"  # default | asc | desc

    # NEW: Allowance range filter (inclusive ranges)
    # e.g., "1-10000" or ["80000-1100000"]
    allowance: Optional[Union[str, List[str]]] = None

    class Config:
        extra = "forbid"