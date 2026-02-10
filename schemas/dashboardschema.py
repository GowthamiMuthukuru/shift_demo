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

    clients: Union[Literal["ALL"], str, List[str]] = "ALL"
    departments: Union[Literal["ALL"], str, List[str]] = "ALL"

    years: Optional[List[int]] = None
    months: Optional[List[int]] = None

    headcounts: Union[Literal["ALL"], str, List[str]] = "ALL"
    shifts: Union[Literal["ALL"], str, List[str]] = "ALL"

    top: str = Field(default="ALL")

    sort_by: Optional[SortBy] = "total_allowance"
    sort_order: SortOrder = "default"

    @field_validator("top")
    def validate_top(cls, v: str):
        if v == "ALL":
            return v
        if not v.isdigit() or int(v) <= 0:
            raise ValueError("top must be 'ALL' or positive number string")
        return v

    @field_validator("headcounts")
    def normalize_headcounts(cls, v):
        if v == "ALL":
            return v
        if isinstance(v, str):
            v = [v]
        normalized = [str(x).strip() for x in v if str(x).strip()]
        return normalized or "ALL"

    @field_validator("shifts")
    def normalize_shifts(cls, v):
        if v == "ALL":
            return v
        if isinstance(v, str):
            v = [v]
        normalized = [str(x).strip().upper() for x in v if str(x).strip()]
        return normalized or "ALL"


class ClientAnalyticsRequest(BaseModel):
    clients: Union[Literal["ALL"], str, List[str]] = "ALL"
    departments: Union[Literal["ALL"], str, List[str]] = "ALL"
    years: List[int] = Field(default_factory=lambda: [0])
    months: List[int] = Field(default_factory=lambda: [0])
    headcounts: Union[Literal["ALL"], str, List[str]] = "ALL"
    shifts: Union[Literal["ALL"], str, List[str]] = "ALL"
    top: Union[Literal["ALL"], str, int] = "ALL"

    #  only two fields for sorting
    sort_by: Optional[SortBy] = "total_allowance"
    sort_order: SortOrder = "default"   # default means keep DB order

    class Config:
        extra = "forbid"
