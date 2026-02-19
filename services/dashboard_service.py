"""Dashboard analytics services for horizontal, vertical, graph, and summary views."""

from typing import List,Any,Optional,Tuple,Set,Dict
from decimal import Decimal
from datetime import datetime,date
from sqlalchemy.orm import Session,aliased
from fastapi import HTTPException
from dateutil.relativedelta import relativedelta
from sqlalchemy import func,extract,Integer,or_,tuple_,and_,cast,desc
from models.models import ShiftAllowances, ShiftsAmount, ShiftMapping
from utils.client_enums import Company
from schemas.dashboardschema import DashboardFilterRequest,DepartmentAnalyticsRequest
import calendar
from utils.shift_config import get_all_shift_keys  
from collections import defaultdict,OrderedDict
from utils.shift_config import SHIFT_TYPES,get_shift_string

def validate_month_format(month: str):
    """Validate and parse a YYYY-MM month string into a date."""
    try:
        return datetime.strptime(month + "-01", "%Y-%m-%d").date()
    except:
        raise HTTPException(status_code=400, detail="Invalid month format. Expected YYYY-MM")


def _map_client_names(client_value: str):
    """
    Returns:
        full_name -> Company.value
        enum_name -> Company.name
    """
    for c in Company:
        if c.value == client_value or c.name == client_value:
            return c.value, c.name

    return client_value, client_value

def get_horizontal_bar_service(db: Session,
                               start_month: str | None,
                               end_month: str | None,
                               top: int | None):
    """Return horizontal bar summary of employees and shifts per client."""
    if start_month is None:
        latest = db.query(func.max(ShiftAllowances.duration_month)).scalar()
        if latest is None:
            raise HTTPException(status_code=404, detail="No records found")
        start_date = latest
    else:
        start_date = validate_month_format(start_month)

    if end_month:
        end_date = validate_month_format(end_month)
        if start_date > end_date:
            raise HTTPException(status_code=400, detail="start_month must be <= end_month")
        records = (
            db.query(ShiftAllowances)
            .filter(ShiftAllowances.duration_month >= start_date)
            .filter(ShiftAllowances.duration_month <= end_date)
            .all()
        )
    else:
        records = (
            db.query(ShiftAllowances)
            .filter(ShiftAllowances.duration_month == start_date)
            .all()
        )

    if not records:
        raise HTTPException(status_code=404, detail="No records found in the given month range")

    output = {}
    for row in records:
        client = row.client or "Unknown"
        if client not in output:
            output[client] = {
                "total_unique_employees": set(),
                "A": Decimal(0),
                "B": Decimal(0),
                "C": Decimal(0),
                "PRIME": Decimal(0)
            }
        output[client]["total_unique_employees"].add(row.emp_id)
        for mapping in row.shift_mappings:
            stype = mapping.shift_type.strip().upper()
            if stype in ("A", "B", "C", "PRIME"):
                output[client][stype] += Decimal(mapping.days or 0)

    result = []
    for client, info in output.items():
        total = len(info["total_unique_employees"])

        client_full, client_enum = _map_client_names(client)

        result.append({
            "client_full_name": client_full,
            "client_enum": client_enum,
            "total_unique_employees": total,
            "A": float(info["A"]),
            "B": float(info["B"]),
            "C": float(info["C"]),
            "PRIME": float(info["PRIME"]),
        })

    result.sort(key=lambda x: x["total_unique_employees"], reverse=True)

    if top is not None:
        if top <= 0:
            raise HTTPException(status_code=400, detail="top must be a positive integer")
        result = result[:top]

    return {"horizontal_bar": result}


def get_graph_service(
    db: Session,
    client_name: str,
    start_month: str | None = None,
    end_month: str | None = None
):
    """Return monthly allowance trend for a given client."""
    if not client_name:
        raise HTTPException(status_code=400, detail="client_name is required")

    if not client_name.replace(" ", "").isalpha():
        raise HTTPException(
            status_code=400,
            detail="Client name must contain letters only (no numbers allowed)"
        )

    client_exists = (
        db.query(ShiftAllowances)
        .filter(ShiftAllowances.client == client_name)
        .first()
    )
    if not client_exists:
        raise HTTPException(
            status_code=404,
            detail=f"Client '{client_name}' not found in database"
        )

    def validate_month(m: str):
        try:
            datetime.strptime(m, "%Y-%m")
            return True
        except:
            return False

    def generate_months(start_m: str, end_m: str):
        result = []
        cur = datetime.strptime(start_m, "%Y-%m")
        end = datetime.strptime(end_m, "%Y-%m")
        while cur <= end:
            result.append(cur)
            cur += relativedelta(months=1)
        return result

    if end_month and not start_month:
        raise HTTPException(
            status_code=400,
            detail="start_month is required when end_month is provided"
        )

    if not start_month and not end_month:
        current_year = datetime.now().year
        months = [datetime(current_year, m, 1) for m in range(1, 13)]
    else:
        if not validate_month(start_month):
            raise HTTPException(status_code=400, detail="start_month must be YYYY-MM format")

        if end_month and not validate_month(end_month):
            raise HTTPException(status_code=400, detail="end_month must be YYYY-MM format")

        if end_month and end_month < start_month:
            raise HTTPException(status_code=400, detail="end_month must be >= start_month")

        if not end_month:
            months = [datetime.strptime(start_month, "%Y-%m")]
        else:
            months = generate_months(start_month, end_month)

    years = {m.year for m in months}
    rate_map = {}

    for yr in years:
        rows = db.query(ShiftsAmount).filter(
            ShiftsAmount.payroll_year == str(yr)
        ).all()
        rate_map[yr] = {
            r.shift_type.strip().upper(): Decimal(str(r.amount)) for r in rows
        }

    monthly_allowances = {}

    for m in months:
        month_num = m.month
        year_num = m.year
        month_name = m.strftime("%b")

        records = db.query(ShiftAllowances).filter(
            ShiftAllowances.client == client_name,
            extract("year", ShiftAllowances.duration_month) == year_num,
            extract("month", ShiftAllowances.duration_month) == month_num
        ).all()

        if not records:
            monthly_allowances[month_name] = 0.0
            continue

        total_amount = Decimal(0)
        rates = rate_map[year_num]

        for row in records:
            for mapping in row.shift_mappings:
                stype = mapping.shift_type.strip().upper()
                days = Decimal(mapping.days or 0)
                rate = rates.get(stype, Decimal(0))
                total_amount += days * rate

        monthly_allowances[month_name] = float(total_amount)

    client_full, client_enum = _map_client_names(client_name)
    return {
        "client_full_name": client_full,
        "client_enum": client_enum,
        "graph": monthly_allowances
    }


def get_all_clients_service(db: Session):
    """Fetch distinct list of all clients."""
    clients = db.query(ShiftAllowances.client).distinct().all()
    client_list = [c[0] for c in clients if c[0]]
    return {"clients": client_list}


def get_piechart_shift_summary(
    db: Session,
    start_month: str | None,
    end_month: str | None,
    top: str | None
):
    """Generate pie chart summary of shift distribution across clients."""
    if top is None:
        top_int = None
    else:
        top_clean = str(top).strip().lower()
        if top_clean == "all":
            top_int = None
        else:
            if not top_clean.isdigit():
                raise HTTPException(400, "top must be a positive integer or 'all'")
            top_int = int(top_clean)
            if top_int <= 0:
                raise HTTPException(400, "top must be greater than 0")

    def validate_month(m: str):
        try:
            datetime.strptime(m, "%Y-%m")
            return True
        except:
            return False

    def generate_months(start_m: str, end_m: str):
        result = []
        cur = datetime.strptime(start_m, "%Y-%m")
        end = datetime.strptime(end_m, "%Y-%m")
        while cur <= end:
            result.append(cur.strftime("%Y-%m"))
            cur += relativedelta(months=1)
        return result

    if not start_month and not end_month:
        check_month = datetime.now().strftime("%Y-%m")
        months = None
        for _ in range(12):
            exists = (
                db.query(ShiftAllowances)
                .filter(func.to_char(ShiftAllowances.duration_month, 'YYYY-MM') == check_month)
                .first()
            )
            if exists:
                months = [check_month]
                break
            check_month = (
                datetime.strptime(check_month, "%Y-%m") - relativedelta(months=1)
            ).strftime("%Y-%m")
        if not months:
            raise HTTPException(
                status_code=404,
                detail="No shift allowance data found for the last 12 months"
            )

    elif start_month and not end_month:
        if not validate_month(start_month):
            raise HTTPException(400, "start_month must be in YYYY-MM format")
        months = [start_month]

    elif not start_month and end_month:
        raise HTTPException(400, "start_month is required if end_month is provided")

    else:
        if not validate_month(start_month) or not validate_month(end_month):
            raise HTTPException(400, "Months must be in YYYY-MM format")
        if end_month < start_month:
            raise HTTPException(400, "end_month cannot be less than start_month")
        months = generate_months(start_month, end_month)

    rate_rows = db.query(ShiftsAmount).all()
    rates = {r.shift_type.upper(): float(r.amount) for r in rate_rows}

    combined = {}
    for m in months:
        year, month = map(int, m.split("-"))
        records = (
            db.query(ShiftAllowances)
            .filter(
                extract("year", ShiftAllowances.duration_month) == year,
                extract("month", ShiftAllowances.duration_month) == month
            )
            .all()
        )
        for row in records:
            client_real = row.client or "Unknown"

            client_full, client_enum = _map_client_names(client_real)

            if client_enum not in combined:
                combined[client_enum] = {
                    "client_full_name": client_full,
                    "client_enum": client_enum,
                    "employees": set(),
                    "shift_a": 0,
                    "shift_b": 0,
                    "shift_c": 0,
                    "prime": 0,
                    "total_allowances": 0
                }

            combined[client_enum]["employees"].add(row.emp_id)

            for mapping in row.shift_mappings:
                stype = mapping.shift_type.upper()
                days = int(mapping.days or 0)

                if stype == "A":
                    combined[client_enum]["shift_a"] += days
                elif stype == "B":
                    combined[client_enum]["shift_b"] += days
                elif stype == "C":
                    combined[client_enum]["shift_c"] += days
                elif stype == "PRIME":
                    combined[client_enum]["prime"] += days

                combined[client_enum]["total_allowances"] += days * rates.get(stype, 0)

    if not combined:
        raise HTTPException(
            status_code=404,
            detail="No shift allowance data found for the selected month(s)"
        )

    result = []
    for _key, info in combined.items():
        total_days = (
            info["shift_a"]
            + info["shift_b"]
            + info["shift_c"]
            + info["prime"]
        )

        result.append({
            "client_full_name": info["client_full_name"],
            "client_enum": info["client_enum"],
            "total_employees": len(info["employees"]),
            "shift_a": info["shift_a"],
            "shift_b": info["shift_b"],
            "shift_c": info["shift_c"],
            "prime": info["prime"],
            "total_days": total_days,
            "total_allowances": info["total_allowances"]
        })

    result = sorted(result, key=lambda x: x["total_allowances"], reverse=True)

    if top_int is not None:
        result = result[:top_int]

    return result


def get_vertical_bar_service(
    db: Session,
    start_month: str | None = None,
    end_month: str | None = None,
    top: str | None = None
) -> List[dict]:
    """Return vertical bar summary of total days and allowances per client."""

    if top is None:
        top_int = None
    else:
        top_clean = str(top).strip().lower()
        if top_clean == "all":
            top_int = None
        else:
            if not top_clean.isdigit():
                raise HTTPException(400, "top must be a positive integer or 'all'")
            top_int = int(top_clean)
            if top_int <= 0:
                raise HTTPException(400, "top must be greater than 0")

    def validate_month_format(m: str):
        try:
            datetime.strptime(m, "%Y-%m")
            return True
        except ValueError:
            return False

    def generate_months_list(start_m: str, end_m: str):
        result = []
        cur = datetime.strptime(start_m, "%Y-%m")
        end = datetime.strptime(end_m, "%Y-%m")
        while cur <= end:
            result.append(cur.strftime("%Y-%m"))
            cur += relativedelta(months=1)
        return result

    if not start_month and not end_month:
        check_month = datetime.now().strftime("%Y-%m")
        months = None

        for _ in range(12):
            exists = db.query(ShiftAllowances).filter(
                func.to_char(ShiftAllowances.duration_month, 'YYYY-MM') == check_month
            ).first()

            if exists:
                months = [check_month]
                break

            check_month = (
                datetime.strptime(check_month, "%Y-%m") - relativedelta(months=1)
            ).strftime("%Y-%m")

        if not months:
            raise HTTPException(404, "No shift allowance data found for the last 12 months")

    elif start_month and not end_month:
        if not validate_month_format(start_month):
            raise HTTPException(400, "start_month must be in YYYY-MM format")
        months = [start_month]

    elif not start_month and end_month:
        raise HTTPException(400, "start_month is required if end_month is provided")

    else:
        if not validate_month_format(start_month) or not validate_month_format(end_month):
            raise HTTPException(400, "Months must be in YYYY-MM format")

        if end_month < start_month:
            raise HTTPException(400, "end_month cannot be less than start_month")

        months = generate_months_list(start_month, end_month)

    rate_rows = db.query(ShiftsAmount).all()
    rates = {r.shift_type.upper(): float(r.amount) for r in rate_rows}

    summary = {}

    for m in months:
        year, month_num = map(int, m.split("-"))

        records = db.query(ShiftAllowances).filter(
            extract("year", ShiftAllowances.duration_month) == year,
            extract("month", ShiftAllowances.duration_month) == month_num
        ).all()

        for row in records:
            client_real = row.client or "Unknown"

            client_full, client_enum = _map_client_names(client_real)
            key = client_enum

            if key not in summary:
                summary[key] = {
                    "client_full_name": client_full,
                    "client_enum": client_enum,
                    "total_days": 0,
                    "total_allowances": 0
                }

            for mapping in row.shift_mappings:
                stype = mapping.shift_type.upper()
                days = float(mapping.days or 0)

                summary[key]["total_days"] += days
                summary[key]["total_allowances"] += days * rates.get(stype, 0)

    if not summary:
        raise HTTPException(404, "No shift allowance data found for the selected month(s)")

    result = []
    for key, info in summary.items():
        result.append({
            "client_full_name": info["client_full_name"],
            "client_enum": info["client_enum"],
            "total_days": info["total_days"],
            "total_allowances": info["total_allowances"]
        })

    result.sort(key=lambda x: x["total_allowances"], reverse=True)

    if top_int is not None:
        result = result[:top_int]

    return result



def _load_shift_types() -> Set[str]:
    """
    Try to load known shift codes once at import.
    If not available, return an empty set and handle dynamically later.
    """
    try:
        if callable(get_all_shift_keys):
            return {str(s).strip().upper() for s in get_all_shift_keys()}  # type: ignore
    except Exception:
        pass
    return set()


SHIFT_TYPES: Set[str] = _load_shift_types()


def _payload_to_dict(payload: Any) -> dict:
    """
    Convert payload to dict safely.
    Works for:
      - dict input
      - Pydantic v2 models (model_dump)
      - Pydantic v1 models (dict)
      - generic mappings convertible to dict()
    Drops None values to simplify downstream .get(...).
    """
    if isinstance(payload, dict):
        return {k: v for k, v in payload.items() if v is not None}
    if hasattr(payload, "model_dump"):  # Pydantic v2
        try:
            return payload.model_dump(exclude_none=True)
        except Exception:
            pass
    if hasattr(payload, "dict"):  # Pydantic v1
        try:
            return payload.dict(exclude_none=True)
        except Exception:
            pass
    try:
        d = dict(payload)
        return {k: v for k, v in d.items() if v is not None}
    except Exception:
        return {}


def clean_str(v: Any) -> str:
    """Normalize string: None/whitespace/quotes/zero-width -> clean string."""
    if v is None:
        return ""
    s = v.strip() if isinstance(v, str) else str(v).strip()
    s = s.replace("\u200b", "").replace("\u00a0", "").strip()

   
    for _ in range(2):
        if len(s) >= 2 and s[0] == s[-1] and s[0] in ("'", '"'):
            s = s[1:-1].strip()

    if s in ("'", "''", '"', '""'):
        return ""
    if s.upper() in ("NULL", "NONE", "NAN"):
        return ""
    return s


def _is_all(value: Any) -> bool:
    """True if value represents ALL (None / 'ALL' / ['ALL'] / empty list)."""
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip().upper() == "ALL"
    if isinstance(value, list):
        if len(value) == 0:
            return True
        if len(value) == 1 and str(value[0]).strip().upper() == "ALL":
            return True
    return False


def _normalize_to_list(value: Any) -> Optional[List[str]]:
    """Normalize filter input to list[str] or None (for ALL)."""
    if _is_all(value):
        return None
    if isinstance(value, str):
        s = clean_str(value)
        return [s] if s else None
    if isinstance(value, list):
        out = [clean_str(x) for x in value if clean_str(x)]
        return out or None
    return None


def _normalize_dash(s: str) -> str:
    """Convert dash variants to standard '-'."""
    return (s or "").replace("–", "-").replace("—", "-").replace("−", "-")


def _coerce_int_list(values: Any, field_name: str, four_digit_year: bool = False) -> List[int]:
    """
    Accept list of ints/strings and return list[int]. Raise 400 on bad input.
    If four_digit_year=True and field_name == 'years', enforce YYYY (exactly 4 digits).
    """
    if values is None:
        return []
    if not isinstance(values, list):
        raise HTTPException(status_code=400, detail=f"'{field_name}' must be a list.")

    out: List[int] = []
    for v in values:
        if v is None:
            continue
        s = clean_str(v)
        if not s:
            continue

        if four_digit_year and field_name == "years":
            if not s.isdigit() or len(s) != 4:
                raise HTTPException(
                    status_code=400,
                    detail="Invalid year. Year must be in YYYY format (e.g., 2024).",
                )
            out.append(int(s))
            continue

        try:
            out.append(int(s))
        except Exception:
            raise HTTPException(status_code=400, detail=f"Invalid value in '{field_name}': {v}")

    return out


def parse_sort_order(value: Any) -> str:
    v = clean_str(value).lower()
    return v if v in ("default", "asc", "desc") else "default"


def parse_sort_by(value: Any) -> str:
    v = clean_str(value).lower()
    allowed = {"client", "client_partner", "departments", "headcount", "total_allowance"}
    return v if v in allowed else ""


def apply_sort_dict_dashboard(data: Dict[str, dict], sort_by: str, sort_order: str) -> Dict[str, dict]:
    """
    Dashboard nodes contain:
      - head_count (int)
      - departments (int)
      - total_allowance (float)

    Request uses:
      headcount -> maps to head_count
    """
    if sort_order == "default" or not sort_by:
        return data

    reverse = sort_order == "desc"

    if sort_by in ("client", "client_partner"):
        return dict(sorted(data.items(), key=lambda kv: (kv[0] or "").lower(), reverse=reverse))

    key_map = {
        "headcount": "head_count",
        "departments": "departments",
        "total_allowance": "total_allowance",
    }
    k = key_map.get(sort_by, sort_by)
    return dict(sorted(data.items(), key=lambda kv: kv[1].get(k, 0) or 0, reverse=reverse))


def parse_shifts(value: Any) -> Optional[List[str]]:
    """Returns list of shift codes or None if ALL/empty."""
    if _is_all(value):
        return None
    if isinstance(value, str):
        return [clean_str(value).upper()]
    if isinstance(value, list):
        return [clean_str(v).upper() for v in value if clean_str(v)]
    raise HTTPException(status_code=400, detail="shifts must be 'ALL', string, or list")


def validate_shifts(payload: Any) -> None:
    payload_dict = _payload_to_dict(payload)
    shifts = parse_shifts(payload_dict.get("shifts", None))
    if not shifts:
        return
    if SHIFT_TYPES:
        invalid = [s for s in shifts if s not in SHIFT_TYPES]
        if invalid:
            raise HTTPException(status_code=400, detail=f"Invalid shift type(s): {invalid}")

def validate_headcounts(payload: Any) -> None:
    """
    Validate headcounts filter.
    Accepts ONLY numeric ranges such as:
        "1-5"
        "10-20"
        "5"
    OR list of such values.
    Does NOT support sorting words (highest-to-lowest etc).
    """
    payload_dict = _payload_to_dict(payload)
    value = payload_dict.get("headcounts")

    if value is None or _is_all(value):
        return

    # Normalize into list
    if isinstance(value, str):
        value = [value]
    elif not isinstance(value, list):
        raise HTTPException(status_code=400, detail="Invalid headcounts format.")

    for item in value:
        s = _normalize_dash(clean_str(item))
        if not s:
            continue

        # Range:  N-M
        if "-" in s:
            parts = [x.strip() for x in s.split("-", 1)]
            if len(parts) != 2:
                raise HTTPException(status_code=400, detail=f"Invalid headcount range: {item}")

            lo, hi = parts
            if not lo.isdigit() or not hi.isdigit():
                raise HTTPException(status_code=400, detail=f"Invalid headcount range: {item}")

            lo, hi = int(lo), int(hi)
            if lo <= 0 or hi <= 0 or lo > hi:
                raise HTTPException(status_code=400, detail=f"Invalid headcount range: {item}")
        else:
            # Single integer
            if not s.isdigit():
                raise HTTPException(status_code=400, detail=f"Invalid headcount value: {item}")

            if int(s) <= 0:
                raise HTTPException(status_code=400, detail=f"Invalid headcount value: {item}")



def parse_headcount_ranges(value: Any) -> Optional[List[Tuple[int, int]]]:
    """
    Convert headcounts filter to list of (min,max) numeric ranges.
    ALWAYS returns exact numeric ranges:
        "1-5" -> [(1,5)]
        "5"   -> [(5,5)]
    """
    if value is None or _is_all(value):
        return None

    if isinstance(value, str):
        value = [value]
    elif not isinstance(value, list):
        raise HTTPException(status_code=400, detail="Invalid headcounts format.")

    out: List[Tuple[int, int]] = []

    for item in value:
        s = _normalize_dash(clean_str(item))
        if not s:
            continue

        if "-" in s:
            lo, hi = [int(x.strip()) for x in s.split("-", 1)]
            out.append((lo, hi))
        else:
            n = int(s)
            out.append((n, n))

    return out or None

def _previous_year_month(year: int, month: int) -> Tuple[int, int]:
    if month == 1:
        return year - 1, 12
    return year, month - 1


def _last_n_month_pairs(end_year: int, end_month: int, n: int = 12) -> List[Tuple[int, int]]:
    pairs: List[Tuple[int, int]] = []
    y, m = end_year, end_month
    for _ in range(n):
        pairs.append((y, m))
        y, m = _previous_year_month(y, m)
    return sorted(set(pairs))  

def validate_years_months_with_warnings(payload: Any, db: Session = None) -> Tuple[List[Tuple[int, int]], List[str]]:
    """
    Soft validator: returns (pairs, warnings).

    Behavior:
    - Months without years -> assume current year (drop future months; add ONLY future-month message).
    - Both years & months provided -> ALWAYS cartesian product (no zip).
    - Years only -> expand to all months (1..12, or 1..current_month for current year).
    - Neither provided ->
        1) If DB has data for the current month -> use (current_year, current_month)
        2) Else, use the latest available month within the last 12 months window up to current month (single pair)
        3) Else, use the absolute latest available month in DB (single pair)
        4) Else, use (current_year, current_month)
    - Only future-month messages are appended. All other fallbacks are SILENT (no messages).
    """
    payload_dict = _payload_to_dict(payload)
    today = date.today()
    warnings: List[str] = []

   
    years = _coerce_int_list(payload_dict.get("years", []) or [], "years", four_digit_year=True)
    months = _coerce_int_list(payload_dict.get("months", []) or [], "months")

    years = [y for y in years if y != 0]
    months = [m for m in months if m != 0]

    months = [m for m in months if 1 <= m <= 12]


    if months and not years:
        future_months = sorted({m for m in months if m > today.month})
        if future_months:
            warnings.append(f"future month(s) for {today.year}: {future_months}")
        months = [m for m in months if m <= today.month]
        if not months:
            pairs, warnings = _fallback_pairs_for_empty_selection(today, db, warnings, silent=True)
            return pairs, warnings

        pairs = sorted({(today.year, m) for m in months})
        return pairs, warnings

    if years:
       
        years = [y for y in years if y <= today.year]
        if not years:
            pairs, warnings = _fallback_pairs_for_empty_selection(today, db, warnings, silent=True)
            return pairs, warnings

        years_ordered = list(dict.fromkeys(years))  

        if not months:
            pairs2: List[Tuple[int, int]] = []
            for y in years_ordered:
                max_month = today.month if y == today.year else 12
                for m in range(1, max_month + 1):
                    pairs2.append((y, m))
            pairs2 = sorted(set(pairs2))
            return pairs2, warnings

       
        pairs3: List[Tuple[int, int]] = []
        for y in years_ordered:
            max_month = today.month if y == today.year else 12
            bad_for_year = sorted({m for m in months if m > max_month})
            if bad_for_year:
                warnings.append(f"future month(s) for {y}: {bad_for_year}")
            allowed_for_year = [m for m in months if m <= max_month]
            for m in allowed_for_year:
                pairs3.append((y, m))

        if not pairs3:
            pairs, warnings = _fallback_pairs_for_empty_selection(today, db, warnings, silent=True)
            return pairs, warnings

        pairs3 = sorted(set(pairs3))
        return pairs3, warnings

    pairs, warnings = _fallback_pairs_for_empty_selection(today, db, warnings, silent=True)
    return pairs, warnings


def _fallback_pairs_for_empty_selection(
    today: date, db: Optional[Session], warnings: List[str], silent: bool = True
) -> Tuple[List[Tuple[int, int]], List[str]]:
    """
    Fallback when no explicit valid selection remains.
    Preference:
      1) If DB has data for current month -> (today.year, today.month)
      2) Else latest available month within the last 12 months window (ending at current month)
      3) Else absolute latest available month in DB
      4) Else (today.year, today.month)

    Always returns exactly ONE (year, month) pair.
    """
   
    if not db or ShiftAllowances is None:
        return ([(today.year, today.month)], warnings)

    try:
        current_exists = (
            db.query(func.count(ShiftAllowances.id))
              .filter(extract("year", ShiftAllowances.duration_month) == today.year)
              .filter(extract("month", ShiftAllowances.duration_month) == today.month)
              .scalar()
        )
    except Exception:
        current_exists = 0

    if current_exists and int(current_exists) > 0:
        return ([(today.year, today.month)], warnings)

    last_12_pairs = _last_n_month_pairs(today.year, today.month, n=12)

    try:
        window_filter = or_(*[
            and_(
                extract("year", ShiftAllowances.duration_month) == y,
                extract("month", ShiftAllowances.duration_month) == m,
            )
            for (y, m) in last_12_pairs
        ])
        latest_in_window = (
            db.query(func.max(ShiftAllowances.duration_month))
              .filter(window_filter)
              .scalar()
        )
    except Exception:
        latest_in_window = None

    if latest_in_window:
        return ([(latest_in_window.year, latest_in_window.month)], warnings)


    try:
        absolute_latest = db.query(func.max(ShiftAllowances.duration_month)).scalar()
    except Exception:
        absolute_latest = None

    if absolute_latest:
        return ([(absolute_latest.year, absolute_latest.month)], warnings)

 
    return ([(today.year, today.month)], warnings)


def validate_years_months(payload: Any, db: Session = None) -> List[Tuple[int, int]]:
    pairs, _ = validate_years_months_with_warnings(payload, db=db)
    return pairs


def get_previous_month_allowance(db: Session, base_filters, year: int, month: int) -> float:
    if ShiftAllowances is None or ShiftMapping is None or ShiftsAmount is None:
        return 0.0

    py, pm = _previous_year_month(year, month)
    ShiftsAmountAlias = aliased(ShiftsAmount)
    allowance_expr = func.coalesce(ShiftMapping.days, 0) * func.coalesce(ShiftsAmountAlias.amount, 0)

    total = (
        db.query(func.coalesce(func.sum(allowance_expr), 0.0))
        .select_from(ShiftAllowances)
        .join(ShiftMapping, ShiftMapping.shiftallowance_id == ShiftAllowances.id)
        .outerjoin(
            ShiftsAmountAlias,
            (extract("year", ShiftAllowances.duration_month) == cast(ShiftsAmountAlias.payroll_year, Integer))
            & (func.upper(func.trim(ShiftMapping.shift_type)) == func.upper(func.trim(ShiftsAmountAlias.shift_type))),
        )
        .filter(*base_filters)
        .filter(extract("year", ShiftAllowances.duration_month) == py)
        .filter(extract("month", ShiftAllowances.duration_month) == pm)
        .scalar()
    )
    return float(total or 0.0)


def get_previous_month_unique_clients(db: Session, base_filters, year: int, month: int) -> int:
    if ShiftAllowances is None:
        return 0
    py, pm = _previous_year_month(year, month)
    count_ = (
        db.query(func.count(func.distinct(ShiftAllowances.client)))
        .filter(*base_filters)
        .filter(extract("year", ShiftAllowances.duration_month) == py)
        .filter(extract("month", ShiftAllowances.duration_month) == pm)
        .scalar()
    )
    return int(count_ or 0)


def get_previous_month_unique_departments(db: Session, base_filters, year: int, month: int) -> int:
    if ShiftAllowances is None:
        return 0
    py, pm = _previous_year_month(year, month)
    count_ = (
        db.query(func.count(func.distinct(ShiftAllowances.department)))
        .filter(*base_filters)
        .filter(extract("year", ShiftAllowances.duration_month) == py)
        .filter(extract("month", ShiftAllowances.duration_month) == pm)
        .scalar()
    )
    return int(count_ or 0)


def get_previous_month_unique_employees(db: Session, base_filters, year: int, month: int) -> int:
    if ShiftAllowances is None:
        return 0
    py, pm = _previous_year_month(year, month)
    count_ = (
        db.query(func.count(func.distinct(ShiftAllowances.emp_id)))
        .filter(*base_filters)
        .filter(extract("year", ShiftAllowances.duration_month) == py)
        .filter(extract("month", ShiftAllowances.duration_month) == pm)
        .scalar()
    )
    return int(count_ or 0)


def get_client_dashboard_summary(db: Session, payload: Any) -> Dict[str, Any]:

    if ShiftAllowances is None or ShiftMapping is None or ShiftsAmount is None:
        return {
            "summary": {"selected_periods": []},
            "messages": ["No data found for selected filters."]
        }

    validate_shifts(payload)
    validate_headcounts(payload)

    payload_dict = _payload_to_dict(payload)

 

    raw_clients = payload_dict.get("clients", "ALL")
    raw_departments = payload_dict.get("departments", "ALL")

    def normalize_comma_list(value):
        if _is_all(value):
            return None
        if isinstance(value, str):
            items = [clean_str(x) for x in value.split(",") if clean_str(x)]
            return items or None
        if isinstance(value, list):
            items = [clean_str(x) for x in value if clean_str(x)]
            return items or None
        return None

    clients_list = normalize_comma_list(raw_clients)
    depts_list = normalize_comma_list(raw_departments)
    selected_shifts = parse_shifts(payload_dict.get("shifts", None))
    headcount_ranges = parse_headcount_ranges(payload_dict.get("headcounts"))

    

    pairs, messages = validate_years_months_with_warnings(payload, db=db)
    pairs = sorted(set(pairs))

    selected_periods = []
    if pairs:
        grouped = defaultdict(list)
        for y, m in pairs:
            grouped[y].append(m)

        for y in sorted(grouped.keys()):
            selected_periods.append({
                "year": y,
                "months": sorted(set(grouped[y]))
            })

    if not pairs:
        return {
            "summary": {"selected_periods": selected_periods},
            "messages": messages
        }

 
    base_filters = []

    if clients_list:
        base_filters.append(
            func.lower(func.trim(ShiftAllowances.client)).in_(
                [c.lower().strip() for c in clients_list]
            )
        )

    if depts_list:
        base_filters.append(
            func.lower(func.trim(ShiftAllowances.department)).in_(
                [d.lower().strip() for d in depts_list]
            )
        )

    

    def fetch_rows_for_month(year, month):
        ShiftsAmountAlias = aliased(ShiftsAmount)

        q = (
            db.query(
                ShiftAllowances.emp_id,
                ShiftAllowances.client,
                ShiftAllowances.department,
                ShiftAllowances.client_partner,
                ShiftMapping.shift_type,
                ShiftMapping.days,
                func.coalesce(ShiftsAmountAlias.amount, 0),
            )
            .select_from(ShiftAllowances)
            .join(ShiftMapping, ShiftMapping.shiftallowance_id == ShiftAllowances.id)
            .outerjoin(
                ShiftsAmountAlias,
                (
                    extract("year", ShiftAllowances.duration_month)
                    == cast(ShiftsAmountAlias.payroll_year, Integer)
                )
                & (
                    func.upper(func.trim(ShiftMapping.shift_type))
                    == func.upper(func.trim(ShiftsAmountAlias.shift_type))
                ),
            )
            .filter(*base_filters)
            .filter(extract("year", ShiftAllowances.duration_month) == year)
            .filter(extract("month", ShiftAllowances.duration_month) == month)
        )

        if selected_shifts:
            q = q.filter(
                func.upper(func.trim(ShiftMapping.shift_type)).in_(selected_shifts)
            )

        return q.all()


    latest_y, latest_m = pairs[-1]
    rows = fetch_rows_for_month(latest_y, latest_m)

    if not rows:
        return {
            "summary": {"selected_periods": selected_periods},
            "messages": messages
        }



    def apply_headcount_filter(rows):
        if not headcount_ranges:
            return rows

        grouping_map = defaultdict(set)

        for emp, client, dept, cp, shift, days, amt in rows:
            if not emp:
                continue

            key = (client, dept) if depts_list else client
            grouping_map[key].add(emp)

        allowed_groups = set()

        for key, emp_set in grouping_map.items():
            hc = len(emp_set)
            for lo, hi in headcount_ranges:
                if lo <= hc <= hi:
                    allowed_groups.add(key)
                    break

        filtered = []
        for row in rows:
            emp, client, dept, cp, shift, days, amt = row
            key = (client, dept) if depts_list else client
            if key in allowed_groups:
                filtered.append(row)

        return filtered

    rows = apply_headcount_filter(rows)

    if not rows:
        return {
            "summary": {"selected_periods": selected_periods},
            "messages": messages
        }

  
 

    total_allowance = 0.0
    clients_set = set()
    depts_set = set()
    headcount_set = set()

    for emp, client, dept, cp, shift, days, amt in rows:
        total_allowance += float(days or 0) * float(amt or 0)

        if emp:
            headcount_set.add(emp)
        if client:
            clients_set.add(client)
        if dept:
            depts_set.add(dept)

  
    prev_y, prev_m = _previous_year_month(latest_y, latest_m)
    prev_rows = fetch_rows_for_month(prev_y, prev_m)
    prev_rows = apply_headcount_filter(prev_rows)

    previous_total = 0.0
    previous_clients_set = set()
    previous_depts_set = set()
    previous_headcount_set = set()

    for emp, client, dept, cp, shift, days, amt in prev_rows:
        previous_total += float(days or 0) * float(amt or 0)

        if emp:
            previous_headcount_set.add(emp)
        if client:
            previous_clients_set.add(client)
        if dept:
            previous_depts_set.add(dept)

    # Previous of previous (allowance trend only)
    prev_prev_total = get_previous_month_allowance(
        db, base_filters, prev_y, prev_m
    )

    

    def calc_change(curr, prev):
        if not prev:
            return "N/A"
        pct = round(((curr - prev) / prev) * 100, 2)
        if pct > 0:
            return f"{pct}% increase"
        if pct < 0:
            return f"{abs(pct)}% decrease"
        return "0% no change"

 

    summary = {
        "selected_periods": selected_periods,
        "total_clients": len(clients_set),
        "total_clients_last_month": calc_change(
            len(clients_set), len(previous_clients_set)
        ),
        "total_departments": len(depts_set),
        "total_departments_last_month": calc_change(
            len(depts_set), len(previous_depts_set)
        ),
        "head_count": len(headcount_set),
        "head_count_last_month": calc_change(
            len(headcount_set), len(previous_headcount_set)
        ),
        "total_allowance": round(total_allowance, 2),
        "total_allowance_last_month": calc_change(
            round(total_allowance, 2), previous_total
        ),
        "previous_month_allowance": previous_total,
        "previous_month_allowance_last_month": calc_change(
            previous_total, prev_prev_total
        ),
    }

    return {
        "summary": summary,
        "messages": messages if messages else []
    }


try:
    from utils.shift_config import get_all_shift_keys
    SHIFT_KEYS: List[str] = [str(k).strip().upper() for k in get_all_shift_keys()]
except Exception:
    SHIFT_KEYS = []
SHIFT_KEY_SET: Set[str] = set(SHIFT_KEYS)


def clean_str(value: Any) -> str:
    """Normalize strings (handles None, whitespace, zero-width & nbsp, and quote-only)."""
    if value is None:
        return ""
    s = value.strip() if isinstance(value, str) else str(value).strip()
    s = s.replace("\u200b", "").replace("\u00a0", "").strip()

    # Strip matching quotes at both ends, at most twice
    for _ in range(2):
        if len(s) >= 2 and s[0] == s[-1] and s[0] in ("'", '"'):
            s = s[1:-1].strip()

    if s in ("'", "''", '"', '""'):
        return ""
    if s.upper() in ("NULL", "NONE", "NAN"):
        return ""
    return s


def _is_all(value: Any) -> bool:
    """True if value represents ALL."""
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip().upper() == "ALL"
    if isinstance(value, list):
        if len(value) == 0:
            return True
        if len(value) == 1 and str(value[0]).strip().upper() == "ALL":
            return True
    return False


def _normalize_dash(s: str) -> str:
    """Convert dash variants to standard '-'."""
    return (s or "").replace("–", "-").replace("—", "-").replace("−", "-")


def _as_list(x: Any) -> List[Any]:
    """Return x as a list (None -> [], scalar -> [scalar], list -> list)."""
    if x is None:
        return []
    return x if isinstance(x, list) else [x]


def _payload_to_plain_dict(payload: Any) -> dict:
    """Ensure payload is a plain dict (handles Pydantic v1/v2 and None)."""
    if payload is None:
        return {}
    if isinstance(payload, dict):
        return {k: v for k, v in payload.items() if v is not None}
    if hasattr(payload, "model_dump"):  # Pydantic v2
        try:
            return payload.model_dump(exclude_none=True)
        except Exception:
            pass
    if hasattr(payload, "dict"):  # Pydantic v1
        try:
            return payload.dict(exclude_none=True)
        except Exception:
            pass
    try:
        return dict(payload)
    except Exception:
        return {}


def _coerce_int_list(values: Any, field_name: str, four_digit_year: bool = False) -> List[int]:
    """
    Accept list of ints/strings and return list[int]. Raise 400 on bad input.
    If four_digit_year=True and field_name == 'years', enforce YYYY (exactly 4 digits).
    """
    if values is None:
        return []
    if not isinstance(values, list):
        raise HTTPException(status_code=400, detail=f"'{field_name}' must be a list.")

    out: List[int] = []
    for v in values:
        if v is None:
            continue

        s = clean_str(v)
        if not s:
            continue

        if four_digit_year and field_name == "years":
            if not s.isdigit() or len(s) != 4:
                raise HTTPException(
                    status_code=400,
                    detail="Invalid year. Year must be in YYYY format (e.g., 2024).",
                )
            y = int(s)
            if y <= 0:
                raise HTTPException(status_code=400, detail="Invalid year. Year must be a positive 4-digit number.")
            out.append(y)
            continue

        try:
            out.append(int(s))
        except Exception:
            raise HTTPException(status_code=400, detail=f"Invalid value in '{field_name}': {v}")

    return out


def parse_clients(value: Any) -> Optional[List[str]]:
    """ALL -> None; string -> [string]; list -> list."""
    if _is_all(value):
        return None
    if isinstance(value, str):
        v = clean_str(value)
        return [v] if v else None
    if isinstance(value, list):
        out = [clean_str(x) for x in value if clean_str(x)]
        return out or None
    raise HTTPException(400, "clients must be 'ALL', string, or list.")


def parse_departments(value: Any) -> Optional[List[str]]:
    """ALL -> None; string -> [string]; list -> list."""
    if _is_all(value):
        return None
    if isinstance(value, str):
        v = clean_str(value)
        return [v] if v else None
    if isinstance(value, list):
        out = [clean_str(x) for x in value if clean_str(x)]
        return out or None
    raise HTTPException(400, "departments must be 'ALL', string, or list.")


def parse_shifts(value: Any) -> Optional[Set[str]]:
    """ALL -> None; else validate shift keys (only if keys are known)."""
    if _is_all(value):
        return None
    if isinstance(value, str):
        v = clean_str(value).upper()
        if SHIFT_KEY_SET and v not in SHIFT_KEY_SET:
            raise HTTPException(400, f"Invalid shift type: {v}")
        return {v}
    if isinstance(value, list):
        out: Set[str] = set()
        for x in value:
            v = clean_str(x).upper()
            if not v:
                continue
            if SHIFT_KEY_SET and v not in SHIFT_KEY_SET:
                raise HTTPException(400, f"Invalid shift type: {v}")
            out.add(v)
        return out or None
    raise HTTPException(400, "shifts must be 'ALL', string, or list.")


def parse_top(value: Any) -> Optional[int]:
    """ALL -> None; numeric -> int."""
    if _is_all(value) or value is None:
        return None
    if isinstance(value, int):
        return value
    s = clean_str(value)
    if s.isdigit():
        return int(s)
    raise HTTPException(400, "top must be 'ALL' or a number.")


def parse_employee_limit(value: Any) -> Optional[int]:
    """
    Headcounts behavior:
    - "ALL" -> None
    - "10" -> 10
    - "1-10" -> 10
    - ["1-10","11-50"] -> 50 (max upper bound)
    Meaning: show up to N employees (overall top N for selected client/period).
    """
    if _is_all(value) or value is None:
        return None

    items = value if isinstance(value, list) else [value]
    limits: List[int] = []

    for item in items:
        s = _normalize_dash(clean_str(item)).upper()
        if not s or s == "ALL":
            continue

        if "-" in s:
            lo, hi = [x.strip() for x in s.split("-", 1)]
            if not lo.isdigit() or not hi.isdigit():
                raise HTTPException(400, "Invalid headcount range.")
            lo_i, hi_i = int(lo), int(hi)
            if lo_i <= 0 or lo_i > hi_i:
                raise HTTPException(400, "Invalid headcount range.")
            limits.append(hi_i)
        else:
            if not s.isdigit() or int(s) <= 0:
                raise HTTPException(400, "Invalid headcount value.")
            limits.append(int(s))

    return max(limits) if limits else None


def parse_sort_order(value: Any) -> str:
    v = clean_str(value).lower()
    return v if v in ("default", "asc", "desc") else "default"


def parse_sort_by(value: Any) -> str:
    v = clean_str(value).lower()
    allowed = {"client", "client_partner", "departments", "headcount", "total_allowance"}
    return v if v in allowed else ""


def apply_sort_dict(data: Dict[str, dict], sort_by: str, sort_order: str) -> Dict[str, dict]:
    """
    sort_order:
      - default => do not sort (keep natural/insertion order)
      - asc/desc
    sort_by:
      - client / client_partner => alphabetical by key
      - departments/headcount/total_allowance => numeric by value
    """
    if sort_order == "default" or not sort_by:
        return data

    reverse = (sort_order == "desc")

    if sort_by in ("client", "client_partner"):
        return dict(sorted(data.items(), key=lambda kv: (kv[0] or "").lower(), reverse=reverse))

    return dict(sorted(data.items(), key=lambda kv: kv[1].get(sort_by, 0) or 0, reverse=reverse))


def top_n_dict(data: Dict[str, dict], n: Optional[int]) -> Dict[str, dict]:
    if not n:
        return data
    return dict(list(data.items())[:n])



def _find_recent_month_with_data(
    db: Session,
    start_year: int,
    start_month: int,
    lookback: int = 12,
) -> Optional[Tuple[int, int]]:
    """
    Starting from (start_year, start_month), walk backward up to `lookback` months (inclusive),
    and return the first (year, month) that exists in DB. Return None if nothing found.
    """
    y, m = start_year, start_month
    for _ in range(lookback):
        exists = db.query(ShiftAllowances.id).filter(
            extract("year", ShiftAllowances.duration_month) == y,
            extract("month", ShiftAllowances.duration_month) == m,
        ).first()
        if exists:
            return (y, m)

        if m == 1:
            y, m = y - 1, 12
        else:
            m -= 1
    return None

def validate_years_months(payload: dict, db: Session) -> Tuple[List[Tuple[int, int]], Optional[str]]:
    """
    Behavior:
    - If no year/month:
        - If current month has data -> use current month only
        - Else -> pick the latest single month within the last 12 months
        - If none found in last 12 months -> default to current month (may yield no rows) with a message
    - If only month provided -> assume current year
    - If year & month provided -> Cartesian product
    - If only year provided -> all 12 months of that year
    - Ignore future periods
    """

    today = date.today()
    cy, cm = today.year, today.month

    payload = _payload_to_plain_dict(payload)

    years_raw = payload.get("years")
    months_raw = payload.get("months")

    if _is_all(years_raw):
        years_raw = None
    if _is_all(months_raw):
        months_raw = None

    years = _coerce_int_list(_as_list(years_raw), "years", four_digit_year=True) if years_raw else []
    months = _coerce_int_list(_as_list(months_raw), "months") if months_raw else []

    pairs: List[Tuple[int, int]] = []
    message: Optional[str] = None

   
    if not years and not months:

        exists_current = db.query(ShiftAllowances.id).filter(
            extract("year", ShiftAllowances.duration_month) == cy,
            extract("month", ShiftAllowances.duration_month) == cm,
        ).first()

        if exists_current:
            pairs = [(cy, cm)]
        else:
            recent = _find_recent_month_with_data(db, cy, cm, lookback=12)
            if recent:
                ry, rm = recent
                pairs = [(ry, rm)]
                message = (
                    f"No data for current month. Showing latest available month within last 12 months: {ry}-{rm:02d}."
                )
            else:
                pairs = [(cy, cm)]
                message = "No data found in the last 12 months. Defaulting to current month."

    elif years and months:
        for y in years:
            for m in months:
                pairs.append((y, m))

   
    elif years:
        for y in years:
            for m in range(1, 13):
                pairs.append((y, m))

   
    elif months:
        for m in months:
            pairs.append((cy, m))

    valid_pairs: List[Tuple[int, int]] = []
    future_pairs: List[str] = []

    for y, m in pairs:
        if y > cy or (y == cy and m > cm):
            future_pairs.append(f"{y}-{m:02d}")
        else:
            valid_pairs.append((y, m))

    if future_pairs:
        msg = f"Future periods ignored: {', '.join(sorted(set(future_pairs)))}"
        message = f"{message} {msg}".strip() if message else msg

    if (years or months) and not valid_pairs:
        return [], message

    return sorted(set(valid_pairs)), message


def month_back_list(y: int, m: int, n: int = 12) -> List[Tuple[int, int]]:
    """Return [(y,m-1), (y,m-2), ...] up to n months back (newest->older)."""
    out: List[Tuple[int, int]] = []
    cy, cm = y, m
    for _ in range(n):
        if cm == 1:
            cy, cm = cy - 1, 12
        else:
            cm -= 1
        out.append((cy, cm))
    return out


def fmt_change(curr: float, prev: float) -> str:
    """
    Return EXACTLY one of:
      - "23% increase"
      - "23% decrease"
      - "no change"
    """
    if prev == 0:
        if curr == 0:
            return "no change"
        return "100% increase"

    pct = ((curr - prev) / prev) * 100.0
    if abs(pct) < 0.005:
        return "no change"
    direction = "increase" if pct > 0 else "decrease"
    return f"{abs(pct):.0f}% {direction}"


def _query_allowance_rows(
    db: Session,
    ym_pairs: List[Tuple[int, int]],
    base_filters_extra: List[Any],
    shifts_filter: Optional[Set[str]],
):
    """
    Query rows needed to compute headcount + allowance for client per period list.
    Returns tuples: (client, yy, mm, emp_id, shift_type, days, amount)
    """
    ym_filters = [
        and_(
            extract("year", ShiftAllowances.duration_month) == y,
            extract("month", ShiftAllowances.duration_month) == m,
        )
        for y, m in ym_pairs
    ]

    ShiftsAmountAlias = aliased(ShiftsAmount)

    q = (
        db.query(
            ShiftAllowances.client,
            extract("year", ShiftAllowances.duration_month).label("yy"),
            extract("month", ShiftAllowances.duration_month).label("mm"),
            ShiftAllowances.emp_id,
            ShiftMapping.shift_type,
            ShiftMapping.days,
            func.coalesce(ShiftsAmountAlias.amount, 0),
        )
        .select_from(ShiftAllowances)
        .join(ShiftMapping, ShiftMapping.shiftallowance_id == ShiftAllowances.id)
        .outerjoin(
            ShiftsAmountAlias,
            and_(
                cast(ShiftsAmountAlias.payroll_year, Integer) == extract("year", ShiftAllowances.duration_month),
                func.upper(func.trim(ShiftMapping.shift_type)) == func.upper(func.trim(ShiftsAmountAlias.shift_type)),
            ),
        )
        .filter(or_(*ym_filters))
        .filter(*base_filters_extra)
    )

    if shifts_filter:
        q = q.filter(func.upper(func.trim(ShiftMapping.shift_type)).in_(list(shifts_filter)))

    return q.all()


def _aggregate_client_period(rows) -> Dict[str, Dict[Tuple[int, int], Dict[str, Any]]]:
    """Aggregate: client -> (y,m) -> {emp_set, allow}."""
    out: Dict[str, Dict[Tuple[int, int], Dict[str, Any]]] = {}

    for client, yy, mm, emp_id, stype, days, amt in rows:
        cname = clean_str(client) or "UNKNOWN"
        y, m = int(yy), int(mm)

        eid = clean_str(emp_id)
        st = clean_str(stype).upper()
        if SHIFT_KEY_SET and st not in SHIFT_KEY_SET:
            continue

        allowance = float(days or 0) * float(amt or 0)

        cdict = out.setdefault(cname, {})
        node = cdict.setdefault((y, m), {"emp_set": set(), "allow": 0.0})

        if eid:
            node["emp_set"].add(eid)
        node["allow"] += allowance

    return out


def _pick_nearest_baseline(
    by_client_period: Dict[str, Dict[Tuple[int, int], Dict[str, Any]]],
    candidates: List[Tuple[int, int]],
) -> Dict[str, Dict[str, Any]]:
    """Pick nearest available month in candidates for each client."""
    baselines: Dict[str, Dict[str, Any]] = {}
    for cname, period_dict in by_client_period.items():
        for y, m in candidates:
            node = period_dict.get((y, m))
            if node and (node["allow"] != 0.0 or len(node["emp_set"]) > 0):
                baselines[cname] = {
                    "headcount": len(node["emp_set"]),
                    "allow": float(node["allow"] or 0.0),
                }
                break
    return baselines


def _group_selected_periods(pairs: List[Tuple[int, int]]) -> List[Dict[str, Any]]:
    by_year: Dict[int, Set[int]] = {}
    for y, m in pairs:
        by_year.setdefault(y, set()).add(m)
    return [{"year": y, "months": sorted(ms)} for y, ms in sorted(by_year.items())]


def _extract_eid(emp_id: Any, emp_name: Any, client: str, dept: str, cp: str) -> str:
    """
    Unique employee key with fallback when emp_id is blank.
    Ensures headcount is counted reliably.
    """
    e = clean_str(emp_id)
    if e:
        return e
    return f"{clean_str(emp_name)}|{client}|{dept}|{cp}"


def _build_shift_details_object_with_currency(db: Session, pairs: List[Tuple[int, int]], currency: str = "INR") -> Dict[str, Dict[str, Any]]:
    """
    Build { SHIFT_KEY: {label, timing, amount, currency} }:
      - label/timing from get_shift_string()
      - amount from ShiftsAmount:
          * prefer the latest year present in selected periods (pairs)
          * fallback to latest available year if selected years have no rows
    """
    keys = [str(k).strip().upper() for k in get_all_shift_keys()]
    years_in_scope = sorted({y for y, _ in (pairs or [])})

    q = db.query(ShiftsAmount)
    if years_in_scope:
        q = q.filter(cast(ShiftsAmount.payroll_year, Integer).in_(years_in_scope))
    amount_rows = q.all()

    # SHIFT -> { year -> amount }
    amounts_by_shift: Dict[str, Dict[int, float]] = {}
    for r in amount_rows:
        skey = (str(r.shift_type) or "").strip().upper()
        try:
            yr = int(r.payroll_year) if r.payroll_year is not None else None
        except Exception:
            yr = None
        amt = float(r.amount or 0.0)
        if skey and yr:
            amounts_by_shift.setdefault(skey, {})[yr] = amt

    # Fallback for shifts with no rates in selected years
    if years_in_scope:
        missing = [k for k in keys if (k not in amounts_by_shift or not amounts_by_shift[k])]
        if missing:
            add_rows = db.query(ShiftsAmount).filter(
                func.upper(func.trim(ShiftsAmount.shift_type)).in_(missing)
            ).all()
            for r in add_rows:
                skey = (str(r.shift_type) or "").strip().upper()
                try:
                    yr = int(r.payroll_year) if r.payroll_year is not None else None
                except Exception:
                    yr = None
                amt = float(r.amount or 0.0)
                if skey and yr:
                    amounts_by_shift.setdefault(skey, {}).setdefault(yr, amt)

    out: Dict[str, Dict[str, Any]] = {}
    for key in keys:
        # label / timing
        s = (get_shift_string(key) or "").strip()
        label, timing = key, ""
        if s:
            lines = s.splitlines()
            if len(lines) >= 1:
                label = (lines[0] or "").strip() or key
            if len(lines) >= 2:
                timing = (lines[1] or "").strip()
                if timing.startswith("(") and timing.endswith(")"):
                    timing = timing[1:-1].strip()

        # choose amount; prefer latest in-scope, else latest overall
        yr_map = amounts_by_shift.get(key, {})
        chosen = 0.0
        if years_in_scope:
            for y in sorted(years_in_scope, reverse=True):
                if y in yr_map:
                    chosen = float(yr_map[y])
                    break
        if chosen == 0.0 and yr_map:
            chosen = float(yr_map[max(yr_map.keys())])

        out[key] = {"label": label, "timing": timing, "amount": chosen, "currency": currency}

    return out


def _alpha(v: Any) -> str:
    """String-normalized lowercase for stable alpha sorting."""
    return (str(v or "")).lower()

def _num(v: Any) -> float:
    """Safe numeric conversion with fallback."""
    try:
        return float(v if v is not None else 0)
    except Exception:
        return 0.0

def _parse_shift_key(sb: str) -> Optional[str]:
    """Extract shift key from 'shift:<KEY>' pattern; returns UPPER KEY or None."""
    if not sb:
        return None
    s = str(sb).strip()
    if s.lower().startswith("shift:"):
        return s.split(":", 1)[1].strip().upper()
    return None

def _effective_order(for_field_kind: str, requested: str) -> str:
    """
    Resolve 'default' order based on field kind:
      - numeric -> desc
      - alpha   -> asc
    """
    so = (requested or "default").strip().lower()
    if so in ("asc", "desc"):
        return so
    return "desc" if for_field_kind == "num" else "asc"


def _alpha(v: Any) -> str:
    """String-normalized lowercase for stable alpha sorting."""
    return (str(v or "")).lower()

def _num(v: Any) -> float:
    """Safe numeric conversion with fallback."""
    try:
        return float(v if v is not None else 0)
    except Exception:
        return 0.0

def _parse_shift_key(sb: str, shift_keys: Set[str]) -> Optional[str]:
    """
    Extract a shift key from either 'shift:<KEY>' or bare '<KEY>'.
    Returns UPPER shift key if recognized and present in shift_keys; otherwise None.
    """
    if not sb:
        return None
    s = str(sb).strip()
    # 'shift:US_INDIA' style
    if s.lower().startswith("shift:"):
        key = s.split(":", 1)[1].strip().upper()
        return key if key in shift_keys else None
    # bare 'US_INDIA' style
    u = s.upper()
    return u if u in shift_keys else None

def _effective_order(for_field_kind: str, requested: str) -> str:
    """
    Resolve 'default' order based on field kind:
      - numeric -> desc
      - alpha   -> asc
    """
    so = (requested or "default").strip().lower()
    if so in ("asc", "desc"):
        return so
    return "desc" if for_field_kind == "num" else "asc"


def client_analytics_service(db: Session, payload: dict) -> Dict[str, Any]:
    """
    Client-anchored analytics with deep nesting and shift meta.

    Features:
    - Filters: clients, departments, shifts, years/months.
    - Aggregation: client → department → partner → employees (+ shift summaries).
    - Sorting (alpha/numeric-aware) at all levels with tie-breakers:
        * Clients: client | departments | headcount | total_allowance | shift:<KEY>/<KEY>
        * Departments: department | headcount | client_partner_count | total_allowance | shift:<KEY>/<KEY>
        * Partners: client_partner | headcount | total_allowance | shift:<KEY>/<KEY>
        * Employees: emp_name | total_allowance | shift:<KEY>/<KEY>
          (If 'headcount' is requested for employees, it falls back to total_allowance)
    - Default **department-level** sorting is `total_allowance` (numeric desc).
    """
    payload = _payload_to_plain_dict(payload)

    # ---- Parse filters / controls
    clients_filter    = parse_clients(payload.get("clients", "ALL"))
    depts_filter      = parse_departments(payload.get("departments", "ALL"))
    shifts_filter     = parse_shifts(payload.get("shifts", "ALL"))              # Set[str] or None
    top_n             = parse_top(payload.get("top", "ALL"))                    # keep your existing 'top' behavior
    employee_cap      = parse_employee_limit(payload.get("headcounts", "ALL"))  # optional employee list cap

    # Backward-compatible (client-level) sort spec (still supported):
    sort_by           = parse_sort_by(payload.get("sort_by", ""))               # client | headcount | total_allowance | departments
    sort_order        = parse_sort_order(payload.get("sort_order", "default"))  # default|asc|desc

    # New granular sort specs (optional): (set department default to total_allowance)
    clients_sort_by_raw   = payload.get("sort_clients_by",  payload.get("sort_by", "total_allowance"))
    clients_sort_order    = payload.get("sort_clients_order", payload.get("sort_order", "default"))

    # IMPORTANT: default departments sort to total_allowance (desc by default)
    depts_sort_by_raw     = payload.get("sort_departments_by", "total_allowance")
    depts_sort_order      = payload.get("sort_departments_order", "default")

    partners_sort_by_raw  = payload.get("sort_partners_by", "client_partner")
    partners_sort_order   = payload.get("sort_partners_order", "default")

    # Employees sort (supports emp_name | total_allowance | shift:<KEY> | <KEY> | headcount[fallback])
    employees_sort_by_raw = payload.get("sort_employees_by", "total_allowance")
    employees_sort_order  = payload.get("sort_employees_order", "default")

    # ---- Resolve periods
    pairs, period_message = validate_years_months(payload, db=db)
    periods = [f"{y:04d}-{m:02d}" for y, m in pairs]

    if not pairs:
        return {
            "periods": [],
            "message": period_message,
            "summary": {
                "total_clients": 0,
                "departments": 0,
                "headcount": 0,
                "total_allowance": 0.0,
            },
            "clients": {},
            "shift_details": _build_shift_details_object_with_currency(db, []),
        }

  
    base_filters_extra: List[Any] = []
    if clients_filter:
        base_filters_extra.append(
            func.lower(func.trim(ShiftAllowances.client)).in_([c.lower() for c in clients_filter])
        )
    if depts_filter:
        base_filters_extra.append(
            func.lower(func.trim(ShiftAllowances.department)).in_([d.lower() for d in depts_filter])
        )

    ym_filters = [
        and_(
            extract("year",  ShiftAllowances.duration_month) == y,
            extract("month", ShiftAllowances.duration_month) == m,
        )
        for y, m in pairs
    ]

   
    ShiftsAmountAlias = aliased(ShiftsAmount)
    rows_q = (
        db.query(
            ShiftAllowances.emp_id,          # 0
            ShiftAllowances.emp_name,        # 1
            ShiftAllowances.client,          # 2
            ShiftAllowances.department,      # 3
            ShiftAllowances.client_partner,  # 4
            ShiftMapping.shift_type,         # 5
            ShiftMapping.days,               # 6
            func.coalesce(ShiftsAmountAlias.amount, 0),  # 7
        )
        .select_from(ShiftAllowances)
        .join(ShiftMapping, ShiftMapping.shiftallowance_id == ShiftAllowances.id)
        .outerjoin(
            ShiftsAmountAlias,
            and_(
                cast(ShiftsAmountAlias.payroll_year, Integer) == extract("year", ShiftAllowances.duration_month),
                func.upper(func.trim(ShiftMapping.shift_type)) == func.upper(func.trim(ShiftsAmountAlias.shift_type)),
            ),
        )
        .filter(or_(*ym_filters))
        .filter(*base_filters_extra)
    )
    if shifts_filter:
        rows_q = rows_q.filter(func.upper(func.trim(ShiftMapping.shift_type)).in_(list(shifts_filter)))

    rows = rows_q.all()
    if not rows:
        return {
            "periods": periods,
            "message": period_message,
            "summary": {
                "total_clients": 0,
                "departments": 0,
                "headcount": 0,
                "total_allowance": 0.0,
            },
            "clients": {},
            "shift_details": _build_shift_details_object_with_currency(db, pairs),
        }

    SHIFT_KEYS = [str(k).strip().upper() for k in get_all_shift_keys()]
    SHIFT_KEY_SET = set(SHIFT_KEYS)


    clients_map: Dict[str, Dict[str, Any]] = {}   
    global_emp_set: Set[str] = set()
    global_dept_set: Set[str] = set()
    global_total = 0.0

    for emp_id, emp_name, client, dept, cp, stype, days, rate in rows:
        client_name = clean_str(client) or "Unknown"
        dept_name   = clean_str(dept) or "Unknown"
        partner     = clean_str(cp) or "Unknown"
        st          = (clean_str(stype) or "").upper()
        if st and SHIFT_KEY_SET and st not in SHIFT_KEY_SET:
            continue

        allowance = float(days or 0) * float(rate or 0)
        eid = _extract_eid(emp_id, emp_name, client_name, dept_name, partner)

        # init client node
        cnode = clients_map.setdefault(
            client_name,
            {
                "_emp_set": set(),
                "_dept_set": set(),
                "total_allowance": 0.0,
                "shifts_summary": {k: 0.0 for k in SHIFT_KEYS},
                "departments_breakdown": {}
            }
        )
        cnode["_emp_set"].add(eid)
        cnode["_dept_set"].add(dept_name)
        cnode["total_allowance"] += allowance
        if st in cnode["shifts_summary"]:
            cnode["shifts_summary"][st] += allowance

        # department node under client
        dnode = cnode["departments_breakdown"].setdefault(
            dept_name,
            {
                "_emp_set": set(),
                "_partner_set": set(),
                "total_allowance": 0.0,
                "shifts_summary": {k: 0.0 for k in SHIFT_KEYS},
                "client_partners": {}
            }
        )
        dnode["_emp_set"].add(eid)
        dnode["_partner_set"].add(partner)
        dnode["total_allowance"] += allowance
        if st in dnode["shifts_summary"]:
            dnode["shifts_summary"][st] += allowance

        # partner node under department
        pnode = dnode["client_partners"].setdefault(
            partner,
            {
                "_emp_set": set(),
                "total_allowance": 0.0,
                "shifts_summary": {k: 0.0 for k in SHIFT_KEYS},
                "_employees": {}  # eid -> employee row with per-shift
            }
        )
        pnode["_emp_set"].add(eid)
        pnode["total_allowance"] += allowance
        if st in pnode["shifts_summary"]:
            pnode["shifts_summary"][st] += allowance

        # employee accumulation under partner
        erow = pnode["_employees"].get(eid)
        if not erow:
            erow = {
                "emp_id": clean_str(emp_id),
                "emp_name": clean_str(emp_name),
                "client": client_name,
                "client_partner": partner,
                **{k: 0.0 for k in SHIFT_KEYS},
                "total_allowance": 0.0
            }
            pnode["_employees"][eid] = erow
        if st in erow:
            erow[st] += allowance
        erow["total_allowance"] += allowance

        # global tallies
        global_emp_set.add(eid)
        global_dept_set.add(f"{client_name}|{dept_name}")  # unique client-dept pairs
        global_total += allowance

    items: List[Dict[str, Any]] = []
    for cname, cnode in clients_map.items():
        items.append({
            "client": cname,
            "departments": len(cnode["_dept_set"]),
            "headcount": len(cnode["_emp_set"]),
            "total_allowance": round(float(cnode["total_allowance"]), 2),
            "_shifts_summary": cnode.get("shifts_summary", {}),
        })

    client_field_kind = {
        "client": "alpha",
        "departments": "num",
        "headcount": "num",
        "total_allowance": "num",
    }
    client_shift_key = _parse_shift_key(str(clients_sort_by_raw), SHIFT_KEY_SET)
    if client_shift_key:
        sort_field = f"shift:{client_shift_key}"
        client_kind = "num"
    else:
        sb = (str(clients_sort_by_raw) or "").strip().lower()
        sort_field = sb if sb in client_field_kind else "total_allowance"
        client_kind = client_field_kind.get(sort_field, "num")

    client_effective_order = _effective_order(client_kind, str(clients_sort_order))
    client_reverse = (client_effective_order == "desc")

    def _client_sort_key(it: Dict[str, Any]):
        if client_shift_key:
            v = it["_shifts_summary"].get(client_shift_key, 0.0)
            return (_num(v), _alpha(it.get("client")))
        if client_kind == "num":
            return (_num(it.get(sort_field)), _alpha(it.get("client")))
        else:
            return (_alpha(it.get(sort_field)), _num(it.get("total_allowance")))

    items.sort(key=_client_sort_key, reverse=client_reverse)

    
    if isinstance(top_n, int) and top_n > 0:
        items = items[:top_n]

    
    clients_out: Dict[str, Any] = OrderedDict()
    for it in items:
        cname = it["client"]
        cnode = clients_map[cname]

       
        departments_out: Dict[str, Any] = OrderedDict()
        dept_items: List[Tuple[str, Dict[str, Any], Dict[str, Any]]] = []
        for dname, dnode in cnode["departments_breakdown"].items():
            dept_items.append((
                dname,
                dnode,
                {
                    "department": dname,
                    "client_partner_count": len(dnode["_partner_set"]),
                    "headcount": len(dnode["_emp_set"]),
                    "total_allowance": round(float(dnode["total_allowance"]), 2),
                    "_shifts_summary": dnode.get("shifts_summary", {}),
                }
            ))

        # Field kinds decide default:
        #   - NUM -> desc (by _effective_order 'default')
        #   - ALPHA -> asc
        dept_field_kind = {
            "department": "alpha",
            "client_partner_count": "num",
            "headcount": "num",
            "total_allowance": "num",
        }
        # Accept both 'shift:<KEY>' and bare '<KEY>'
        dept_shift_key = _parse_shift_key(str(depts_sort_by_raw), SHIFT_KEY_SET)
        if dept_shift_key:
            d_sort_field = f"shift:{dept_shift_key}"
            d_kind = "num"
        else:
            dsb = (str(depts_sort_by_raw) or "").strip().lower()
            # DEFAULT: total_allowance for department level
            d_sort_field = dsb if dsb in dept_field_kind else "total_allowance"
            d_kind = dept_field_kind.get(d_sort_field, "num")

        d_effective_order = _effective_order(d_kind, str(depts_sort_order))
        d_reverse = (d_effective_order == "desc")

        def _dept_sort_key(tup):
            _dname, _dnode, metrics = tup
            if dept_shift_key:
                v = metrics["_shifts_summary"].get(dept_shift_key, 0.0)
                return (_num(v), _alpha(metrics.get("department")))
            if d_kind == "num":
                return (_num(metrics.get(d_sort_field)), _alpha(metrics.get("department")))
            else:
                return (_alpha(metrics.get(d_sort_field)), _num(metrics.get("total_allowance")))

        dept_items.sort(key=_dept_sort_key, reverse=d_reverse)

        # Now render departments (sorted)
        for dname, dnode, metrics in dept_items:
          
            partners_out: Dict[str, Any] = OrderedDict()
            partner_items: List[Tuple[str, Dict[str, Any], Dict[str, Any]]] = []
            for pname, pnode in dnode["client_partners"].items():
                partner_items.append((
                    pname,
                    pnode,
                    {
                        "client_partner": pname,
                        "headcount": len(pnode["_emp_set"]),
                        "total_allowance": round(float(pnode["total_allowance"]), 2),
                        "_shifts_summary": pnode.get("shifts_summary", {}),
                    }
                ))

            partner_field_kind = {
                "client_partner": "alpha",
                "headcount": "num",
                "total_allowance": "num",
            }
            partner_shift_key = _parse_shift_key(str(partners_sort_by_raw), SHIFT_KEY_SET)
            if partner_shift_key:
                p_sort_field = f"shift:{partner_shift_key}"
                p_kind = "num"
            else:
                psb = (str(partners_sort_by_raw) or "").strip().lower()
                p_sort_field = psb if psb in partner_field_kind else "client_partner"
                p_kind = partner_field_kind.get(p_sort_field, "alpha")

            p_effective_order = _effective_order(p_kind, str(partners_sort_order))
            p_reverse = (p_effective_order == "desc")

            def _partner_sort_key(tup):
                _pname, _pnode, pmetrics = tup
                if partner_shift_key:
                    v = pmetrics["_shifts_summary"].get(partner_shift_key, 0.0)
                    return (_num(v), _alpha(pmetrics.get("client_partner")))
                if p_kind == "num":
                    return (_num(pmetrics.get(p_sort_field)), _alpha(pmetrics.get("client_partner")))
                else:
                    return (_alpha(pmetrics.get(p_sort_field)), _num(pmetrics.get("total_allowance")))

            partner_items.sort(key=_partner_sort_key, reverse=p_reverse)

            # Render partners (sorted)
            for pname, pnode, pmetrics in partner_items:
                
                employees_list = list(pnode["_employees"].values())

                emp_field_kind = {
                    "emp_name": "alpha",
                    "total_allowance": "num",
                }
                # Employees don't have headcount; if requested, fall back to total_allowance
                esb_raw = str(employees_sort_by_raw or "").strip().lower()
                if esb_raw == "headcount":
                    esb = "total_allowance"
                else:
                    esb = esb_raw

                emp_shift_key = _parse_shift_key(str(esb), SHIFT_KEY_SET)
                if emp_shift_key:
                    e_sort_field = f"shift:{emp_shift_key}"
                    e_kind = "num"
                else:
                    e_sort_field = esb if esb in emp_field_kind else "total_allowance"
                    e_kind = emp_field_kind.get(e_sort_field, "num")

                e_effective_order = _effective_order(e_kind, str(employees_sort_order))
                e_reverse = (e_effective_order == "desc")

                def _employee_sort_key(erow: Dict[str, Any]):
                    if emp_shift_key:
                        v = erow.get(emp_shift_key, 0.0)  # per-shift value present on erow
                        return (_num(v), _alpha(erow.get("emp_name")))
                    if e_kind == "num":
                        return (_num(erow.get(e_sort_field)), _alpha(erow.get("emp_name")))
                    else:
                        return (_alpha(erow.get(e_sort_field)), _num(erow.get("total_allowance")))

                employees_list.sort(key=_employee_sort_key, reverse=e_reverse)

                if isinstance(employee_cap, int) and employee_cap > 0:
                    employees_list = employees_list[:employee_cap]

                partners_out[pname] = {
                    "headcount": pmetrics["headcount"],
                    "total_allowance": pmetrics["total_allowance"],
                    "shifts_summary": {k: round(float(v), 2) for k, v in pnode["shifts_summary"].items()},
                    "employees": employees_list,
                }

            departments_out[dname] = {
                "client_partner_count": metrics["client_partner_count"],
                "headcount": metrics["headcount"],
                "total_allowance": metrics["total_allowance"],
                "shifts_summary": {k: round(float(v), 2) for k, v in dnode["shifts_summary"].items()},
                "client_partners": partners_out,
            }

        clients_out[cname] = {
            "departments": it["departments"],
            "headcount": it["headcount"],
            "total_allowance": it["total_allowance"],
            "shifts_summary": {k: round(float(v), 2) for k, v in cnode["shifts_summary"].items()},
            "departments_breakdown": departments_out,
        }

    
    result = {
        "periods": periods,
        "message": period_message or "",
        "summary": {
            "total_clients": len(clients_out),
            "departments": len(set(global_dept_set)),  # unique client|dept pairs across all clients
            "headcount": len(global_emp_set),
            "total_allowance": round(float(global_total), 2),
        },
        "clients": clients_out,
        "shift_details": _build_shift_details_object_with_currency(db, pairs, currency="INR"),
    }

    return result

try:
    # SHIFT_TYPES may be a dict mapping OR a set/list of codes in your project
    from utils.shift_config import SHIFT_TYPES, get_all_shift_keys
    SHIFT_KEYS: List[str] = [str(k).strip().upper() for k in get_all_shift_keys()]
except Exception:
    # Fallback: derive SHIFT_KEYS from SHIFT_TYPES if possible
    try:
        from utils.shift_config import SHIFT_TYPES  # type: ignore
    except Exception:
        SHIFT_TYPES = {}  # last-resort fallback (empty)
    if isinstance(SHIFT_TYPES, dict):
        SHIFT_KEYS = [str(k).strip().upper() for k in SHIFT_TYPES.keys()]
    elif isinstance(SHIFT_TYPES, (set, list, tuple)):
        SHIFT_KEYS = [str(k).strip().upper() for k in SHIFT_TYPES]
    else:
        SHIFT_KEYS = []
SHIFT_KEY_SET: Set[str] = set(SHIFT_KEYS)

def clean_str(value: Any) -> str:
    """Normalize strings (handles None, whitespace, zero-width, nbsp, and quote-only)."""
    if value is None:
        return ""
    s = value if isinstance(value, str) else str(value)
    s = s.strip().replace("\u200b", "").replace("\u00a0", "").strip()
    for _ in range(2):
        if len(s) >= 2 and s[0] == s[-1] and s[0] in ("'", '"'):
            s = s[1:-1].strip()
    if s in ("'", "''", '"', '""'):
        return ""
    if s.upper() in ("NULL", "NONE", "NAN"):
        return ""
    return s


def _is_all(value: Any) -> bool:
    """True if value represents ALL."""
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip().upper() == "ALL"
    if isinstance(value, list):
        if len(value) == 0:
            return True
        if len(value) == 1 and str(value[0]).strip().upper() == "ALL":
            return True
    return False


def _normalize_dash(s: str) -> str:
    """Convert dash variants to standard '-'."""
    return (s or "").replace("–", "-").replace("—", "-").replace("−", "-")


def _as_list(x: Any) -> List[Any]:
    """Return x as a list (None -> [], scalar -> [scalar], list -> list)."""
    if x is None:
        return []
    return x if isinstance(x, list) else [x]


def _payload_to_plain_dict(payload: Any) -> dict:
    """Ensure payload is a plain dict (handles Pydantic v1/v2 and None)."""
    if payload is None:
        return {}
    if isinstance(payload, dict):
        return {k: v for k, v in payload.items() if v is not None}
    if hasattr(payload, "model_dump"):  # Pydantic v2
        try:
            return payload.model_dump(exclude_none=True)
        except Exception:
            pass
    if hasattr(payload, "dict"):  # Pydantic v1
        try:
            return payload.dict(exclude_none=True)
        except Exception:
            pass
    try:
        return dict(payload)
    except Exception:
        return {}



def _coerce_int_list(values: Any, field_name: str, four_digit_year: bool = False) -> List[int]:
    if values is None:
        return []
    if not isinstance(values, list):
        raise HTTPException(status_code=400, detail=f"'{field_name}' must be a list.")
    out: List[int] = []
    for v in values:
        if v is None:
            continue
        s = clean_str(v)
        if not s:
            continue
        if four_digit_year and field_name == "years":
            if not s.isdigit() or len(s) != 4:
                raise HTTPException(400, "Invalid year. Year must be in YYYY format (e.g., 2024).")
            y = int(s)
            if y <= 0:
                raise HTTPException(400, "Invalid year. Year must be a positive 4-digit number.")
            out.append(y)
            continue
        try:
            out.append(int(s))
        except Exception:
            raise HTTPException(400, f"Invalid value in '{field_name}': {v}")
    return out


def parse_clients(value: Any) -> Optional[List[str]]:
    if _is_all(value):
        return None
    if isinstance(value, str):
        v = clean_str(value)
        return [v] if v else None
    if isinstance(value, list):
        out = [clean_str(x) for x in value if clean_str(x)]
        return out or None
    raise HTTPException(400, "clients must be 'ALL', string, or list.")


def parse_departments(value: Any) -> Optional[List[str]]:
    if _is_all(value):
        return None
    if isinstance(value, str):
        v = clean_str(value)
        return [v] if v else None
    if isinstance(value, list):
        out = [clean_str(x) for x in value if clean_str(x)]
        return out or None
    raise HTTPException(400, "departments must be 'ALL', string, or list.")


def parse_shifts(value: Any) -> Optional[Set[str]]:
    if _is_all(value):
        return None
    if isinstance(value, str):
        v = clean_str(value).upper()
        if SHIFT_KEY_SET and v not in SHIFT_KEY_SET:
            raise HTTPException(400, f"Invalid shift type: {v}")
        return {v}
    if isinstance(value, list):
        out: Set[str] = set()
        for x in value:
            v = clean_str(x).upper()
            if not v:
                continue
            if SHIFT_KEY_SET and v not in SHIFT_KEY_SET:
                raise HTTPException(400, f"Invalid shift type: {v}")
            out.add(v)
        return out or None
    raise HTTPException(400, "shifts must be 'ALL', string, or list.")


def parse_top(value: Any) -> Optional[int]:
    if _is_all(value) or value is None:
        return None
    if isinstance(value, int):
        if value <= 0:
            raise HTTPException(400, "top must be > 0.")
        return value
    s = clean_str(value)
    if s.isdigit():
        iv = int(s)
        if iv <= 0:
            raise HTTPException(400, "top must be > 0.")
        return iv
    raise HTTPException(400, "top must be 'ALL' or a number.")


def parse_employee_limit(value: Any) -> Optional[int]:
    """
    Headcounts behavior:
    - "ALL" -> None
    - "10" -> 10
    - "1-10" -> 10
    - ["1-10","11-50"] -> 50 (max upper bound)
    Meaning: show up to N employees at the detailed list level.
    """
    if _is_all(value) or value is None:
        return None
    items = value if isinstance(value, list) else [value]
    limits: List[int] = []
    for item in items:
        s = _normalize_dash(clean_str(item)).upper()
        if not s or s == "ALL":
            continue
        if "-" in s:
            lo, hi = [x.strip() for x in s.split("-", 1)]
            if not lo.isdigit() or not hi.isdigit():
                raise HTTPException(400, "Invalid headcount range.")
            lo_i, hi_i = int(lo), int(hi)
            if lo_i <= 0 or lo_i > hi_i:
                raise HTTPException(400, "Invalid headcount range.")
            limits.append(hi_i)
        else:
            if not s.isdigit() or int(s) <= 0:
                raise HTTPException(400, "Invalid headcount value.")
            limits.append(int(s))
    return max(limits) if limits else None


def parse_sort_order(value: Any) -> str:
    v = clean_str(value).lower()
    return v if v in ("default", "asc", "desc") else "default"


def parse_sort_by_department(value: Any) -> str:
    v = clean_str(value).lower()
    allowed = {"department", "clients", "headcount", "total_allowance"}
    return v if v in allowed else ""


def apply_sort_dict_department(data: Dict[str, dict], sort_by: str, sort_order: str) -> Dict[str, dict]:
    if sort_order == "default" or not sort_by:
        return data
    reverse = (sort_order == "desc")
    if sort_by == "department":
        return dict(sorted(data.items(), key=lambda kv: (kv[0] or "").lower(), reverse=reverse))
    return dict(sorted(data.items(), key=lambda kv: kv[1].get(sort_by, 0) or 0, reverse=reverse))


def parse_allowance_ranges(value: Any) -> Optional[List[tuple]]:
    """
    None/"ALL" -> None
    "A-B" -> [(A,B)]
    ["A-B", "C-D"] -> [(A,B), (C,D)]
    """
    if _is_all(value) or value is None:
        return None
    items = _as_list(value)
    ranges: List[tuple] = []
    for item in items:
        s = _normalize_dash(clean_str(item))
        if not s or "-" not in s:
            raise HTTPException(400, f"Invalid allowance range: {item}. Use 'min-max'.")
        lo_s, hi_s = [x.strip() for x in s.split("-", 1)]

        def is_num(x: str) -> bool:
            try:
                float(x)
                return True
            except Exception:
                return False

        if not is_num(lo_s) or not is_num(hi_s):
            raise HTTPException(400, f"Invalid allowance range: {item}. Use numeric 'min-max'.")
        lo, hi = float(lo_s), float(hi_s)
        if lo < 0 or hi < 0 or lo > hi:
            raise HTTPException(400, f"Invalid allowance range (min <= max and non-negative): {item}")
        ranges.append((lo, hi))
    return ranges or None


def allowance_in_ranges(total: float, ranges: Optional[List[tuple]]) -> bool:
    if not ranges:
        return True
    return any(lo <= total <= hi for lo, hi in ranges)


def top_n_dict(data: Dict[str, dict], n: Optional[int]) -> Dict[str, dict]:
    if not n:
        return data
    return dict(list(data.items())[:n])



def _find_recent_month_with_data(
    db: Session, start_year: int, start_month: int, lookback: int = 12
) -> Optional[Tuple[int, int]]:
    y, m = start_year, start_month
    for _ in range(lookback):
        exists = db.query(ShiftAllowances.id).filter(
            extract("year", ShiftAllowances.duration_month) == y,
            extract("month", ShiftAllowances.duration_month) == m,
        ).first()
        if exists:
            return (y, m)
        if m == 1:
            y, m = y - 1, 12
        else:
            m -= 1
    return None


def validate_years_months(payload: dict, db: Session) -> Tuple[List[Tuple[int, int]], Optional[str]]:
    today = date.today()
    cy, cm = today.year, today.month

    payload = _payload_to_plain_dict(payload)
    years_raw = payload.get("years")
    months_raw = payload.get("months")

    if _is_all(years_raw):
        years_raw = None
    if _is_all(months_raw):
        months_raw = None

    years = _coerce_int_list(_as_list(years_raw), "years", four_digit_year=True) if years_raw else []
    months = _coerce_int_list(_as_list(months_raw), "months") if months_raw else []

    pairs: List[Tuple[int, int]] = []
    message: Optional[str] = None

    if not years and not months:
        exists_current = db.query(ShiftAllowances.id).filter(
            extract("year", ShiftAllowances.duration_month) == cy,
            extract("month", ShiftAllowances.duration_month) == cm,
        ).first()
        if exists_current:
            pairs = [(cy, cm)]
        else:
            recent = _find_recent_month_with_data(db, cy, cm, lookback=12)
            if recent:
                ry, rm = recent
                pairs = [(ry, rm)]
                message = (
                    f"No data for current month. Showing latest available month within last 12 months: {ry}-{rm:02d}."
                )
            else:
                pairs = [(cy, cm)]
                message = "No data found in the last 12 months. Defaulting to current month."
    elif years and months:
        for y in years:
            for m in months:
                pairs.append((y, m))
    elif years:
        for y in years:
            for m in range(1, 13):
                pairs.append((y, m))
    elif months:
        for m in months:
            pairs.append((cy, m))

    valid_pairs: List[Tuple[int, int]] = []
    future_pairs: List[str] = []
    for y, m in pairs:
        if y > cy or (y == cy and m > cm):
            future_pairs.append(f"{y}-{m:02d}")
        else:
            valid_pairs.append((y, m))

    if future_pairs:
        msg = f"Future periods ignored: {', '.join(sorted(set(future_pairs)))}"
        message = f"{message} {msg}".strip() if message else msg

    if (years or months) and not valid_pairs:
        return [], message

    return sorted(set(valid_pairs)), message


def _build_shift_details_from_config(shift_types_obj: Any) -> Dict[str, Dict[str, Any]]:
    """
    Accepts either:
      - dict mapping: { "PST_MST": "PST/MST\\n(07 PM - 06 AM)\\nINR 700", ... }
      - iterable of keys: {"PST_MST", "US_INDIA", ...}
    Returns:
      {
        "PST_MST": {
          "label": "PST/MST" or "PST_MST",
          "timing": "07 PM - 06 AM" or None,
          "amount": 700 or None,
          "currency": "INR" or None
        },
        ...
      }
    """
    out: Dict[str, Dict[str, Any]] = {}
    if isinstance(shift_types_obj, dict):
        for key, blob in shift_types_obj.items():
            lines = str(blob).splitlines()
            label = clean_str(lines[0]) if len(lines) >= 1 else clean_str(key)
            timing_raw = clean_str(lines[1]) if len(lines) >= 2 else ""
            timing = timing_raw.strip("()") if timing_raw else None

            amount_raw = clean_str(lines[2]) if len(lines) >= 3 else ""
            currency: Optional[str] = None
            amount_val: Optional[float] = None
            if amount_raw:
                parts = amount_raw.split()
                if len(parts) >= 2 and parts[0].isalpha():
                    currency = parts[0]
                    try:
                        amount_val = float(parts[1].replace(",", ""))
                    except Exception:
                        amount_val = None
                else:
                    for p in parts:
                        try:
                            amount_val = float(p.replace(",", ""))
                            break
                        except Exception:
                            continue
                    if "INR" in amount_raw.upper():
                        currency = "INR"

            out[key] = {
                "label": label or key,
                "timing": timing,
                "amount": int(amount_val) if isinstance(amount_val, float) and amount_val.is_integer() else amount_val,
                "currency": currency,
            }
        return out

    if isinstance(shift_types_obj, (set, list, tuple)):
        for key in shift_types_obj:
            skey = clean_str(key)
            out[skey] = {"label": skey, "timing": None, "amount": None, "currency": None}
        return out

    return out

def _alpha(v: Any) -> str:
    """String-normalized lowercase for stable alpha sorting."""
    return (str(v or "")).lower()

def _num(v: Any) -> float:
    """Safe numeric conversion with fallback."""
    try:
        return float(v if v is not None else 0)
    except Exception:
        return 0.0

def _parse_shift_key(sb: str, shift_keys: Set[str]) -> Optional[str]:
    """
    Extract a shift key from either 'shift:<KEY>' or bare '<KEY>'.
    Returns UPPER shift key if recognized; otherwise None.
    """
    if not sb:
        return None
    s = str(sb).strip()
    if s.lower().startswith("shift:"):
        key = s.split(":", 1)[1].strip().upper()
        return key if key in shift_keys else None
    u = s.upper()
    return u if u in shift_keys else None

def _effective_order(for_field_kind: str, requested: str) -> str:
    """
    Resolve 'default' order based on field kind:
      - numeric -> desc
      - alpha   -> asc
    """
    so = (requested or "default").strip().lower()
    if so in ("asc", "desc"):
        return so
    return "desc" if for_field_kind == "num" else "asc"


def department_analytics_service(db: Session, payload: 'DepartmentAnalyticsRequest') -> Dict[str, Any]:
    """
    Department analytics:
      - Aggregate by department
      - Drilldown (only if exactly one department is selected): clients -> client_partners
      - Sorting numeric/alphabetic
      - Allowance range filter (inclusive)
      - No "starts with" filters
      - OPTION A: Employees only under partners (omit client-level employees when partners exist)
      - Adds top-level 'shift_details' parsed from SHIFT_TYPES (imported, not hardcoded)

    ENHANCEMENT:
      - Client-level sorting in drilldown via sort_clients_by/sort_clients_order.
        Allowed: client | client_partner_count | headcount | total_allowance | shift:<KEY> | <KEY>
        Defaults to total_allowance (numeric desc).
    """
    payload_dict = _payload_to_plain_dict(payload)

    clients_filter = parse_clients(payload_dict.get("clients", "ALL"))
    depts_filter = parse_departments(payload_dict.get("departments", "ALL"))
    shifts_filter = parse_shifts(payload_dict.get("shifts", "ALL"))
    top_n = parse_top(payload_dict.get("top", "ALL"))
    employee_cap = parse_employee_limit(payload_dict.get("headcounts", "ALL"))

    # Department-level sorting (existing)
    sort_by = parse_sort_by_department(payload_dict.get("sort_by", ""))
    sort_order = parse_sort_order(payload_dict.get("sort_order", "default"))

    # NEW: Drilldown client sorting
    sort_clients_by = payload_dict.get("sort_clients_by", "total_allowance")
    sort_clients_order = payload_dict.get("sort_clients_order", "default")

    allowance_ranges = parse_allowance_ranges(payload_dict.get("allowance"))

    # Periods
    pairs, period_message = validate_years_months(payload_dict, db=db)
    periods = [f"{y:04d}-{m:02d}" for y, m in pairs]

    # Shift details (from config)
    shift_details = _build_shift_details_from_config(SHIFT_TYPES)

    # Prepare shift keys / validation set
    try:
        # Prefer config-driven keys (if you have get_all_shift_keys, you can use that instead)
        SHIFT_KEYS = [str(k).strip().upper() for k in get_all_shift_keys()]  # type: ignore[name-defined]
    except Exception:
        # Fallback from SHIFT_TYPES if needed
        SHIFT_KEYS = [str(k).strip().upper() for k in getattr(globals().get("SHIFT_TYPES", {}), "keys", lambda: {})()]
        if not SHIFT_KEYS and isinstance(SHIFT_TYPES, dict):  # type: ignore[name-defined]
            SHIFT_KEYS = [str(k).strip().upper() for k in SHIFT_TYPES.keys()]  # type: ignore[name-defined]
    SHIFT_KEY_SET: Set[str] = set(SHIFT_KEYS)

    if not pairs:
        return {
            "periods": [],
            "message": period_message,
            "summary": {
                "total_departments": 0,
                "clients": 0,
                "headcount": 0,
                "total_allowance": 0.0,
            },
            "departments": {},
            "shift_details": shift_details,
        }

    # Drilldown if exactly one department selected
    drilldown_dept = None
    if depts_filter and len(depts_filter) == 1:
        drilldown_dept = depts_filter[0]

    # Base filters (exact only)
    base_filters_extra: List[Any] = []
    if clients_filter:
        base_filters_extra.append(
            func.lower(func.trim(ShiftAllowances.client)).in_([c.lower() for c in clients_filter])
        )
    if depts_filter:
        base_filters_extra.append(
            func.lower(func.trim(ShiftAllowances.department)).in_([d.lower() for d in depts_filter])
        )

    # Period filter
    ym_filters = [
        and_(
            extract("year", ShiftAllowances.duration_month) == y,
            extract("month", ShiftAllowances.duration_month) == m,
        )
        for y, m in pairs
    ]

    ShiftsAmountAlias = aliased(ShiftsAmount)

    rows_q = (
        db.query(
            ShiftAllowances.emp_id,
            ShiftAllowances.emp_name,
            ShiftAllowances.client,
            ShiftAllowances.department,
            ShiftAllowances.client_partner,
            ShiftMapping.shift_type,
            ShiftMapping.days,
            func.coalesce(ShiftsAmountAlias.amount, 0),
        )
        .select_from(ShiftAllowances)
        .join(ShiftMapping, ShiftMapping.shiftallowance_id == ShiftAllowances.id)
        .outerjoin(
            ShiftsAmountAlias,
            and_(
                cast(ShiftsAmountAlias.payroll_year, Integer) == extract("year", ShiftAllowances.duration_month),
                func.upper(func.trim(ShiftMapping.shift_type)) == func.upper(func.trim(ShiftsAmountAlias.shift_type)),
            ),
        )
        .filter(or_(*ym_filters))
        .filter(*base_filters_extra)
    )
    if shifts_filter:
        rows_q = rows_q.filter(func.upper(func.trim(ShiftMapping.shift_type)).in_(list(shifts_filter)))
    rows = rows_q.all()

    if not rows:
        return {
            "periods": periods,
            "message": period_message,
            "summary": {
                "total_departments": 0,
                "clients": 0,
                "headcount": 0,
                "total_allowance": 0.0,
            },
            "departments": {},
            "shift_details": shift_details,
        }

    # Aggregation structures
    dept_nodes: Dict[str, Dict[str, Any]] = {}
    clients_map: Dict[str, Dict[str, Any]] = {}  # drilldown
    partners_map: Dict[Tuple[str, str], Dict[str, Any]] = {}  # drilldown
    employees_global: Dict[str, Dict[str, Any]] = {}  # dept-level shift summary

    for emp_id, emp_name, client, dept, cp, stype, days, amt in rows:
        dname = clean_str(dept) or "UNKNOWN"
        cname = clean_str(client) or "UNKNOWN"
        cpname = clean_str(cp) or "UNKNOWN"
        eid = clean_str(emp_id)
        st = (clean_str(stype) or "").upper()

        if SHIFT_KEY_SET and st and st not in SHIFT_KEY_SET:
            continue

        allowance = float(days or 0) * float(amt or 0)

        # Department aggregation
        dnode = dept_nodes.setdefault(dname, {"clients_set": set(), "emp_set": set(), "total_allowance": 0.0})
        dnode["clients_set"].add(cname)
        if eid:
            dnode["emp_set"].add(eid)
        dnode["total_allowance"] += allowance

        # Drilldown population only for the selected department
        if drilldown_dept and dname.lower() == drilldown_dept.lower():
            # Client-level node
            cnode = clients_map.setdefault(
                cname,
                {
                    "partners_set": set(),
                    "emp_set": set(),
                    "total_allowance": 0.0,
                    "shift_totals": {k: 0.0 for k in SHIFT_KEYS},
                    "employees": {},  # eid -> employee totals (client level)
                },
            )
            cnode["partners_set"].add(cpname)
            if eid:
                cnode["emp_set"].add(eid)
            cnode["total_allowance"] += allowance
            if st in cnode["shift_totals"]:
                cnode["shift_totals"][st] += allowance

            # Client-level employees (may be omitted in response if partners exist)
            if eid:
                pe = cnode["employees"].get(eid)
                if not pe:
                    pe = {
                        "emp_id": eid,
                        "emp_name": clean_str(emp_name),
                        "client": cname,
                        "client_partner": cpname,
                        **{k: 0.0 for k in SHIFT_KEYS},
                        "total_allowance": 0.0,
                    }
                    cnode["employees"][eid] = pe
                if st in pe:
                    pe[st] += allowance
                pe["total_allowance"] += allowance

            # Partner-level node
            pnode = partners_map.setdefault(
                (cname, cpname),
                {
                    "emp_set": set(),
                    "total_allowance": 0.0,
                    "shift_totals": {k: 0.0 for k in SHIFT_KEYS},
                    "employees": {},  # eid -> employee totals (partner level)
                },
            )
            if eid:
                pnode["emp_set"].add(eid)
            pnode["total_allowance"] += allowance
            if st in pnode["shift_totals"]:
                pnode["shift_totals"][st] += allowance

            # Partner-level employees (Option A display source)
            if eid:
                pe_partner = pnode["employees"].get(eid)
                if not pe_partner:
                    pe_partner = {
                        "emp_id": eid,
                        "emp_name": clean_str(emp_name),
                        "client": cname,
                        "client_partner": cpname,
                        **{k: 0.0 for k in SHIFT_KEYS},
                        "total_allowance": 0.0,
                    }
                    pnode["employees"][eid] = pe_partner
                if st in pe_partner:
                    pe_partner[st] += allowance
                pe_partner["total_allowance"] += allowance

            # Dept-level shifts summary
            if eid:
                eg = employees_global.setdefault(
                    eid,
                    {
                        "emp_id": eid,
                        "emp_name": clean_str(emp_name),
                        "client": cname,
                        "client_partner": cpname,
                        **{k: 0.0 for k in SHIFT_KEYS},
                        "total_allowance": 0.0,
                    },
                )
                if st in eg:
                    eg[st] += allowance
                eg["total_allowance"] += allowance

    # Build departments (apply allowance ranges)
    departments_out: Dict[str, Any] = {}
    for dname, node in dept_nodes.items():
        total_allow = round(node["total_allowance"], 2)
        if not allowance_in_ranges(total_allow, allowance_ranges):
            continue
        departments_out[dname] = {
            "clients": len(node["clients_set"]),
            "headcount": len(node["emp_set"]),
            "total_allowance": total_allow,
        }

    # Sorting & Top-N for departments
    if sort_order != "default" and sort_by:
        departments_out = apply_sort_dict_department(departments_out, sort_by, sort_order)
    departments_out = top_n_dict(departments_out, top_n)

    # Summary
    overall_clients: Set[str] = set()
    overall_emps: Set[str] = set()
    total_allowance_sum = 0.0
    for dname in departments_out:
        node = dept_nodes[dname]
        overall_clients |= node["clients_set"]
        overall_emps |= node["emp_set"]
        total_allowance_sum += node["total_allowance"]

    result: Dict[str, Any] = {
        "periods": periods,
        "message": period_message,
        "summary": {
            "total_departments": len(departments_out),
            "clients": len(overall_clients),
            "headcount": len(overall_emps),
            "total_allowance": round(total_allowance_sum, 2),
        },
        "departments": departments_out,
        "shift_details": shift_details,  
    }

    # Drilldown (only if exactly one department selected AND it survived filters)
    if drilldown_dept:
        selected_key = next((k for k in departments_out if k.lower() == drilldown_dept.lower()), None)
        if selected_key:
            dept_obj = result["departments"][selected_key]

            # Department-level shifts summary
            shifts_summary = {k: 0.0 for k in SHIFT_KEYS}
            for _eid, emp_data in employees_global.items():
                for k in SHIFT_KEYS:
                    shifts_summary[k] += emp_data.get(k, 0.0)
            dept_obj["shifts_summary"] = {k: round(v, 2) for k, v in shifts_summary.items()}

          
            # Build sortable list from clients_map for this department
            client_items: List[Tuple[str, Dict[str, Any], Dict[str, Any]]] = []
            for cname, cnode in clients_map.items():
                client_items.append((
                    cname,
                    cnode,
                    {
                        "client": cname,
                        "client_partner_count": len(cnode["partners_set"]),
                        "headcount": len(cnode["emp_set"]),
                        "total_allowance": round(float(cnode["total_allowance"]), 2),
                        "_shifts_summary": cnode.get("shift_totals", {}),
                    }
                ))

            # Allowed client fields & kinds
            client_field_kind = {
                "client": "alpha",
                "client_partner_count": "num",
                "headcount": "num",
                "total_allowance": "num",
            }

            client_shift_key = _parse_shift_key(str(sort_clients_by), SHIFT_KEY_SET)
            if client_shift_key:
                c_sort_field = f"shift:{client_shift_key}"
                c_kind = "num"
            else:
                csb = (str(sort_clients_by) or "").strip().lower()
                # Fallback to total_allowance (numeric desc default)
                c_sort_field = csb if csb in client_field_kind else "total_allowance"
                c_kind = client_field_kind.get(c_sort_field, "num")

            c_effective_order = _effective_order(c_kind, str(sort_clients_order))
            c_reverse = (c_effective_order == "desc")

            def _client_sort_key(tup):
                _name, _node, metrics = tup
                if client_shift_key:
                    v = metrics["_shifts_summary"].get(client_shift_key, 0.0)
                    return (_num(v), _alpha(metrics.get("client")))
                if c_kind == "num":
                    return (_num(metrics.get(c_sort_field)), _alpha(metrics.get("client")))
                else:
                    return (_alpha(metrics.get(c_sort_field)), _num(metrics.get("total_allowance")))

            client_items.sort(key=_client_sort_key, reverse=c_reverse)

            # clients -> client_partners (Option A behavior)
            clients_out: "OrderedDict[str, Any]" = OrderedDict()
            for cname, cnode, cmetrics in client_items:
                # Build partner collections
                partners_out: Dict[str, Any] = {}
                for (client_key, partner_name), pnode in partners_map.items():
                    if client_key != cname:
                        continue

                    partner_employees = list(pnode["employees"].values())
                    if employee_cap:
                        partner_employees = sorted(
                            partner_employees, key=lambda x: x.get("total_allowance", 0.0), reverse=True
                        )[:employee_cap]

                    partners_out[partner_name] = {
                        "headcount": len(pnode["emp_set"]),
                        "total_allowance": round(pnode["total_allowance"], 2),
                        "shifts_summary": {k: round(v, 2) for k, v in pnode["shift_totals"].items()},
                        "employees": sorted(
                            partner_employees, key=lambda x: x.get("total_allowance", 0.0), reverse=True
                        ),
                    }

                has_partners = len(partners_out) > 0

                client_obj = {
                    "client_partner_count": cmetrics["client_partner_count"],
                    "headcount": cmetrics["headcount"],
                    "total_allowance": cmetrics["total_allowance"],
                    "shifts_summary": {k: round(float(v), 2) for k, v in cnode["shift_totals"].items()},
                    "client_partners": partners_out,
                }

                if not has_partners:
                    # Only when there are NO partners, include client-level employees
                    employees_list = list(cnode["employees"].values())
                    if employee_cap:
                        employees_list = sorted(
                            employees_list, key=lambda x: x["total_allowance"], reverse=True
                        )[:employee_cap]
                    client_obj["employees"] = sorted(
                        employees_list, key=lambda x: x["total_allowance"], reverse=True
                    )

                clients_out[cname] = client_obj

            dept_obj["clients_breakdown"] = clients_out
            result["departments"][selected_key] = dept_obj

    return result



