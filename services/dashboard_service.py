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
from schemas.dashboardschema import DashboardFilterRequest
import calendar
from utils.shift_config import get_all_shift_keys  
from collections import defaultdict

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
def client_analytics_service(db: Session, payload: dict) -> Dict[str, Any]:
    """
    Client analytics service with drilldown,
    partner-level breakdown, employee-level shift totals,
    and department-level allowance summary.

    - Previous month metrics are calculated ONLY for the selected drilldown client.
    - Adds client_partner_count in client summary.
    """

    payload = _payload_to_plain_dict(payload)

    clients_filter = parse_clients(payload.get("clients", "ALL"))
    depts_filter = parse_departments(payload.get("departments", "ALL"))
    shifts_filter = parse_shifts(payload.get("shifts", "ALL"))
    top_n = parse_top(payload.get("top", "ALL"))
    employee_cap = parse_employee_limit(payload.get("headcounts", "ALL"))
    sort_by = parse_sort_by(payload.get("sort_by", ""))
    sort_order = parse_sort_order(payload.get("sort_order", "default"))

  
    pairs, period_message = validate_years_months(payload, db=db)

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
        }

    periods = [f"{y:04d}-{m:02d}" for y, m in pairs]
    drilldown_client = clients_filter[0] if clients_filter and len(clients_filter) == 1 else None

    base_filters_extra: List[Any] = []

    if clients_filter:
        base_filters_extra.append(
            func.lower(func.trim(ShiftAllowances.client)).in_([c.lower() for c in clients_filter])
        )

    if depts_filter:
        base_filters_extra.append(
            func.lower(func.trim(ShiftAllowances.department)).in_([d.lower() for d in depts_filter])
        )

    yr_month_filters_selected = [
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
        .filter(or_(*yr_month_filters_selected))
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
        }


    latest_y, latest_m = sorted(pairs)[-1]
    prev_y, prev_m = (latest_y - 1, 12) if latest_m == 1 else (latest_y, latest_m - 1)

    prev_rows_q = (
        db.query(
            ShiftAllowances.emp_id,
            ShiftAllowances.client,
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
        .filter(
            extract("year", ShiftAllowances.duration_month) == prev_y,
            extract("month", ShiftAllowances.duration_month) == prev_m,
            *base_filters_extra
        )
    )

    if shifts_filter:
        prev_rows_q = prev_rows_q.filter(func.upper(func.trim(ShiftMapping.shift_type)).in_(list(shifts_filter)))

    prev_rows = prev_rows_q.all()


    client_nodes: Dict[str, Dict[str, Any]] = {}
    partners: Dict[str, Dict[str, Any]] = {}
    employees_global: Dict[str, Dict[str, Any]] = {}
    departments_by_client: Dict[str, Dict[str, float]] = {}

    for emp_id, emp_name, client, dept, cp, stype, days, amt in rows:
        client_name = clean_str(client) or "UNKNOWN"
        dept_name = clean_str(dept) or "UNKNOWN"
        partner_name = clean_str(cp) or "UNKNOWN"
        eid = clean_str(emp_id)
        st = clean_str(stype).upper()

        if SHIFT_KEY_SET and st not in SHIFT_KEY_SET:
            continue

        allowance = float(days or 0) * float(amt or 0)

        cnode = client_nodes.setdefault(
            client_name,
            {"dept_set": set(), "emp_set": set(), "total_allowance": 0.0, "partners_set": set()},
        )

        cnode["dept_set"].add(dept_name)
        cnode["partners_set"].add(partner_name)
        if eid:
            cnode["emp_set"].add(eid)
        cnode["total_allowance"] += allowance

        dept_map = departments_by_client.setdefault(client_name, {})
        dept_map[dept_name] = dept_map.get(dept_name, 0.0) + allowance

        if drilldown_client and client_name.lower() == drilldown_client.lower():
            pnode = partners.setdefault(
                partner_name,
                {
                    "dept_set": set(),
                    "emp_set": set(),
                    "total_allowance": 0.0,
                    "shift_totals": {k: 0.0 for k in SHIFT_KEYS},
                    "employees": {},
                },
            )

            pnode["dept_set"].add(dept_name)
            if eid:
                pnode["emp_set"].add(eid)
            pnode["total_allowance"] += allowance

            if st in pnode["shift_totals"]:
                pnode["shift_totals"][st] += allowance

            if eid:
                pe = pnode["employees"].get(eid)
                if not pe:
                    pe = {
                        "emp_id": eid,
                        "emp_name": clean_str(emp_name),
                        "department": dept_name,
                        "client_partner": partner_name,
                        **{k: 0.0 for k in SHIFT_KEYS},
                        "total_allowance": 0.0,
                    }
                    pnode["employees"][eid] = pe

                if st in pe:
                    pe[st] += allowance
                pe["total_allowance"] += allowance

                eg = employees_global.setdefault(
                    eid,
                    {
                        "emp_id": eid,
                        "emp_name": clean_str(emp_name),
                        "department": dept_name,
                        "client_partner": partner_name,
                        **{k: 0.0 for k in SHIFT_KEYS},
                        "total_allowance": 0.0,
                    },
                )
                if st in eg:
                    eg[st] += allowance
                eg["total_allowance"] += allowance

    
    clients_out: Dict[str, Any] = {}
    for cname, node in client_nodes.items():
        client_obj: Dict[str, Any] = {
            "departments": len(node["dept_set"]),
            "headcount": len(node["emp_set"]),
            "total_allowance": round(node["total_allowance"], 2),
            "client_partner_count": len(node["partners_set"]), 
        }

        
        if drilldown_client and cname.lower() == drilldown_client.lower():
            prev_rows_client = [r for r in prev_rows if clean_str(r[1]).lower() == cname.lower()]
            prev_emp_set = set()
            prev_total_allowance = 0.0
            for emp_id, client, days, amt in prev_rows_client:
                if emp_id:
                    prev_emp_set.add(emp_id)
                prev_total_allowance += float(days or 0) * float(amt or 0)

            client_obj["headcount_last_month"] = len(prev_emp_set)
            client_obj["total_allowance_last_month"] = round(prev_total_allowance, 2)

        clients_out[cname] = client_obj

    if sort_order != "default" and sort_by:
        clients_out = apply_sort_dict(clients_out, sort_by, sort_order)
    clients_out = top_n_dict(clients_out, top_n)

   
    overall_depts: Set[str] = set()
    overall_emps: Set[str] = set()
    total_allowance_sum = 0.0
    for cname in clients_out:
        node = client_nodes[cname]
        overall_depts |= node["dept_set"]
        overall_emps |= node["emp_set"]
        total_allowance_sum += node["total_allowance"]

    result: Dict[str, Any] = {
        "periods": periods,
        "message": period_message,
        "summary": {
            "total_clients": len(clients_out),
            "departments": len(overall_depts),
            "headcount": len(overall_emps),
            "total_allowance": round(total_allowance_sum, 2),
        },
        "clients": clients_out,
    }

    if drilldown_client and drilldown_client in client_nodes:
        selected_key = next(
            (k for k in client_nodes if k.lower() == drilldown_client.lower()),
            drilldown_client,
        )
        node = client_nodes[selected_key]

        client_obj: Dict[str, Any] = result["clients"][selected_key]

        dept_summary_raw = departments_by_client.get(selected_key, {})
        client_obj["department_summary"] = {
            dept: round(total, 2)
            for dept, total in sorted(dept_summary_raw.items(), key=lambda x: x[1], reverse=True)
        }

   
        shifts_summary = {k: 0.0 for k in SHIFT_KEYS}
        for emp_id, emp_data in employees_global.items():
            if emp_id not in node["emp_set"]:
                continue
            for k in SHIFT_KEYS:
                shifts_summary[k] += emp_data.get(k, 0.0)
        client_obj["shifts_summary"] = {k: round(v, 2) for k, v in shifts_summary.items()}

        partners_out: Dict[str, Any] = {}
        for partner_name, pnode in partners.items():
            employees_list = list(pnode["employees"].values())
            if employee_cap:
                employees_list = sorted(
                    employees_list, key=lambda x: x["total_allowance"], reverse=True
                )[:employee_cap]

            partners_out[partner_name] = {
                "departments": len(pnode["dept_set"]),
                "headcount": len(pnode["emp_set"]),
                "total_allowance": round(pnode["total_allowance"], 2),
                "shifts_summary": {k: round(v, 2) for k, v in pnode["shift_totals"].items()},
                "employees": sorted(employees_list, key=lambda x: x["total_allowance"], reverse=True),
            }

        client_obj["client_partners"] = partners_out
        result["clients"][selected_key] = client_obj

    return result
