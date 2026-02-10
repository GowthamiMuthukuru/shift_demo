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

try:
    from utils.shift_config import get_all_shift_keys  
except Exception:
    get_all_shift_keys = None  

def _load_shift_types() -> Set[str]:
    """
    Try to load known shift codes once at import.
    If not available, return an empty set and handle dynamically later.
    """
    try:
        if callable(get_all_shift_keys):
            return {str(s).strip().upper() for s in get_all_shift_keys()}
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
                    detail="Invalid year. Year must be in YYYY format (e.g., 2024)."
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

    reverse = (sort_order == "desc")

   
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
    payload_dict = _payload_to_dict(payload)
    value = payload_dict.get("headcounts", None)

    if value is None or _is_all(value):
        return

    if not isinstance(value, list):
        value = [value]

    for item in value:
        s = _normalize_dash(clean_str(item)).upper()
        if not s or s == "ALL":
            continue

        if "-" in s:
            lo, hi = [x.strip() for x in s.split("-", 1)]
            if not lo.isdigit() or not hi.isdigit():
                raise HTTPException(status_code=400, detail="Invalid headcount range.")
            lo_i, hi_i = int(lo), int(hi)
            if lo_i <= 0 or lo_i > hi_i:
                raise HTTPException(status_code=400, detail="Invalid headcount range.")
        else:
            if not s.isdigit() or int(s) <= 0:
                raise HTTPException(status_code=400, detail="Invalid headcount value.")


def parse_headcount_limit(value: Any) -> Optional[int]:
    """
    Returns numeric limit for employees.
    If multiple ranges are selected, returns max upper bound.
    """
    if value is None or _is_all(value):
        return None

    if not isinstance(value, list):
        value = [value]

    limits: List[int] = []
    for item in value:
        s = _normalize_dash(clean_str(item)).upper()
        if not s or s == "ALL":
            continue

        if "-" in s:
            lo, hi = [x.strip() for x in s.split("-", 1)]
            if not lo.isdigit() or not hi.isdigit():
                raise HTTPException(status_code=400, detail="Invalid headcount range.")
            lo_i, hi_i = int(lo), int(hi)
            if lo_i <= 0 or lo_i > hi_i:
                raise HTTPException(status_code=400, detail="Invalid headcount range.")
            limits.append(hi_i)
        else:
            if not s.isdigit() or int(s) <= 0:
                raise HTTPException(status_code=400, detail="Invalid headcount value.")
            limits.append(int(s))

    return max(limits) if limits else None

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


def validate_years_months(payload: Any, db: Session = None) -> List[Tuple[int, int]]:
    """
    Returns allowed (year, month) pairs.
    Works with Pydantic payloads (no payload.get).
    Enforces YYYY year format.
    Supports ZIP pairing if len(years)==len(months), else cartesian product.

    Behavior:
    - If user selects one or more years and no months -> expands to all months for each year.
      * Non-current years -> Jan..Dec.
      * Current year -> Jan..<current month> (prevents future months by default).
      Toggle below if you want all 12 months even for the current year.
    """
    payload_dict = _payload_to_dict(payload)

    today = date.today()

    years = _coerce_int_list(payload_dict.get("years", []) or [], "years", four_digit_year=True)
    months = _coerce_int_list(payload_dict.get("months", []) or [], "months")

    years = [y for y in years if y != 0]
    months = [m for m in months if m != 0]

    if months and not years:
        raise HTTPException(status_code=400, detail="Year is mandatory when month is selected.")

    bad_months = [m for m in months if m < 1 or m > 12]
    if bad_months:
        raise HTTPException(status_code=400, detail=f"Invalid month(s): {bad_months}. Months must be 1-12.")

    if years:
        future_years = [y for y in years if y > today.year]
        if future_years:
            raise HTTPException(status_code=400, detail=f"Future year(s) not allowed: {sorted(set(future_years))}")

        years_ordered = list(years)

        if not months:
            pairs: List[Tuple[int, int]] = []
            for y in years_ordered:
               
                max_month = today.month if y == today.year else 12
               
                for m in range(1, max_month + 1):
                    pairs.append((y, m))
            return sorted(set(pairs))


        if len(months) == len(years_ordered):
            pairs: List[Tuple[int, int]] = []
            for y, m in zip(years_ordered, months):
                if y == today.year and m > today.month:
                    raise HTTPException(status_code=400, detail=f"Future month {m} not allowed for {today.year}.")
                pairs.append((y, m))
            return sorted(set(pairs))

      
        pairs2: List[Tuple[int, int]] = []
        for y in sorted(set(years_ordered)):
            max_month = today.month if y == today.year else 12
            future_months = [m for m in months if m > max_month]
            if future_months:
                raise HTTPException(
                    status_code=400,
                    detail=f"Future month(s) not allowed for {y}: {sorted(set(future_months))}"
                )
            for m in months:
                pairs2.append((y, m))
        return sorted(set(pairs2))

   
    if db:
        latest = db.query(func.max(ShiftAllowances.duration_month)).scalar()
        if latest:
            current_exists = (
                db.query(func.count(ShiftAllowances.id))
                .filter(extract("year", ShiftAllowances.duration_month) == today.year)
                .filter(extract("month", ShiftAllowances.duration_month) == today.month)
                .scalar()
            )
            if current_exists and int(current_exists) > 0:
                return [(today.year, today.month)]
            return _last_n_month_pairs(latest.year, latest.month, n=12)

    return [(today.year, today.month)]


def get_previous_month_allowance(db: Session, base_filters, year: int, month: int) -> float:
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
            & (func.upper(func.trim(ShiftMapping.shift_type)) == func.upper(func.trim(ShiftsAmountAlias.shift_type)))
        )
        .filter(*base_filters)
        .filter(extract("year", ShiftAllowances.duration_month) == py)
        .filter(extract("month", ShiftAllowances.duration_month) == pm)
        .scalar()
    )
    return float(total or 0.0)


def get_previous_month_unique_clients(db: Session, base_filters, year: int, month: int) -> int:
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
    """
    Dashboard summary with sorting + fixed payload handling.
    sort_by: client | client_partner | departments | headcount | total_allowance
    sort_order: default | asc | desc
    default => no sorting
    """
 
    validate_shifts(payload)
    validate_headcounts(payload)

    payload_dict = _payload_to_dict(payload)

    sort_by = parse_sort_by(payload_dict.get("sort_by", ""))
    sort_order = parse_sort_order(payload_dict.get("sort_order", "default"))

    clients_list = _normalize_to_list(payload_dict.get("clients", "ALL"))
    depts_list = _normalize_to_list(payload_dict.get("departments", "ALL"))

    base_filters: List[Any] = []
    if clients_list:
        base_filters.append(
            func.lower(func.trim(ShiftAllowances.client)).in_([c.lower() for c in clients_list])
        )
    if depts_list:
        base_filters.append(
            func.lower(func.trim(ShiftAllowances.department)).in_([d.lower() for d in depts_list])
        )

    pairs = sorted(set(validate_years_months(payload, db=db) or []))
    if not pairs:
        return {
            "summary": {},
            "dashboard": {"clients": {}, "client_partner": {}, "total_allowance": 0.0, "head_count": 0}
        }

    selected_shifts = parse_shifts(payload_dict.get("shifts", None))
    hc_limit = parse_headcount_limit(payload_dict.get("headcounts", None))

    ShiftsAmountAlias = aliased(ShiftsAmount)

    yr_month_filters = [
        and_(
            extract("year", ShiftAllowances.duration_month) == y,
            extract("month", ShiftAllowances.duration_month) == m
        )
        for y, m in pairs
    ]

 
    def build_employee_limit_subquery(limit_n: int):
        if not limit_n:
            return None

        allowance_expr = func.coalesce(ShiftMapping.days, 0) * func.coalesce(ShiftsAmountAlias.amount, 0)

        q = (
            db.query(
                ShiftAllowances.emp_id.label("emp_id"),
                func.coalesce(func.sum(allowance_expr), 0).label("emp_allowance"),
            )
            .select_from(ShiftAllowances)
            .join(ShiftMapping, ShiftMapping.shiftallowance_id == ShiftAllowances.id)
            .outerjoin(
                ShiftsAmountAlias,
                (extract("year", ShiftAllowances.duration_month) == cast(ShiftsAmountAlias.payroll_year, Integer))
                & (func.upper(func.trim(ShiftMapping.shift_type)) == func.upper(func.trim(ShiftsAmountAlias.shift_type)))
            )
            .filter(*base_filters)
            .filter(or_(*yr_month_filters))
        )

        if selected_shifts:
            q = q.filter(func.upper(func.trim(ShiftMapping.shift_type)).in_(selected_shifts))

        return (
            q.group_by(ShiftAllowances.emp_id)
            .order_by(desc("emp_allowance"), ShiftAllowances.emp_id)
            .limit(limit_n)
            .subquery()
        )

    emp_limit_sq = build_employee_limit_subquery(hc_limit)

    rows_q = (
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
            (extract("year", ShiftAllowances.duration_month) == cast(ShiftsAmountAlias.payroll_year, Integer))
            & (func.upper(func.trim(ShiftMapping.shift_type)) == func.upper(func.trim(ShiftsAmountAlias.shift_type)))
        )
        .filter(*base_filters)
        .filter(or_(*yr_month_filters))
    )

    if emp_limit_sq is not None:
        rows_q = rows_q.filter(ShiftAllowances.emp_id.in_(db.query(emp_limit_sq.c.emp_id)))

    rows = rows_q.all()
    if not rows:
        return {
            "summary": {},
            "dashboard": {"clients": {}, "client_partner": {}, "total_allowance": 0.0, "head_count": 0}
        }

    def empty_node() -> Dict[str, Any]:
        base = {
            "total_allowance": 0.0,
            "head_count": set(),  
            "dept_set": set(),    
        }
        
        for s in SHIFT_TYPES:
            base[s] = {"total": 0.0, "head_count": set()}
        return base

    dashboard: Dict[str, Any] = {
        "total_allowance": 0.0,
        "head_count_set": set(),  
        "clients": {},
        "client_partner": {},
    }

    clients_set, depts_set = set(), set()
    total_allowance = 0.0
    seen_shifts: Set[str] = set()

    for emp, client, dept, cp, shift, days, amt in rows:
        shift = clean_str(shift).upper()
       
        if SHIFT_TYPES and shift not in SHIFT_TYPES:
            continue
        if selected_shifts and shift not in selected_shifts:
            continue

        
        if shift:
            seen_shifts.add(shift)

        client = clean_str(client) or "UNKNOWN"
        dept = clean_str(dept) or "UNKNOWN"
        cp_name = clean_str(cp) or "Unassigned"
        eid = clean_str(emp)

        allowance = float(days or 0) * float(amt or 0)
        total_allowance += allowance

        if eid:
            dashboard["head_count_set"].add(eid)

        clients_set.add(client)
        depts_set.add(dept)

       
        c = dashboard["clients"].setdefault(client, empty_node())
        c["total_allowance"] += allowance
        if eid:
            c["head_count"].add(eid)
        c["dept_set"].add(dept)
        if shift not in c:
            c[shift] = {"total": 0.0, "head_count": set()}
        c[shift]["total"] += allowance
        if eid:
            c[shift]["head_count"].add(eid)

      
        a = dashboard["client_partner"].setdefault(cp_name, empty_node())
        a["total_allowance"] += allowance
        if eid:
            a["head_count"].add(eid)
        a["dept_set"].add(dept)
        if shift not in a:
            a[shift] = {"total": 0.0, "head_count": set()}
        a[shift]["total"] += allowance
        if eid:
            a[shift]["head_count"].add(eid)

        
        a_client = a.setdefault("clients", {}).setdefault(client, empty_node())
        a_client["total_allowance"] += allowance
        if eid:
            a_client["head_count"].add(eid)
        a_client["dept_set"].add(dept)
        if shift not in a_client:
            a_client[shift] = {"total": 0.0, "head_count": set()}
        a_client[shift]["total"] += allowance
        if eid:
            a_client[shift]["head_count"].add(eid)

    def finalize(node: Dict[str, Any], all_shift_keys: Set[str], *, add_shift_buckets: bool = True, add_departments: bool = True) -> None:
        if "head_count_set" in node:
            node["head_count"] = len(node.pop("head_count_set"))
        else:
            hc = node.get("head_count", set())
            node["head_count"] = len(hc) if isinstance(hc, set) else int(hc or 0)

        if add_departments:
            node["departments"] = len(node.get("dept_set", set()))
        node.pop("dept_set", None)

        if add_shift_buckets:
            shifts_to_finalize = SHIFT_TYPES or all_shift_keys
            for s in shifts_to_finalize:
                if s not in node:
                    node[s] = {"total": 0.0, "head_count": set()}
                hc = node[s].get("head_count", set())
                node[s]["head_count"] = len(hc) if isinstance(hc, set) else int(hc or 0)
                node[s]["total"] = float(node[s].get("total", 0.0) or 0.0)

    
    for cnode in dashboard["clients"].values():
        finalize(cnode, seen_shifts, add_shift_buckets=True, add_departments=True)
    for pnode in dashboard["client_partner"].values():
        finalize(pnode, seen_shifts, add_shift_buckets=True, add_departments=True)
        for cnode in pnode.get("clients", {}).values():
            finalize(cnode, seen_shifts, add_shift_buckets=True, add_departments=True)

    finalize(dashboard, seen_shifts, add_shift_buckets=False, add_departments=False)
   
   
    if sort_order != "default" and sort_by:
        dashboard["clients"] = apply_sort_dict_dashboard(dashboard["clients"], sort_by, sort_order)
        dashboard["client_partner"] = apply_sort_dict_dashboard(dashboard["client_partner"], sort_by, sort_order)
        for _, pnode in dashboard["client_partner"].items():
            if "clients" in pnode:
                pnode["clients"] = apply_sort_dict_dashboard(pnode["clients"], sort_by, sort_order)

   
    top = payload_dict.get("top", "ALL")
    if top != "ALL" and str(top).isdigit():
        top_n = int(top)
        dashboard["clients"] = dict(list(dashboard["clients"].items())[:top_n])
        dashboard["client_partner"] = dict(list(dashboard["client_partner"].items())[:top_n])

    
    latest_y, latest_m = pairs[-1]

    previous_total = get_previous_month_allowance(db, base_filters, latest_y, latest_m)
    prev_y, prev_m = _previous_year_month(latest_y, latest_m)
    prev_prev_total = get_previous_month_allowance(db, base_filters, prev_y, prev_m)

    previous_clients_count = get_previous_month_unique_clients(db, base_filters, latest_y, latest_m)
    previous_departments_count = get_previous_month_unique_departments(db, base_filters, latest_y, latest_m)
    previous_head_count = get_previous_month_unique_employees(db, base_filters, latest_y, latest_m)

    def calc_change(curr, prev):
        if not prev:
            return "N/A"
        try:
            pct = round(((curr - prev) / prev) * 100, 2)
        except ZeroDivisionError:
            return "N/A"
        if pct > 0:
            return f"{pct}% increase"
        if pct < 0:
            return f"{abs(pct)}% decrease"
        return "0% no change"

    dashboard["total_allowance"] = round(total_allowance, 2)

    summary = {
        "total_clients": len(clients_set),
        "total_clients_last_month": calc_change(len(clients_set), previous_clients_count),

        "total_departments": len(depts_set),
        "total_departments_last_month": calc_change(len(depts_set), previous_departments_count),

        "head_count": dashboard["head_count"],
        "head_count_last_month": calc_change(dashboard["head_count"], previous_head_count),

        "total_allowance": round(total_allowance, 2),
        "total_allowance_last_month": calc_change(round(total_allowance, 2), previous_total),

        "previous_month_allowance": previous_total,
        "previous_month_allowance_last_month": calc_change(previous_total, prev_prev_total),
    }

    return {"summary": summary, "dashboard": dashboard}

SHIFT_KEYS: List[str] = [str(k).strip().upper() for k in get_all_shift_keys()]
SHIFT_KEY_SET: Set[str] = set(SHIFT_KEYS)


def clean_str(value: Any) -> str:
    """Normalize strings (handles None, whitespace, quote-only)."""
    if value is None:
        return ""
    s = value.strip() if isinstance(value, str) else str(value).strip()
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
                    detail="Invalid year. Year must be in YYYY format (e.g., 2024)."
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
    """ALL -> None; string -> [string]; list -> list"""
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
    """ALL -> None; string -> [string]; list -> list"""
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
    """ALL -> None; else validate shift keys."""
    if _is_all(value):
        return None
    if isinstance(value, str):
        v = clean_str(value).upper()
        if v not in SHIFT_KEY_SET:
            raise HTTPException(400, f"Invalid shift type: {v}")
        return {v}
    if isinstance(value, list):
        out: Set[str] = set()
        for x in value:
            v = clean_str(x).upper()
            if not v:
                continue
            if v not in SHIFT_KEY_SET:
                raise HTTPException(400, f"Invalid shift type: {v}")
            out.add(v)
        return out or None
    raise HTTPException(400, "shifts must be 'ALL', string, or list.")


def parse_top(value: Any) -> Optional[int]:
    """ALL -> None; numeric -> int"""
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
    if v in ("default", "asc", "desc"):
        return v
    return "default"


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


def validate_years_months(payload: dict, db: Session) -> List[Tuple[int, int]]:
    """
    Behavior:
    - If no years and no months (or [0]) -> latest month in DB ONLY
    - months selected -> year mandatory
    - future years blocked
    - future months blocked for current year
    - ZIP pairing when len(years)==len(months) (duplicates supported)
    - Cartesian otherwise
    - If year selected but months not selected -> all months of that year
      (up to current month for current year)
    """
    today = date.today()
    years = _coerce_int_list(payload.get("years", []) or [], "years", four_digit_year=True)
    months = _coerce_int_list(payload.get("months", []) or [], "months")

    years = [y for y in years if y != 0]
    months = [m for m in months if m != 0]

    if not years and not months:
        latest = db.query(func.max(ShiftAllowances.duration_month)).scalar()
        if latest:
            return [(latest.year, latest.month)]
        return [(today.year, today.month)]

    if months and not years:
        raise HTTPException(400, "Year is mandatory when month is selected.")

    bad_months = [m for m in months if m < 1 or m > 12]
    if bad_months:
        raise HTTPException(400, f"Invalid month(s): {bad_months}. Months must be 1-12.")

    future_years = [y for y in years if y > today.year]
    if future_years:
        raise HTTPException(400, f"Future year(s) not allowed: {sorted(set(future_years))}")

    years_ordered = list(years)

    if years and not months:
        pairs: List[Tuple[int, int]] = []
        for y in years_ordered:
            max_month = today.month if y == today.year else 12
            for m in range(1, max_month + 1):
                pairs.append((y, m))
        return sorted(set(pairs))

    if len(years_ordered) == len(months):
        pairs: List[Tuple[int, int]] = []
        for y, m in zip(years_ordered, months):
            max_month = today.month if y == today.year else 12
            if m > max_month:
                raise HTTPException(400, f"Future month {m} not allowed for year {y}.")
            pairs.append((y, m))
        return sorted(set(pairs))

    
    pairs: List[Tuple[int, int]] = []
    for y in sorted(set(years_ordered)):
        max_month = today.month if y == today.year else 12
        future_months = [m for m in months if m > max_month]
        if future_months:
            raise HTTPException(400, f"Future month(s) not allowed for {y}: {sorted(set(future_months))}")
        for m in months:
            pairs.append((y, m))
    return sorted(set(pairs))


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

    If prev==0 and curr>0: percent undefined; return "100% increase" (still matches required format).
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
    """Aggregate: client -> (y,m) -> {emp_set, allow}"""
    out: Dict[str, Dict[Tuple[int, int], Dict[str, Any]]] = {}

    for client, yy, mm, emp_id, stype, days, amt in rows:
        cname = clean_str(client) or "UNKNOWN"
        y, m = int(yy), int(mm)

        eid = clean_str(emp_id)
        st = clean_str(stype).upper()
        if st not in SHIFT_KEY_SET:
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
    Sorting:
      sort_by: client|client_partner|departments|headcount|total_allowance
      sort_order: default|asc|desc
      default => no sorting (keep natural order)

    Drilldown-only keys:
      - headcount_previous_month: "23% increase"/"23% decrease"/"no change"
      - total_allowance_previous_month: "23% increase"/"23% decrease"/"no change"
    """
    payload = payload or {}

    clients_filter = parse_clients(payload.get("clients", "ALL"))
    depts_filter = parse_departments(payload.get("departments", "ALL"))
    shifts_filter = parse_shifts(payload.get("shifts", "ALL"))
    top_n = parse_top(payload.get("top", "ALL"))

    employee_cap = parse_employee_limit(payload.get("headcounts", "ALL"))

    sort_by = parse_sort_by(payload.get("sort_by", ""))
    sort_order = parse_sort_order(payload.get("sort_order", "default"))

    pairs = validate_years_months(payload, db=db)
    periods = [f"{y:04d}-{m:02d}" for y, m in pairs]

    drilldown_client = clients_filter[0] if clients_filter and len(clients_filter) == 1 else None

    base_filters_extra: List[Any] = []
    if clients_filter:
        base_filters_extra.append(func.lower(func.trim(ShiftAllowances.client)).in_([c.lower() for c in clients_filter]))
    if depts_filter:
        base_filters_extra.append(func.lower(func.trim(ShiftAllowances.department)).in_([d.lower() for d in depts_filter]))

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
            "summary": {"total_clients": 0, "departments": 0, "headcount": 0, "total_allowance": 0.0},
            "clients": {},
        }

    client_nodes: Dict[str, Dict[str, Any]] = {}
    partners: Dict[str, Dict[str, Any]] = {}
    employees_global: Dict[str, Dict[str, Any]] = {}

    for emp_id, emp_name, client, dept, cp, stype, days, amt in rows:
        client_name = clean_str(client) or "UNKNOWN"
        dept_name = clean_str(dept) or "UNKNOWN"
        partner_name = clean_str(cp) or "UNKNOWN"
        eid = clean_str(emp_id)

        st = clean_str(stype).upper()
        if st not in SHIFT_KEY_SET:
            continue

        allowance = float(days or 0) * float(amt or 0)

        cnode = client_nodes.setdefault(client_name, {"dept_set": set(), "emp_set": set(), "total_allowance": 0.0})
        cnode["dept_set"].add(dept_name)
        if eid:
            cnode["emp_set"].add(eid)
        cnode["total_allowance"] += allowance

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
            pnode["shift_totals"][st] += allowance

            if not eid:
                continue

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

            pe[st] += allowance
            pe["total_allowance"] += allowance

            eg = employees_global.get(eid)
            if not eg:
                eg = {
                    "emp_id": eid,
                    "emp_name": clean_str(emp_name),
                    "department": dept_name,
                    "client_partner": partner_name,
                    **{k: 0.0 for k in SHIFT_KEYS},
                    "total_allowance": 0.0,
                }
                employees_global[eid] = eg

            eg[st] += allowance
            eg["total_allowance"] += allowance

  
    clients_out: Dict[str, Any] = {}
    for cname, node in client_nodes.items():
        clients_out[cname] = {
            "departments": len(node["dept_set"]),
            "headcount": len(node["emp_set"]),
            "total_allowance": round(float(node["total_allowance"] or 0.0), 2),
        }

    if sort_order != "default" and sort_by:
        clients_out = apply_sort_dict(clients_out, sort_by=("client" if sort_by == "client" else sort_by), sort_order=sort_order)

    clients_out = top_n_dict(clients_out, top_n)

    overall_depts: Set[str] = set()
    overall_emps: Set[str] = set()
    total_allowance_sum = 0.0
    for cname in clients_out.keys():
        node = client_nodes.get(cname)
        if node:
            overall_depts |= set(node["dept_set"])
            overall_emps |= set(node["emp_set"])
            total_allowance_sum += float(node["total_allowance"] or 0)

    result: Dict[str, Any] = {
        "periods": periods,
        "summary": {
            "total_clients": len(clients_out),
            "departments": len(overall_depts),
            "headcount": len(overall_emps),
            "total_allowance": round(total_allowance_sum, 2),
        },
        "clients": clients_out,
    }

   
    if drilldown_client:
        selected_key = next((k for k in client_nodes.keys() if k.lower() == drilldown_client.lower()), drilldown_client)

        if selected_key not in result["clients"] and selected_key in client_nodes:
            node = client_nodes[selected_key]
            result["clients"][selected_key] = {
                "departments": len(node["dept_set"]),
                "headcount": len(node["emp_set"]),
                "total_allowance": round(float(node["total_allowance"] or 0.0), 2),
            }

        trend_y, trend_m = max(pairs)
        candidates = month_back_list(trend_y, trend_m, n=12)

        trend_rows = _query_allowance_rows(db, [(trend_y, trend_m)], base_filters_extra, shifts_filter)
        trend_by_client_period = _aggregate_client_period(trend_rows)

        baseline_rows = _query_allowance_rows(db, candidates, base_filters_extra, shifts_filter)
        baseline_by_client_period = _aggregate_client_period(baseline_rows)
        baselines = _pick_nearest_baseline(baseline_by_client_period, candidates)

        trend_client_key = next((k for k in trend_by_client_period.keys() if k.lower() == selected_key.lower()), selected_key)
        baseline_client_key = next((k for k in baselines.keys() if k.lower() == selected_key.lower()), selected_key)

        t_periods = trend_by_client_period.get(trend_client_key, {})
        t_node = t_periods.get((trend_y, trend_m), {"emp_set": set(), "allow": 0.0})
        curr_hc = len(t_node["emp_set"])
        curr_allow = float(t_node["allow"] or 0.0)

        b = baselines.get(baseline_client_key, {"headcount": 0, "allow": 0.0})
        prev_hc = int(b["headcount"])
        prev_allow = float(b["allow"])

        result["clients"][selected_key].update({
            "headcount_previous_month": fmt_change(curr_hc, prev_hc),
            "total_allowance_previous_month": fmt_change(curr_allow, prev_allow),
        })

        
        employees_global_list = sorted(employees_global.values(), key=lambda e: e.get("total_allowance", 0.0), reverse=True)
        if employee_cap:
            employees_global_list = employees_global_list[:employee_cap]
        top_emp_ids = {e["emp_id"] for e in employees_global_list}

        client_partners_detailed: Dict[str, Any] = {}
        for pname, pnode in partners.items():
            emps = [e for eid, e in pnode["employees"].items() if eid in top_emp_ids]
            emps.sort(key=lambda e: e.get("total_allowance", 0.0), reverse=True)

            client_partners_detailed[pname] = {
                "departments": len(pnode["dept_set"]),
                "headcount": len(pnode["emp_set"]),
                "shift_allowances": {k: round(v, 2) for k, v in pnode["shift_totals"].items()},
                "total_allowance": round(pnode["total_allowance"], 2),
                "employees": emps,
            }

        if sort_order != "default" and sort_by:
            if sort_by == "client_partner":
                client_partners_detailed = apply_sort_dict(client_partners_detailed, "client_partner", sort_order)
            elif sort_by in ("departments", "headcount", "total_allowance"):
                client_partners_detailed = apply_sort_dict(client_partners_detailed, sort_by, sort_order)

        result["clients"][selected_key] = {
            **result["clients"][selected_key],
            "client_partner_count": len(partners),
            "client_partners": client_partners_detailed,
        }

    return result