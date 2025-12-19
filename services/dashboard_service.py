from sqlalchemy.orm import Session
from sqlalchemy import extract
from datetime import datetime,date
from decimal import Decimal
from fastapi import HTTPException
from models.models import ShiftAllowances, ShiftsAmount, ShiftMapping
from dateutil.relativedelta import relativedelta
from sqlalchemy import func,extract,Integer,or_
from typing import List
from utils.client_enums import Company
from collections import defaultdict
from schemas.dashboardschema import DashboardFilterRequest


def validate_month_format(month: str):
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

def get_horizontal_bar_service(db: Session, start_month: str | None, end_month: str | None, top: int | None):
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
    clients = db.query(ShiftAllowances.client).distinct().all()
    client_list = [c[0] for c in clients if c[0]]
    return {"clients": client_list}


def get_piechart_shift_summary(
    db: Session,
    start_month: str | None,
    end_month: str | None,
    top: str | None
):
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
    for key, info in combined.items():
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

MONTH_MAP = {
    "January": 1, "February": 2, "March": 3, "April": 4,
    "May": 5, "June": 6, "July": 7, "August": 8,
    "September": 9, "October": 10, "November": 11, "December": 12
}

QUARTER_MAP = {
    "Q1": [1, 2, 3],
    "Q2": [4,5,6],
    "Q3": [7,8,9],
    "Q4": [10,11,12]
}

SHIFT_TYPES = ["A", "B", "C", "PRIME"]


def get_client_dashboard_summary(db: Session, payload: DashboardFilterRequest):

    if payload.start_month and payload.selected_year:
        raise HTTPException(
            status_code=400,
            detail="Use either month range OR year-based filters, not both"
        )

    filters = []

    if payload.start_month:
        start = date.fromisoformat(payload.start_month + "-01")
        if payload.end_month:
            end = date.fromisoformat(payload.end_month + "-01")
            if payload.end_month and start > end:
                raise HTTPException(
                    status_code=400,
                    detail="start_month must be less than or equal to end_month"
                )
            filters.append(ShiftAllowances.duration_month.between(start, end))
        else:
            filters.append(ShiftAllowances.duration_month == start)

    elif payload.selected_year:
        filters.append(
            extract("year", ShiftAllowances.duration_month) == payload.selected_year
        )

        if payload.selected_months:
            month_nums = [int(m) for m in payload.selected_months]
            filters.append(
                extract("month", ShiftAllowances.duration_month).in_(month_nums)
            )

        if payload.selected_quarters:
            quarter_months = set()
            for q in payload.selected_quarters:
                quarter_months.update(QUARTER_MAP[q])
            filters.append(
                extract("month", ShiftAllowances.duration_month).in_(quarter_months)
            )

    q = (
        db.query(
            ShiftAllowances.emp_id,
            ShiftAllowances.client,
            ShiftAllowances.department,
            ShiftAllowances.account_manager,
            ShiftMapping.shift_type,
            ShiftMapping.days,
            ShiftsAmount.amount
        )
        .join(ShiftMapping, ShiftMapping.shiftallowance_id == ShiftAllowances.id)
        .join(
            ShiftsAmount,
            func.extract("year", ShiftAllowances.duration_month)
            == func.cast(ShiftsAmount.payroll_year, Integer)
        )
        .filter(ShiftMapping.shift_type == ShiftsAmount.shift_type)
    )

    if payload.clients != "ALL":
        conditions = []
        for client, depts in payload.clients.items():
            conditions.append(
                (ShiftAllowances.client == client) &
                (ShiftAllowances.department.in_(depts))
            )
        q = q.filter(or_(*conditions))

    if filters:
        q = q.filter(*filters)

    rows = q.all()
    if not rows:
        return {"dashboard": {}}

    dashboard = {
        "total_allowance": 0,
        "head_count": set(),
        **{f"shift_{s}": {"total": 0, "head_count": set()} for s in SHIFT_TYPES},
        "clients": {},
        "account_manager": {}
    }

    for emp_id, client, dept, account_manager, shift, days, amount in rows:
        allowance = float(days) * float(amount)
        am = account_manager or "Unknown"

        dashboard["total_allowance"] += allowance
        dashboard["head_count"].add(emp_id)
        dashboard[f"shift_{shift}"]["total"] += allowance
        dashboard[f"shift_{shift}"]["head_count"].add(emp_id)

        client_data = dashboard["clients"].setdefault(client, {
            "total_allowance": 0,
            "head_count": set(),
            **{f"shift_{s}": {"total": 0, "head_count": set()} for s in SHIFT_TYPES},
            "department": {}
        })

        client_data["total_allowance"] += allowance
        client_data["head_count"].add(emp_id)
        client_data[f"shift_{shift}"]["total"] += allowance
        client_data[f"shift_{shift}"]["head_count"].add(emp_id)

        dept_data = client_data["department"].setdefault(dept, {
            "total_allowance": 0,
            "head_count": set(),
            **{f"shift_{s}": {"total": 0, "head_count": set()} for s in SHIFT_TYPES},
        })

        dept_data["total_allowance"] += allowance
        dept_data["head_count"].add(emp_id)
        dept_data[f"shift_{shift}"]["total"] += allowance
        dept_data[f"shift_{shift}"]["head_count"].add(emp_id)

        am_data = dashboard["account_manager"].setdefault(am, {
            "total_allowance": 0,
            "head_count": set(),
            **{f"shift_{s}": {"total": 0, "head_count": set()} for s in SHIFT_TYPES},
            "clients": {}
        })

        am_data["total_allowance"] += allowance
        am_data["head_count"].add(emp_id)
        am_data[f"shift_{shift}"]["total"] += allowance
        am_data[f"shift_{shift}"]["head_count"].add(emp_id)

        am_client = am_data["clients"].setdefault(client, {
            "total_allowance": 0,
            "head_count": set(),
            **{f"shift_{s}": {"total": 0, "head_count": set()} for s in SHIFT_TYPES},
            "department": {}
        })

        am_client["total_allowance"] += allowance
        am_client["head_count"].add(emp_id)
        am_client[f"shift_{shift}"]["total"] += allowance
        am_client[f"shift_{shift}"]["head_count"].add(emp_id)

        am_dept = am_client["department"].setdefault(dept, {
            "total_allowance": 0,
            "head_count": set(),
            **{f"shift_{s}": {"total": 0, "head_count": set()} for s in SHIFT_TYPES},
        })

        am_dept["total_allowance"] += allowance
        am_dept["head_count"].add(emp_id)
        am_dept[f"shift_{shift}"]["total"] += allowance
        am_dept[f"shift_{shift}"]["head_count"].add(emp_id)

    def finalize(node):
        node["head_count"] = len(node["head_count"])
        for s in SHIFT_TYPES:
            node[f"shift_{s}"]["head_count"] = len(node[f"shift_{s}"]["head_count"])

    finalize(dashboard)

    for c in dashboard["clients"].values():
        finalize(c)
        for d in c["department"].values():
            finalize(d)

    for am in dashboard["account_manager"].values():
        finalize(am)
        for c in am["clients"].values():
            finalize(c)
            for d in c["department"].values():
                finalize(d)

    if payload.top != "ALL":
        top_n = int(payload.top)
        dashboard["clients"] = dict(
            sorted(
                dashboard["clients"].items(),
                key=lambda x: x[1]["total_allowance"],
                reverse=True
            )[:top_n]
        )

    return {"dashboard": dashboard}