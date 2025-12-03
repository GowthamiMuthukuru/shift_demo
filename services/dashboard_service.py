from sqlalchemy.orm import Session
from sqlalchemy import extract
from datetime import datetime
from decimal import Decimal
from fastapi import HTTPException
from models.models import ShiftAllowances, ShiftsAmount
from schemas.dashboardschema import VerticalGraphResponse
from dateutil.relativedelta import relativedelta
from sqlalchemy import func


def validate_month_format(month: str):
    try:
        return datetime.strptime(month + "-01", "%Y-%m-%d").date()
    except:
        raise HTTPException(status_code=400, detail="Invalid month format. Expected YYYY-MM")


def get_horizontal_bar_service(db: Session, start_month: str, end_month: str | None, top: int | None):

    # Validate start month
    start_date = validate_month_format(start_month)

    # Determine query range
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
        # Only one month
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

    # Convert sets to counts and decimals to floats
    result = []
    for client, info in output.items():
        total = len(info["total_unique_employees"])
        result.append({
            "client": client,
            "total_unique_employees": total,
            "A": float(info["A"]),
            "B": float(info["B"]),
            "C": float(info["C"]),
            "PRIME": float(info["PRIME"]),
        })

    # Sort by descending total employees
    result.sort(key=lambda x: x["total_unique_employees"], reverse=True)

    # Apply top filter if provided
    if top is not None:
        if top <= 0:
            raise HTTPException(status_code=400, detail="top must be a positive integer")
        result = result[:top]

    return {"horizontal_bar": result}



def get_graph_service(db: Session, client_name: str):

    if not client_name:
        raise HTTPException(status_code=400, detail="client_name is required")

    current_year = datetime.now().year
    monthly_allowances = {}

    # Fetch shift rate table once
    rate_rows = db.query(ShiftsAmount).filter(ShiftsAmount.payroll_year == str(current_year)).all()
    rates = {r.shift_type.strip().upper(): Decimal(str(r.amount)) for r in rate_rows}

    for month in range(1, 13):
        records = db.query(ShiftAllowances).filter(
            ShiftAllowances.client == client_name,
            extract("year", ShiftAllowances.duration_month) == current_year,
            extract("month", ShiftAllowances.duration_month) == month
        ).all()

        month_key = datetime(1900, month, 1).strftime("%b")

        if not records:
            monthly_allowances[month_key] = 0.0
            continue

        total_amount = Decimal(0)

        for row in records:
            for mapping in row.shift_mappings:
                stype = mapping.shift_type.strip().upper()
                days = Decimal(mapping.days or 0)
                rate = rates.get(stype, Decimal(0))
                total_amount += days * rate

        monthly_allowances[month_key] = float(total_amount)

    return {"graph": monthly_allowances}



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
        if top.lower() == "all":
            top_int = None
        else:
            if not top.isdigit():
                raise HTTPException(400, "top must be a positive integer or 'all'")
            top_int = int(top)
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

        for _ in range(12):  # go back 12 months
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
            return [{"message": "No data found in the last 12 months"}]

    
    elif start_month and not end_month:

        if not validate_month(start_month):
            raise HTTPException(400, "start_month must be in YYYY-MM format")

        months = [start_month]

    else:
        if not validate_month(start_month):
            raise HTTPException(400, "start_month must be in YYYY-MM format")

        if not validate_month(end_month):
            raise HTTPException(400, "end_month must be in YYYY-MM format")

        if end_month < start_month:
            raise HTTPException(400, "end_month cannot be less than start_month")

        months = generate_months(start_month, end_month)

    rate_rows = db.query(ShiftsAmount).all()
    rates = {r.shift_type.upper(): float(r.amount) for r in rate_rows}

    final_response = []

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

        if not records:
            final_response.append({
                "month": m,
                "message": f"No shift data found for month {m}"
            })
            continue

        summary = {}

        for row in records:
            client = row.client or "Unknown"

            if client not in summary:
                summary[client] = {
                    "employees": set(),
                    "shift_a": 0,
                    "shift_b": 0,
                    "shift_c": 0,
                    "prime": 0,
                    "total_allowances": 0
                }

            summary[client]["employees"].add(row.emp_id)

            for mapping in row.shift_mappings:
                stype = mapping.shift_type.upper()
                days = int(mapping.days or 0)

                if stype == "A":
                    summary[client]["shift_a"] += days
                elif stype == "B":
                    summary[client]["shift_b"] += days
                elif stype == "C":
                    summary[client]["shift_c"] += days
                elif stype == "PRIME":
                    summary[client]["prime"] += days

                summary[client]["total_allowances"] += days * rates.get(stype, 0)


        result = []
        for client, info in summary.items():

            total_days = info["shift_a"] + info["shift_b"] + info["shift_c"] + info["prime"]

            result.append({
                "client_name": client,
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

        final_response.append({
            "month": m,
            "clients": result
        })

    return final_response




def get_vertical_bar_service(
    db: Session,
    start_month: str | None = None,
    end_month: str | None = None,
    top: str | None = None
):
    
    if top is None:
        top_int = None 
    else:
        if str(top).lower() == "all":
            top_int = None
        else:
            if not str(top).isdigit():
                raise HTTPException(
                    status_code=400,
                    detail="top must be a positive integer or 'all'"
                )
            top_int = int(top)
            if top_int <= 0:
                raise HTTPException(
                    status_code=400,
                    detail="top must be greater than 0"
                )

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
            return [{"message": "No data found for last 12 months"}]

    elif start_month and not end_month:
        if not validate_month_format(start_month):
            raise HTTPException(status_code=400, detail="start_month must be in YYYY-MM format")
        months = [start_month]

    else:
        if not validate_month_format(start_month):
            raise HTTPException(status_code=400, detail="start_month must be in YYYY-MM format")
        if not validate_month_format(end_month):
            raise HTTPException(status_code=400, detail="end_month must be in YYYY-MM format")
        if end_month < start_month:
            raise HTTPException(status_code=400, detail="end_month cannot be less than start_month")
        months = generate_months_list(start_month, end_month)

    rate_rows = db.query(ShiftsAmount).all()
    rates = {r.shift_type.upper(): float(r.amount) for r in rate_rows}

    final_response = []

    for m in months:
        year, month_num = map(int, m.split("-"))
        records = (
            db.query(ShiftAllowances)
            .filter(
                extract("year", ShiftAllowances.duration_month) == year,
                extract("month", ShiftAllowances.duration_month) == month_num
            )
            .all()
        )

        if not records:
            final_response.append({
                "month": m,
                "message": f"No shift data found for month {m}"
            })
            continue

        summary = {}
        for row in records:
            client = row.client or "Unknown"
            if client not in summary:
                summary[client] = {
                    "total_days": 0,
                    "total_allowances": 0
                }
            for mapping in row.shift_mappings:
                stype = mapping.shift_type.upper()
                days = float(mapping.days or 0)
                summary[client]["total_days"] += days
                summary[client]["total_allowances"] += days * rates.get(stype, 0)

        # Build sorted result
        result = [
            {
                "client_name": c,
                "total_days": info["total_days"],
                "total_allowances": info["total_allowances"]
            }
            for c, info in summary.items()
        ]
        result = sorted(result, key=lambda x: x["total_allowances"], reverse=True)

        if top_int is not None:
            result = result[:top_int]

        final_response.append({
            "month": m,
            "clients": result
        })

    return final_response


