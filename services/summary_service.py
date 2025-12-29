"""
Client shift summary service.

This module provides functionality to generate a summarized view of
shift allowance data for clients, grouped by account manager and client,
for a specific month.

If no month is provided, the service automatically selects the most
recent available month from the database. Optional filtering by
account manager is supported, with strict validation rules.
"""

import re
from datetime import datetime
from decimal import Decimal
from sqlalchemy.orm import Session
from sqlalchemy import extract
from fastapi import HTTPException
from models.models import ShiftAllowances, ShiftsAmount

def get_client_shift_summary(db: Session,
                             duration_month: str | None = None,
                             account_manager: str | None = None):
    """
    Generate a client-wise shift summary for a given month.

    The summary is grouped by account manager and client, and includes
    employee count, shift-wise day totals, total days worked, and total
    allowance amount.

    If `duration_month` is not provided, the most recent available month
    (current or previous) is automatically selected from the database.

    Args:
        db (Session): Active SQLAlchemy database session.
        duration_month (str | None): Target month in YYYY-MM format.
        account_manager (str | None): Optional account manager filter.

    Returns:
        dict: A mapping of YYYY-MM to a list of client shift summaries.

    Raises:
        HTTPException:
            - 400 for invalid input formats.
            - 404 if no data is found for the given filters.
    """

    # Validate account_manager
    if account_manager:
        if account_manager != account_manager.strip():
            raise HTTPException(status_code=400,
                                detail="Spaces are not allowed at start/end of account_manager")
        if not all(x.isalpha() or x.isspace() for x in account_manager):
            raise HTTPException(status_code=400,
                                detail="Account manager must contain only letters and spaces")
        manager_exists = db.query(ShiftAllowances).filter(
            ShiftAllowances.account_manager == account_manager).first()
        if not manager_exists:
            raise HTTPException(status_code=404,
                                detail=f"Account manager '{account_manager}' not found")

    # Determine duration_month
    if duration_month:
        if " " in duration_month:
            raise HTTPException(status_code=400, detail="Spaces are not allowed in duration_month")
        if not re.match(r"^\d{4}-\d{2}$", duration_month):
            raise HTTPException(status_code=400,
                                detail="Invalid duration_month format. Use YYYY-MM")
        year, month = map(int, duration_month.split("-"))
        month_str = duration_month
    else:
        # No duration_month → pick current month or previous in DB
        current_month = datetime.today().replace(day=1).date()
        query = db.query(ShiftAllowances.duration_month)
        if account_manager:
            query = query.filter(ShiftAllowances.account_manager == account_manager)
        nearest = query.filter(ShiftAllowances.duration_month <= current_month)\
                       .order_by(ShiftAllowances.duration_month.desc()).first()
        if nearest and nearest[0]:
            year, month = nearest[0].year, nearest[0].month
            month_str = nearest[0].strftime("%Y-%m")
        else:
            raise HTTPException(status_code=404,
                                detail="No records found for current or previous months")

    # Fetch records
    query = db.query(ShiftAllowances).filter(
        extract("year", ShiftAllowances.duration_month) == year,
        extract("month", ShiftAllowances.duration_month) == month
    )
    if account_manager:
        query = query.filter(ShiftAllowances.account_manager == account_manager)

    records = query.all()
    if not records:
        raise HTTPException(
    status_code=404,
    detail=(
        f"No records found for duration_month '{month_str}'"
        f"{f' for manager {account_manager}' if account_manager else ''}"
    )
)
    # Get shift rates
    rates = {r.shift_type.upper(): float(r.amount) for r in db.query(ShiftsAmount).all()}

    # Group data
    summary = {}
    for row in records:
        am = row.account_manager or "Unknown"
        client = row.client or "Unknown"

        if am not in summary:
            summary[am] = {}
        if client not in summary[am]:
            summary[am][client] = {
                "employees": set(),
                "shift_a": Decimal(0),
                "shift_b": Decimal(0),
                "shift_c": Decimal(0),
                "prime": Decimal(0),
                "total_allowances": Decimal(0)
            }

        summary[am][client]["employees"].add(row.emp_id)

        for mapping in row.shift_mappings:
            stype = mapping.shift_type.strip().upper()
            days = Decimal(mapping.days or 0)

            if stype == "A":
                summary[am][client]["shift_a"] += days
            elif stype == "B":
                summary[am][client]["shift_b"] += days
            elif stype == "C":
                summary[am][client]["shift_c"] += days
            elif stype == "PRIME":
                summary[am][client]["prime"] += days

            rate = Decimal(str(rates.get(stype, 0)))
            summary[am][client]["total_allowances"] += days * rate

    #  Build response -
    result = []
    for am, clients in summary.items():
        for client, info in clients.items():
            total_days = float(info["shift_a"] + info["shift_b"] + info["shift_c"] + info["prime"])
            result.append({
                "account_manager": am,
                "client": client,
                "total_employees": len(info["employees"]),
                "shift_a_days": float(info["shift_a"]),
                "shift_b_days": float(info["shift_b"]),
                "shift_c_days": float(info["shift_c"]),
                "prime_days": float(info["prime"]),
                "total_days": total_days,
                "total_allowances": float(info["total_allowances"]),
                "duration_month": month_str
            })

    return {month_str: result}
