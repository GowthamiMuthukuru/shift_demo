"""Service for generating department-wise shift allowance summaries."""

from sqlalchemy.orm import Session
from sqlalchemy import func, case
from models.models import ShiftAllowances, ShiftsAmount, ShiftMapping

def get_department_summary(db: Session, month: str):
    """Return department-wise, client-wise, and employee-wise allowance summary for a given month."""
    query = (
        db.query(
            ShiftAllowances.department,
            ShiftAllowances.client,
            ShiftAllowances.emp_id,
            ShiftAllowances.emp_name,
            func.sum(
                case((ShiftMapping.shift_type == "A", ShiftsAmount.amount), else_=0))
                .label("shift_a_amount"),
            func.sum(
                case((ShiftMapping.shift_type == "B", ShiftsAmount.amount), else_=0))
                .label("shift_b_amount"),
            func.sum(
                case((ShiftMapping.shift_type == "C", ShiftsAmount.amount), else_=0))
                .label("shift_c_amount"),
            func.sum(
                case((ShiftMapping.shift_type == "PRIME", ShiftsAmount.amount), else_=0))
                .label("prime_amount"),
            func.sum(ShiftsAmount.amount).label("total_allowance")
        )
        .join(ShiftMapping, ShiftMapping.shiftallowance_id == ShiftAllowances.id)
        .join(ShiftsAmount, ShiftsAmount.shift_type == ShiftMapping.shift_type)
        .filter(func.to_char(ShiftAllowances.duration_month, 'YYYY-MM') == month)
        .group_by(
            ShiftAllowances.department,
            ShiftAllowances.client,
            ShiftAllowances.emp_id,
            ShiftAllowances.emp_name
        )
        .order_by(ShiftAllowances.department, ShiftAllowances.client)
    )

    results = query.all()

    department_data = {}
    grand_totals = {
        "grand_employee_count": 0,
        "grand_shift_a": 0,
        "grand_shift_b": 0,
        "grand_shift_c": 0,
        "grand_prime": 0,
        "grand_total_allowances": 0
    }

    for row in results:
        if row.department not in department_data:
            department_data[row.department] = {
                "department": row.department,
                "department_employee_count": 0,       # ADDED HERE
                "clients": {},
                "department_total_shift_a": 0,
                "department_total_shift_b": 0,
                "department_total_shift_c": 0,
                "department_total_prime": 0,
                "department_total_allowances": 0
            }

        dept = department_data[row.department]

        # Client creation
        if row.client not in dept["clients"]:
            dept["clients"][row.client] = {
                "client": row.client,
                "employee_count": 0,
                "employees": [],
                "client_total_shift_a": 0,
                "client_total_shift_b": 0,
                "client_total_shift_c": 0,
                "client_total_prime": 0,
                "client_total_allowances": 0
            }

        client_group = dept["clients"][row.client]

        # Update employee count
        client_group["employee_count"] += 1
        dept["department_employee_count"] += 1   # DEPARTMENT COUNT UPDATE

        # Add employee details
        client_group["employees"].append({
            "emp_id": row.emp_id,
            "emp_name": row.emp_name,
            "shift_a_amount": row.shift_a_amount,
            "shift_b_amount": row.shift_b_amount,
            "shift_c_amount": row.shift_c_amount,
            "prime_amount": row.prime_amount,
            "total_allowance": row.total_allowance
        })

        # Update totals
        client_group["client_total_shift_a"] += row.shift_a_amount
        client_group["client_total_shift_b"] += row.shift_b_amount
        client_group["client_total_shift_c"] += row.shift_c_amount
        client_group["client_total_prime"] += row.prime_amount
        client_group["client_total_allowances"] += row.total_allowance

        dept["department_total_shift_a"] += row.shift_a_amount
        dept["department_total_shift_b"] += row.shift_b_amount
        dept["department_total_shift_c"] += row.shift_c_amount
        dept["department_total_prime"] += row.prime_amount
        dept["department_total_allowances"] += row.total_allowance

        # Grand totals
        grand_totals["grand_employee_count"] += 1
        grand_totals["grand_shift_a"] += row.shift_a_amount
        grand_totals["grand_shift_b"] += row.shift_b_amount
        grand_totals["grand_shift_c"] += row.shift_c_amount
        grand_totals["grand_prime"] += row.prime_amount
        grand_totals["grand_total_allowances"] += row.total_allowance

    return {
        "departments": [
            {
                **dept,
                "clients": list(dept["clients"].values())
            }
            for dept in department_data.values()
        ],
        "grand_totals": grand_totals
    }
