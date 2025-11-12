from sqlalchemy.orm import Session
from models.models import ShiftAllowances
 
def get_client_shift_summary(db: Session):
    """Fetch and summarize employee and shift data by client"""
 
    data = db.query(ShiftAllowances).all()
    if not data:
        return []
 
    summary = {}
 
    for row in data:
        client = row.client or "Unknown"
 
        if client not in summary:
            summary[client] = {
                "employees": set(),
                "shift_a": 0,
                "shift_b": 0,
                "shift_c": 0,
                "prime": 0,
            }
 
        summary[client]["employees"].add(row.emp_id)
        summary[client]["shift_a"] += row.shift_a_days or 0
        summary[client]["shift_b"] += row.shift_b_days or 0
        summary[client]["shift_c"] += row.shift_c_days or 0
        summary[client]["prime"] += row.prime_days or 0
 
    # Convert to a clean list for JSON response
    result = [
        {
            "client": client,
            "total_employees": len(info["employees"]),
            "shift_a_days": info["shift_a"],
            "shift_b_days": info["shift_b"],
            "shift_c_days": info["shift_c"],
            "prime_days": info["prime"],
        }
        for client, info in summary.items()
    ]
 
    return result