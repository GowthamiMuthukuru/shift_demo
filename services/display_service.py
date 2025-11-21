from fastapi import HTTPException
from sqlalchemy.orm import Session
from models.models import ShiftAllowances, ShiftsAmount

def partial_update_shift(db: Session, record_id: int, updates: dict):
    allowed_fields = ["shift_a_days", "shift_b_days", "shift_c_days", "prime_days"]
 
    invalid_fields = [k for k in updates.keys() if k not in allowed_fields]
    if invalid_fields:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid fields: {invalid_fields}. Only {allowed_fields} are allowed."
        )
 
    record = db.query(ShiftAllowances).filter(ShiftAllowances.id == record_id).first()
    if not record:
        raise HTTPException(status_code=404, detail=f"Record with ID {record_id} not found")
 
    for key, value in updates.items():
        try:
            setattr(record, key, int(value))
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid value for '{key}', expected integer")
 
    shift_a = record.shift_a_days or 0
    shift_b = record.shift_b_days or 0
    shift_c = record.shift_c_days or 0
    prime = record.prime_days or 0
    record.total_days = shift_a + shift_b + shift_c + prime
 
    db_rates = {a.shift: float(a.amount) for a in db.query(ShiftsAmount).all()}
    rates = {
        "shift_a_days": 500,
        "shift_b_days": 350,
        "shift_c_days": 100,
        "prime_days": 700,
        **db_rates,
    }
 
    record.shift_a_allowance = shift_a * rates.get("shift_a_days", 0)
    record.shift_b_allowance = shift_b * rates.get("shift_b_days", 0)
    record.shift_c_allowance = shift_c * rates.get("shift_c_days", 0)
    record.prime_allowance = prime * rates.get("prime_days", 0)
    record.total_days_allowance = (
        record.shift_a_allowance +
        record.shift_b_allowance +
        record.shift_c_allowance +
        record.prime_allowance
    )
 
    db.commit()
    db.refresh(record)
    return record