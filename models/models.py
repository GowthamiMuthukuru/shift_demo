from sqlalchemy import (
    Column, Integer, String, Text, TIMESTAMP, Numeric, func, ForeignKey,UniqueConstraint
)
from sqlalchemy.orm import relationship
from datetime import datetime
from db import Base


# USERS TABLE
class Users(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String(100), unique=True, nullable=False)
    email = Column(String(150), unique=True, nullable=False)
    password_hash = Column(Text, nullable=False)

    created_at = Column(TIMESTAMP, server_default=func.now())
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())

    uploaded_files = relationship("UploadedFiles", back_populates="uploader")


# UPLOADED FILES TABLE
class UploadedFiles(Base):
    __tablename__ = "uploaded_files"

    id = Column(Integer, primary_key=True, index=True)
    filename = Column(String(255), nullable=False)
    uploaded_by = Column(Integer, ForeignKey("users.id", ondelete="SET NULL"))
    uploaded_at = Column(TIMESTAMP, server_default=func.now())
    record_count = Column(Integer, default=0)
    status = Column(String(20), default="processed")  # processing, processed, failed
    payroll_month = Column(String(7), nullable=False)  # MM-YYYY format

    uploader = relationship("Users", back_populates="uploaded_files")


# SHIFT ALLOWANCES TABLE
class ShiftAllowances(Base):
    __tablename__ = "shift_allowances"

    id = Column(Integer, primary_key=True, index=True)

    emp_id = Column(String(50))
    emp_name = Column(String(150))
    grade = Column(String(20))
    department = Column(String(100))
    client = Column(String(100))
    project = Column(String(150))
    project_code = Column(String(50))
    account_manager = Column(String(100))
    practice_lead = Column(String(100))
    delivery_manager = Column(String(100))

    month_year = Column(String(7),nullable=False,default=lambda: datetime.now().strftime("%m-%Y"))
    duration_month = Column(String(7))               # MM-YYYY format
    payroll_month = Column(String(7))                # MM-YYYY format

    shift_a_days = Column(Integer, default=0)
    shift_b_days = Column(Integer, default=0)
    shift_c_days = Column(Integer, default=0)
    prime_days = Column(Integer, default=0)
    total_days = Column(Integer, default=0)

    billability_status = Column(String(50))
    practice_remarks = Column(Text)
    rmg_comments = Column(Text)
    amar_approval = Column(String(50))

    created_at = Column(TIMESTAMP, server_default=func.now())
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())
    __table_args__ = (
    UniqueConstraint('duration_month', 'payroll_month', 'emp_id', name='uix_payroll_employee'),
)



# ALLOWANCE TABLE
class Allowance(Base):
    __tablename__ = "allowance"

    id = Column(Integer, primary_key=True, index=True)
    shift = Column(String(50), nullable=False)
    amount = Column(Numeric(10, 2), nullable=False)
    payroll_month = Column(String(7), nullable=False)  # MM-YYYY format

    created_at = Column(TIMESTAMP, server_default=func.now())
