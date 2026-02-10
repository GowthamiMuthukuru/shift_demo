from enum import Enum

class ExcelColumnMap(Enum):
    """Excel to DB column header mapping"""

    emp_id = "Emp ID"
    emp_name = "Emp Name"
    grade = "Grade"
    current_status = "Current Status(e)"
    department = "Department"
    client = "Client"
    project = "Project"
    project_code = "Project Code"
    client_partner = "Client Partner"

    practice_lead = "Practice Lead/ Head"
    delivery_manager = "Delivery/ Project Manager"

    duration_month = "Duration Month"
    payroll_month = "Payroll Month"

    billability_status = "Billability Status"
    practice_remarks = "Practice Remarks"
    rmg_comments = "RMG Comments"

    ANZ = "ANZ – Australia New Zealand\n(3 AM - 12 PM)\nINR 500"
    PST_MST = "PST/MST\n(07 PM - 06 AM)\nINR 700"
    SG = "SG - Singapore\n(6 AM - 3 PM)\nINR 100"
    US_INDIA = "US/India\n(04 PM - 01 AM)\nINR 300"

   
    total_days = "TOTAL DAYS"