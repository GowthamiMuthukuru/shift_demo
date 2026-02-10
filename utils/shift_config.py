

SHIFT_TYPES = {
    "PST_MST": "PST/MST\n(07 PM - 06 AM)\nINR 700",
    "US_INDIA": "US/India\n(04 PM - 01 AM)\nINR 300",
    "SG": "SG - Singapore\n(06 AM - 03 PM)\nINR 100",
    "ANZ": "ANZ – Australia New Zealand\n(03 AM - 12 PM)\nINR 500",
}

ALLOWANCE_COLUMNS = {
    "PST/ MST Allowances",
    "US/India Allowances",
    "SG – Singapore Allowances",
    "ANZ – Australia New Zealand Allowances",
    "TOTAL DAYS Allowances",
}

def get_shift_string(shift_key: str) -> str:
    return SHIFT_TYPES.get(shift_key.upper())

def get_all_shift_keys() -> list:
    return list(SHIFT_TYPES.keys())

def get_allowance_columns() -> set:
    return set(ALLOWANCE_COLUMNS)