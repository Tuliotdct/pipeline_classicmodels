SILVER_TABLE_CONFIG = {
    "customers": {
        "strategy": "scd2",
        "primary_key": ["customerNumber"],
    },
    "employees": {
        "strategy": "scd2",
        "primary_key": ["employeenumber"],
    },
    "orders": {
        "strategy": "scd2",
        "primary_key": ["ordernumber"],
    },
    "products": {
        "strategy": "scd2",
        "primary_key": ["productcode"],
    },
    "payments": {
        "strategy": "append",
        "primary_key": ["customernumber", "checknumber"],
    },
    "orderdetails": {
        "strategy": "append",
        "primary_key": ["ordernumber", "productcode"],
    },
    "offices": {
        "strategy": "scd1",
        "primary_key": ["officecode"],
    },
    "productlines": {
        "strategy": "scd1",
        "primary_key": ["productline"],
    },
}
