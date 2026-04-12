SILVER_TABLE_CONFIG = {
    "customers": {
        "strategy": "scd2",
        "primary_key": ["customerNumber"],
    },
    "employees": {
        "strategy": "scd2",
        "primary_key": ["employeeNumber"],
    },
    "orders": {
        "strategy": "scd2",
        "primary_key": ["orderNumber"],
    },
    "products": {
        "strategy": "scd2",
        "primary_key": ["productCode"],
    },
    "payments": {
        "strategy": "append",
        "primary_key": ["customerNumber", "checkNumber"],
    },
    "orderdetails": {
        "strategy": "append",
        "primary_key": ["orderNumber", "productCode"],
    },
    "offices": {
        "strategy": "scd1",
        "primary_key": ["officeCode"],
    },
    "productlines": {
        "strategy": "scd1",
        "primary_key": ["productLine"],
    },
}
