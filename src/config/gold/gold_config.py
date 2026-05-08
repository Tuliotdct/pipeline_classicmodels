GOLD_TABLE_CONFIG = {
    "order_payment": {
        "description": "Order details joined with orders and payments",
        "query": """
        SELECT
            od.orderNumber,
            od.productCode,
            od.quantityOrdered,
            od.priceEach,
            od.orderLineNumber,
            o.orderDate,
            o.status,
            o.customerNumber,
            p.paymentDate,
            p.amount,
            p.checkNumber
        FROM glue_catalog.silver.orderdetails od
        JOIN glue_catalog.silver.orders o
            ON od.orderNumber = o.orderNumber
            AND o.is_current = true
        JOIN glue_catalog.silver.payments p
            ON o.customerNumber = p.customerNumber
        """
    },
    "products": {
        "description": "Sales summary by product line joining productlines, products, orderdetails, and orders",
        "query": """
        SELECT
            pl.productLine,
            pl.textDescription,
            p.productCode,
            p.productName,
            p.productVendor,
            o.orderNumber,
            o.orderDate,
            od.quantityOrdered,
            od.priceEach,
            od.quantityOrdered * od.priceEach AS line_total
        FROM glue_catalog.silver.productlines pl
        JOIN glue_catalog.silver.products p
            ON pl.productLine = p.productLine
            AND p.is_current = true
        JOIN glue_catalog.silver.orderdetails od
            ON p.productCode = od.productCode
        JOIN glue_catalog.silver.orders o
            ON od.orderNumber = o.orderNumber
            AND o.is_current = true
        """
    },
    "customer": {
        "description": "Customer summary joining customers, orders, orderdetails, and payments",
        "query": """
        SELECT
            c.customerNumber,
            c.customerName,
            c.contactLastName,
            c.contactFirstName,
            c.phone,
            c.addressLine1,
            c.city,
            c.country,
            o.orderNumber,
            o.orderDate,
            o.status,
            od.productCode,
            od.quantityOrdered,
            od.priceEach,
            p.paymentDate,
            p.amount
        FROM glue_catalog.silver.customers c
        LEFT JOIN glue_catalog.silver.orders o
            ON c.customerNumber = o.customerNumber
            AND o.is_current = true
        LEFT JOIN glue_catalog.silver.orderdetails od
            ON o.orderNumber = od.orderNumber
        LEFT JOIN glue_catalog.silver.payments p
            ON c.customerNumber = p.customerNumber
        WHERE c.is_current = true
        """
    },
    "employee": {
        "description": "Employee summary joining employees, customers, orders, and orderdetails",
        "query": """
        SELECT
            e.employeeNumber,
            e.firstName,
            e.lastName,
            e.jobTitle,
            c.customerNumber,
            c.customerName,
            o.orderNumber,
            o.orderDate,
            od.productCode,
            od.quantityOrdered,
            od.priceEach
        FROM glue_catalog.silver.employees e
        LEFT JOIN glue_catalog.silver.customers c
            ON e.employeeNumber = c.salesRepEmployeeNumber
            AND c.is_current = true
        LEFT JOIN glue_catalog.silver.orders o
            ON c.customerNumber = o.customerNumber
            AND o.is_current = true
        LEFT JOIN glue_catalog.silver.orderdetails od
            ON o.orderNumber = od.orderNumber
        WHERE e.is_current = true
        """
    },
    "customer_employee": {
        "description": "Union of customers and employees for a consolidated view",
        "query": """
        SELECT
            'customer' AS entity_type,
            CAST(customerNumber AS STRING) AS entity_id,
            customerName AS entity_name,
            city,
            country,
            NULL AS jobTitle,
            NULL AS fullName
        FROM glue_catalog.silver.customers
        WHERE is_current = true
        UNION ALL
        SELECT
            'employee' AS entity_type,
            CAST(employeeNumber AS STRING) AS entity_id,
            CONCAT(firstName, ' ', lastName) AS entity_name,
            NULL AS city,
            NULL AS country,
            jobTitle,
            CONCAT(firstName, ' ', lastName) AS fullName
        FROM glue_catalog.silver.employees
        WHERE is_current = true
        """
    }
}
