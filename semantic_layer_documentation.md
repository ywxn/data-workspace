# Semantic Layer Documentation

> A comprehensive reference for designing and implementing a semantic layer for data analytics and reporting.

## Overview

A semantic layer provides a business-friendly abstraction over an underlying physical database. It maps raw tables and columns to meaningful business concepts — enabling consistent reporting, natural-language querying, and simplified data access across teams.

---

## Table of Contents

- [Entities](#entities)
- [Dimensions](#dimensions)
- [Measures](#measures)
- [Relationships](#relationships)
- [Date Fields](#date-fields)
- [Common Queries](#common-queries)
- [Example Structure](#example-structure)

---

## Entities

Entities represent core business objects. Each entity maps to one or more physical database tables.

| Entity | Business Name | Type | Description |
|---|---|---|---|
| `order` | Sales/Purchase Order | Transaction | Represents a business order with header and line-item detail tables |
| `receipt` | Goods Receipt | Transaction | Tracks inbound receipt of goods against orders |
| `inventory` | Inventory Summary | Fact | Aggregated stock/inventory balances |
| `product` | Product/Item Master | Master | Reference data for products or materials |
| `vendor` | Vendor/Supplier Master | Master | Reference data for suppliers or vendors |
| `customer` | Customer Master | Master | Reference data for customers |
| `location` | Warehouse/Location Master | Master | Reference data for storage or operational locations |
| `employee` | Employee Master | Master | Reference data for staff or personnel |

### Entity Types

- **Master** — Reference/dimension data (products, vendors, employees, etc.)
- **Transaction** — Business events (orders, receipts, issues, etc.)
- **Fact** — Aggregated/summary data (inventory movements, financial summaries)

### Synonyms

Each entity can have recognized synonyms to support natural-language lookups. For example:

- **order** → `purchase order`, `sales order`, `PO`, `SO`
- **receipt** → `goods receipt`, `inbound`, `receiving`
- **inventory** → `stock`, `stock balance`, `on-hand`

> Store synonyms alongside entity definitions (e.g., in a `synonyms` array in your semantic layer configuration).

---

## Dimensions

Dimensions are the axes by which data can be sliced and filtered.

| Dimension | Description |
|---|---|
| `product` | Filter by product, material, or SKU |
| `vendor` | Filter by vendor or supplier |
| `customer` | Filter by customer or client |
| `location` | Filter by warehouse, store, or site |
| `employee` | Filter by staff member or user |
| `status` | Transaction status (e.g., Open, In-process, Closed) |
| `approval_status` | Approval status (e.g., Approved, Rejected, Pending) |
| `date` | Time-based analysis at day, week, month, quarter, or year granularity |

Each dimension should define:

- **Source table** — the physical table backing the dimension
- **Key column** — the primary key used for joins
- **Display column** — the human-readable label

---

## Measures

Measures are the numeric values that can be aggregated for analysis.

### Order Measures

| Measure | Aggregation | Description |
|---|---|---|
| `order_quantity` | SUM | Total ordered quantity |
| `unit_price` | AVG | Average unit price |
| `gross_amount` | SUM | Total gross amount |
| `tax_amount` | SUM | Total tax amount |
| `net_amount` | SUM | Total net amount |
| `pending_quantity` | SUM | Quantity pending fulfillment |

### Receipt Measures

| Measure | Aggregation | Description |
|---|---|---|
| `received_quantity` | SUM | Total received quantity |
| `receipt_value` | SUM | Total receipt value |

### Inventory Measures

| Measure | Aggregation | Description |
|---|---|---|
| `opening_quantity` | SUM | Opening stock quantity |
| `opening_value` | SUM | Opening stock value |
| `inbound_quantity` | SUM | Inbound quantity in period |
| `inbound_value` | SUM | Inbound value in period |
| `outbound_quantity` | SUM | Outbound quantity in period |
| `outbound_value` | SUM | Outbound value in period |
| `closing_quantity` | SUM | Closing stock quantity |
| `closing_value` | SUM | Closing stock value |
| `adjustment_quantity` | SUM | Adjustment quantity |
| `adjustment_value` | SUM | Adjustment value |

### Employee / HR Measures

| Measure | Aggregation | Description |
|---|---|---|
| `working_hours` | SUM | Total working hours |
| `expense_amount` | SUM | Total employee expenses |

### Tips for Defining Measures

- Always specify the **source column**, **aggregation function** (SUM, AVG, COUNT, MIN, MAX), and a clear **description**.
- Use consistent naming conventions (e.g., prefix measures with the entity name).

---

## Relationships

Relationships define how entities connect. Most relationships in a semantic layer are **many-to-one** (fact/transaction → dimension/master).

### Example Relationship Patterns

```
order ──────┬──► customer
            ├──► vendor
            ├──► status
            └──► product (via order_detail)

receipt ────┬──► order
            ├──► vendor
            └──► product (via receipt_detail)

inventory ──┬──► product
            ├──► location
            └──► (time dimension)

employee ───┬──► department
            └──► location
```

### Defining Relationships

Each relationship should specify:

| Field | Description |
|---|---|
| **Name** | A descriptive identifier (e.g., `order_to_customer`) |
| **From** | Source entity and column (e.g., `order.customer_id`) |
| **To** | Target entity and column (e.g., `customer.customer_id`) |
| **Cardinality** | Many-to-one, one-to-one, many-to-many |
| **Join type** | Inner, left outer, etc. |

---

## Date Fields

Each entity should expose one or more date/datetime fields for time-based filtering and analysis. Common patterns include:

| Category | Typical Date Fields |
|---|---|
| **Document dates** | `document_date`, `order_date`, `invoice_date` |
| **Lifecycle dates** | `created_date`, `modified_date`, `deleted_date` |
| **Approval dates** | `approved_date`, `rejected_date` |
| **Delivery/Fulfillment** | `expected_delivery_date`, `actual_delivery_date`, `receipt_date` |
| **Period dates** | `from_date`, `to_date`, `period_date` |
| **HR-specific dates** | `joining_date`, `termination_date`, `attendance_date` |

> Define a **default date field** for each entity so that time-based queries resolve unambiguously.

---

## Common Queries

These are examples of pre-defined query patterns a semantic layer can support:

| Query | Entities | Dimensions | Measures | Filters |
|---|---|---|---|---|
| Show all open orders | `order` | — | `document_no`, `date`, `customer`, `status` | `status = 'Open'` |
| Total order value by vendor | `order` | `vendor` | `net_amount` (SUM) | — |
| Closing inventory by product and location | `inventory` | `product`, `location` | `closing_quantity`, `closing_value` | — |
| Receipts against orders | `receipt` | — | `document_no`, `date`, `order`, `vendor`, `receipt_value` | `order_id IS NOT NULL` |
| Products with low stock | `inventory` | `product`, `location` | `closing_quantity` | `closing_quantity < reorder_level` |

---

## Example Structure

Below is a simplified JSON skeleton showing how to structure a semantic layer configuration. Adapt the table names, columns, and values to match your own schema.

```jsonc
{
    "entities": [
        {
            "name": "order",
            "physical_tables": ["order_hdr", "order_dtl"],
            "business_name": "Sales/Purchase Order",
            "description": "Represents a business order with header and line-item detail tables",
            "primary_key": "order_id",
            "entity_type": "transaction",
            "synonyms": ["purchase order", "sales order", "PO", "SO"]
        },
        {
            "name": "receipt",
            "physical_tables": ["receipt_hdr", "receipt_dtl"],
            "business_name": "Goods Receipt",
            "description": "Tracks inbound receipt of goods against orders",
            "primary_key": "receipt_id",
            "entity_type": "transaction",
            "synonyms": ["goods receipt", "inbound", "receiving"]
        }
        // ... add more entities as needed
    ],

    "dimensions": [
        {
            "name": "product",
            "source_table": "product_master",
            "key_column": "product_id",
            "display_column": "name",
            "description": "Filter by product, material, or SKU",
            "attributes": ["code", "name", "product_group_id"]
        },
        {
            "name": "vendor",
            "source_table": "vendor_master",
            "key_column": "vendor_id",
            "display_column": "name",
            "description": "Filter by vendor or supplier",
            "attributes": ["code", "name", "vendor_group_id"]
        }
        // ... add more dimensions as needed
    ],

    "measures": [
        {
            "name": "order_quantity",
            "source_table": "order_dtl",
            "source_column": "quantity",
            "aggregation": "sum",
            "data_type": "decimal",
            "description": "Total ordered quantity"
        },
        {
            "name": "net_amount",
            "source_table": "order_dtl",
            "source_column": "net_amount",
            "aggregation": "sum",
            "data_type": "decimal",
            "description": "Total net amount"
        }
        // ... add more measures as needed
    ],

    "relationships": [
        {
            "name": "order_to_customer",
            "from_entity": "order",
            "from_table": "order_hdr",
            "from_column": "customer_id",
            "to_entity": "customer",
            "to_table": "customer_master",
            "to_column": "customer_id",
            "relationship_type": "many_to_one"
        },
        {
            "name": "order_to_vendor",
            "from_entity": "order",
            "from_table": "order_hdr",
            "from_column": "vendor_id",
            "to_entity": "vendor",
            "to_table": "vendor_master",
            "to_column": "vendor_id",
            "relationship_type": "many_to_one"
        }
        // ... add more relationships as needed
    ],

    "columns": [
        {
            "entity": "order",
            "physical_table": "order_hdr",
            "physical_column": "document_no",
            "business_name": "Order Number",
            "data_type": "string",
            "synonyms": ["order number", "PO number", "SO number"]
        },
        {
            "entity": "order",
            "physical_table": "order_hdr",
            "physical_column": "status",
            "business_name": "Order Status",
            "data_type": "string",
            "synonyms": ["status"]
        }
        // ... add more column mappings as needed
    ],

    "date_fields": [
        {
            "entity": "order",
            "fields": ["document_date", "approved_date", "created_date", "expected_delivery_date"]
        },
        {
            "entity": "receipt",
            "fields": ["document_date", "receipt_date", "created_date"]
        }
        // ... add date fields for each entity
    ],

    "common_queries": [
        {
            "query": "Show all open orders",
            "entities": ["order"],
            "dimensions": [],
            "measures": ["document_no", "date", "customer", "status"],
            "filters": "status = 'Open'"
        },
        {
            "query": "Total order value by vendor",
            "entities": ["order"],
            "dimensions": ["vendor"],
            "measures": ["net_amount"],
            "aggregation": "sum"
        }
        // ... add more query templates as needed
    ]
}
```

### Key Points

- **Header + Detail pattern** — Transaction entities typically span a header table (dates, status) and a detail table (line-item measures). Map both and clarify which table each field belongs to.
- **Synonyms** — Include a `synonyms` array on each entity so natural-language queries (e.g., *"show me all POs"*) resolve correctly.
- **Relationships** — Connect every transaction/fact entity back to its related dimensions using `many_to_one` joins.
- **Date field registry** — Declare all date fields per entity so time-based filters resolve unambiguously.
- **Common queries** — Pre-define query templates to accelerate reporting and serve as examples for consumers of the semantic layer.

---

## Best Practices

When building or modifying a semantic layer:

1. **Keep a single source of truth** — maintain entity, measure, dimension, and relationship definitions in a structured configuration file (e.g., JSON or YAML).
2. **Use synonyms** — they power natural-language query resolution and improve discoverability.
3. **Validate relationships** — ensure all joins specify the correct physical table and column mappings.
4. **Document changes** — update documentation whenever the semantic model evolves.
5. **Version control** — track changes to the semantic layer configuration alongside application code.
6. **Use the header + detail pattern** — for transaction entities with line items, always map both the header and detail tables and clarify which table each measure or column belongs to.
7. **Register all date fields** — a complete date field registry prevents ambiguity when users ask time-based questions.
8. **Decompose composite measures** — break out tax, freight, discount, and other components individually so consumers can aggregate at the granularity they need.

---