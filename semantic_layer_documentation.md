# Semantic Layer Documentation

> A comprehensive reference for designing and implementing a semantic layer for data analytics and reporting.

## Overview

A semantic layer provides a **business-friendly abstraction** over an underlying physical database. It maps raw tables and columns to meaningful business concepts, enabling:

- **Consistent reporting** — every team uses the same definitions for key metrics
- **Natural-language querying** — users ask questions in business language, not SQL
- **Self-service analytics** — non-technical users can discover and analyze data independently
- **Governance** — central control over data definitions, metrics, and access

### Key Components

The semantic layer consists of five core elements:

1. **Entities** — Logical groupings of related database tables (orders, requisitions, inventory)
2. **Dimensions** — Axes for slicing and filtering data (supplier, location, project, date)
3. **Measures** — Numeric values that can be aggregated (quantity, amount, hours)
4. **Relationships** — Connections between entities enabling joins and navigation
5. **Date Fields** — Temporal dimensions enabling time-based analysis

### Advanced Features

Beyond the core components, modern semantic layers support:

- **Query Patterns** — Deterministic shortcuts for frequently-asked questions (bypasses embedding lookup)
- **Term Glossary** — Business-language dictionary mapping jargon to database columns
- **Common Queries** — Pre-defined templates documenting supported analyses
- **Database Prefixes** — Multi-database federation for complex architectures
- **Column Mappings** — Business names and synonyms for every column
- **Semantic Enrichment** — Optional descriptions and comments augmenting schema metadata

---

## Table of Contents

- [Entities](#entities)
- [Dimensions](#dimensions)
- [Relationships](#relationships)
- [Measures](#measures)
- [Date Fields](#date-fields)
- [Columns](#columns)
- [Example Structure](#example-structure)
- [Query Patterns](#query-patterns)
- [Term Glossary](#term-glossary)
- [Common Queries](#common-queries)
- [Database Prefix](#database-prefix)
- [Best Practices](#best-practices)
- [Summary Checklist](#summary-checklist)

---

## Entities

Entities represent the core business concepts in your domain. Each entity maps to one or more physical database tables and serves as the primary unit for query resolution. When a user asks a question, the semantic layer identifies which entities are relevant and then retrieves the specific tables, columns, and measures associated with those entities.

### Entity Purpose and Scope

An entity is a **logical grouping** of related physical tables:
- **"purchase_order"** — header (`purchase_order`) + detail (`purchase_order_dtl`) tables
- **"stock"** — single table (`stock`) with inventory summaries  
- **"employee"** — master table (`mst_employee`)

### Entity Structure

| Field | Type | Description | Example |
|---|---|---|---|
| `name` | `string` | Unique logical identifier for the entity | `"purchase_order"`, `"stock"`, `"employee"` |
| `physical_tables` | `string[]` | One or more database tables that comprise this entity | `["purchase_order", "purchase_order_dtl"]` |
| `business_name` | `string` | Human-readable business name | `"Purchase Order"`, `"Stock Summary"` |
| `description` | `string` | Brief explanation of what this entity represents | `"Purchase orders issued to suppliers for material procurement"` |
| `primary_key` | `string` or `string[]` | Column(s) uniquely identifying records in this entity | `"purchase_order_id"` or `["attend_date", "employee_id", "project_id"]` |
| `entity_type` | `string` | Classification: `"master"`, `"transaction"`, or `"fact"` | See Entity Types below |
| `synonyms` | `string[]` | Alternative names/phrases users might use | `["po", "purchase order", "order", "po document"]` |

### Entity Types

Each entity has a type that describes its role in the business logic:

| Type | Purpose | Characteristics | Examples |
|---|---|---|---|
| **Master** | Reference/dimension data | Static or slowly-changing; looked up and joined | Customer, Supplier, Employee, Product, Location, Project |
| **Transaction** | Business events | Time-stamped operational records; typically have header+detail structure | Purchase Order, Requisition, Goods Receipt, Stock Issue, Invoice |
| **Fact** | Aggregated/summary data | Pre-computed summaries across time or categories | Stock Summary, Monthly Aggregates, KPI Tables |

### Entities vs. Dimensions

**Entities** are business objects (nouns): orders, employees, stock. **Dimensions** are axes for analysis: slice by supplier, location, date. Most dimensions correspond to master entities, but not always.

### Field Notes

- **`physical_tables`** — list tables in order (header first for transactions). Use exact table names
- **`primary_key`** — single ID (`"order_id"`) or composite (`["date", "emp_id"]`)
- **`synonyms`** — include all variations (abbreviations, colloquialisms). Matched case-insensitively

### Example Entity Definitions

```jsonc
{
    "name": "purchase_order",
    "physical_tables": ["purchase_order", "purchase_order_dtl"],
    "business_name": "Purchase Order",
    "description": "Purchase orders issued to suppliers for material procurement",
    "primary_key": "purchase_order_id",
    "entity_type": "transaction",
    "synonyms": ["po", "purchase order", "order", "po document"]
},
{
    "name": "stock",
    "physical_tables": ["stock"],
    "business_name": "Stock Summary",
    "description": "Period-wise stock movement and valuation summary",
    "primary_key": "stock_id",
    "entity_type": "fact",
    "synonyms": ["inventory", "stock", "stock summary", "stock balance", "reorder"]
},
{
    "name": "employee",
    "physical_tables": ["mst_employee"],
    "business_name": "Employee Master",
    "description": "Master data for employees",
    "primary_key": "employee_id",
    "entity_type": "master",
    "synonyms": ["employee", "staff", "worker", "personnel"]
},
{
    "name": "employee_attendance",
    "physical_tables": ["employee_attendance_project"],
    "business_name": "Employee Attendance",
    "description": "Project-wise employee attendance records",
    "primary_key": ["attend_date", "employee_id", "project_id"],
    "entity_type": "transaction",
    "synonyms": ["attendance", "employee attendance", "punch record"]
}
```

### Best Practices for Entities

- **Keep entity boundaries clean** — one entity = one coherent business concept
- **Always include synonyms** — anticipate how users will refer to each entity
- **Document the header-detail structure** — if spanning multiple tables, clarify which is header/detail
- **Use consistent naming** — follow your organization's entity naming conventions
- **Avoid overly granular entities** — don't create an entity for every table; group related tables
- **Declare all composite keys** — for non-transactional entities with multi-part keys, be explicit
- **Cross-reference relationships** — entities should align with defined relationships in the semantic layer

> **Tip:** Think of entities as "the nouns in your business language." When users ask questions, they're asking about entities: "Show me all open requisitions (entity: requisition) by supplier (entity: supplier)".

---

## Dimensions

Dimensions define the **axes along which fact and transaction data can be sliced, filtered, and grouped**. While entities answer "what business objects exist?", dimensions answer "how can we analyze that data?"

Dimensions enable grouping and filtering: "total spending **by supplier**", "average price **by location**", "orders **by project**"

### Dimension Structure

| Field | Type | Description | Example |
|---|---|---|---|
| `name` | `string` | Unique identifier for the dimension | `"supplier"`, `"project"`, `"item"` |
| `source_table` | `string` | Physical table containing the key and attributes | `"mst_supplier"` |
| `key_column` | `string` | Column that uniquely identifies each dimension member | `"supplier_id"` |
| `display_column` | `string` | Column to display in results (e.g., name, code) | `"supplier_name"` |
| `description` | `string` | Business meaning of the dimension | `"Filter by vendor or supplier"` |
| `attributes` | `string[]` | Other columns available for analysis or filtering | `["code", "name", "contact_email", "payment_term_id"]` |

### Dimensions in the Semantic Layer

Dimensions typically **mirror master/reference entities**, but not always:

- **supplier** dimension ↔ **supplier** entity (master)
- **project** dimension ↔ **project** entity (master)
- **item** dimension ↔ **item** entity (master)
- **status** dimension ↔ no corresponding entity (a set of fixed values)
- **date** dimension ↔ implicit (often implicit in transactions)

The `attributes` array lists additional columns available for filtering/grouping (code, email, phone). Users can reference any attribute directly.

#### Dimension Attributes Example

```jsonc
{
    "name": "supplier",
    "source_table": "mst_supplier",
    "key_column": "supplier_id",
    "display_column": "supplier_name",
    "description": "Filter by vendor or supplier",
    "attributes": ["supplier_code", "contact_email", "contact_mobile", "payment_term_id"]
}
```

### Common Dimensions

- **Master dimensions** — supplier, item, project, location, employee (backed by master tables)
- **Status dimensions** — status, approval_status (fixed values from transactions)
- **Date dimension** — allow analysis by day, week, month, quarter, year

Date fields (document_date, delivery_date, created_date, etc.) enable time-based analysis. Declare available date fields per entity in the `date_fields` section.

### Best Practices

- **Name dimensions clearly** — use singular nouns (`supplier`, `item`, `project`)
- **Always specify a display_column** — users see this in results, not the key
- **Include relevant attributes** — add any column users might want to filter on
- **Keep attributes focused** — avoid cluttering dimensions with too many minor fields
- **Align dimensions with entities** — most dimensions correspond to master entities
- **Handle null/unknown gracefully** — ensure dimension tables have entries for "unknown" values if needed

> **Tip:** When designing dimensions, think: "What are the natural ways my business users slice this data?" Those are your dimensions.

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

## Columns

The columns section provides **business-friendly mappings** for every important column in your schema. It bridges the gap between physical database column names and how business users reference those columns.

### Purpose

- **Synonym mapping** — Users can reference columns by business name or abbreviation (not just the raw table.column)
- **Documentation** — Record the data type and business meaning of each column
- **Discovery** — Enable end-users to explore available columns by entity
- **Consistency** — Enforce standard naming and terminology across the organization

### Column Structure

| Field | Type | Description | Example |
|---|---|---|---|
| `entity` | `string` | Which entity this column belongs to | `"purchase_order"` |
| `physical_table` | `string` | The actual database table | `"purchase_order"` or `"purchase_order_dtl"` |
| `physical_column` | `string` | The actual database column name | `"document_no"`, `"net_amount"` |
| `business_name` | `string` | Human-readable business name | `"Order Number"`, `"Net Amount"` |
| `data_type` | `string` | SQL data type | `"string"`, `"decimal"`, `"date"` |
| `synonyms` | `string[]` | Alternative names users might use | `["po number", "order number"]` |

### Practical Examples

```jsonc
"columns": [
    // Purchase Order header columns
    {
        "entity": "purchase_order",
        "physical_table": "purchase_order",
        "physical_column": "document_no",
        "business_name": "Order Number",
        "data_type": "string",
        "synonyms": ["po number", "purchase order number", "order number", "po no"]
    },
    {
        "entity": "purchase_order",
        "physical_table": "purchase_order",
        "physical_column": "document_date",
        "business_name": "Order Date",
        "data_type": "date",
        "synonyms": ["po date", "order date", "document date"]
    },
    {
        "entity": "purchase_order",
        "physical_table": "purchase_order",
        "physical_column": "supplier_id",
        "business_name": "Supplier",
        "data_type": "int",
        "synonyms": ["vendor", "party", "supplier"]
    },
    // Purchase Order detail columns
    {
        "entity": "purchase_order",
        "physical_table": "purchase_order_dtl",
        "physical_column": "net_amount",
        "business_name": "Net Amount",
        "data_type": "decimal",
        "synonyms": ["net value", "base amount", "line total"]
    },
    {
        "entity": "purchase_order",
        "physical_table": "purchase_order_dtl",
        "physical_column": "freight_amount",
        "business_name": "Freight Charges",
        "data_type": "decimal",
        "synonyms": ["freight", "shipping cost", "transportation cost"]
    },
    // Stock columns
    {
        "entity": "stock",
        "physical_table": "stock",
        "physical_column": "closing_quantity",
        "business_name": "Closing Stock",
        "data_type": "decimal",
        "synonyms": ["closing stock", "ending inventory", "on-hand quantity"]
    },
    {
        "entity": "stock",
        "physical_table": "stock",
        "physical_column": "closing_amount",
        "business_name": "Stock Value",
        "data_type": "decimal",
        "synonyms": ["stock value", "inventory value", "valuation"]
    }
]
```

Columns enable **synonym matching** (user says "order number" → resolves to `document_no`), **discovery** of available fields, and **validation** that columns exist.

Map important columns with synonyms, business names, and data types. Group logically by entity and table.

---

## Common Queries

Common queries are **pre-defined, template-based query patterns** that serve as executable templates and examples for end users. They document the types of analyses your organization supports and can be used for testing, documentation, and guiding users toward valuable insights.

### Purpose

- **Documentation** — Show users what kinds of questions the semantic layer can answer
- **Template generation** — Provide starting points for custom analysis
- **Validation** — Test the semantic layer configuration against real business questions
- **Performance** — Pre-validate SQL patterns before users create ad-hoc queries

### Common Query Structure

| Field | Type | Description | Example |
|---|---|---|---|
| `name` | `string` | Descriptive query identifier | `"Open Requisitions"`, `"Stock by Location"` |
| `query` | `string` | Natural language description of the query | `"Show all open requisitions"` |
| `entities` | `string[]` | Core entities involved | `["requisition"]` |
| `dimensions` | `string[]` | Grouping/filtering dimensions | `["project", "employee"]` |
| `measures` | `string[]` | Aggregated values to display | `["quantity", "status"]` |
| `aggregation` | `string` | If single aggregate, specify function | `"sum"`, `"count"`, `"avg"` |
| `filters` | `string` | Optional WHERE conditions | `"status = 'Open'"` |

### Practical Examples

```jsonc
"common_queries": [
    {
        "name": "Open Requisitions",
        "query": "Show all open requisitions",
        "entities": ["requisition"],
        "dimensions": [],
        "measures": ["document_no", "document_date", "project", "employee", "status"],
        "filters": "status = 'Open'"
    },
    {
        "name": "Total Purchase Value by Supplier",
        "query": "Total order value by vendor",
        "entities": ["purchase_order"],
        "dimensions": ["supplier"],
        "measures": ["total_amount"],
        "aggregation": "sum"
    },
    {
        "name": "Closing Inventory by Location",
        "query": "Closing inventory by product and location",
        "entities": ["stock"],
        "dimensions": ["item", "location"],
        "measures": ["closing_quantity", "closing_amount"],
        "aggregation": "sum"
    },
    {
        "name": "Pending Receipts",
        "query": "Goods receipts pending against orders",
        "entities": ["goods_receipt", "purchase_order"],
        "dimensions": ["supplier", "project"],
        "measures": ["document_no", "total_amount"],
        "filters": "purchase_order_id IS NOT NULL"
    }
]
```

Create common queries for high-impact business questions. Keep names concise and test against actual schema.

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
    ],

    "query_patterns": [
        {
            "pattern": ["open orders", "pending orders", "outstanding POs"],
            "entities": ["order"],
            "filters": { "status": "Open" }
        },
        {
            "pattern": ["low stock", "below reorder"],
            "entities": ["inventory"],
            "filters": { "closing_quantity": "< reorder_level" }
        }
        // ... add more patterns for frequently-asked queries
    ],

    "term_glossary": {
        "revenue": { "table": "order_dtl", "column": "net_amount", "aggregation": "sum" },
        "headcount": { "table": "employee_master", "column": "employee_id", "aggregation": "count" },
        "COGS": { "table": "order_dtl", "column": "cost_amount", "aggregation": "sum" }
        // ... add business terms used by your organization
    },

    "database_prefix": ["db1", "db2"]
    // Optional: omit for single-database setups
}
```

### Key Points

- **Header + Detail pattern** — Transaction entities typically span a header table (dates, status) and a detail table (line-item measures). Map both and clarify which table each field belongs to.
- **Synonyms** — Include a `synonyms` array on each entity so natural-language queries (e.g., *"show me all POs"*) resolve correctly.
- **Relationships** — Connect every transaction/fact entity back to its related dimensions using `many_to_one` joins.
- **Date field registry** — Declare all date fields per entity so time-based filters resolve unambiguously.
- **Common queries** — Pre-define query templates to accelerate reporting and serve as examples for consumers of the semantic layer.

---

## Query Patterns

Query patterns provide a **deterministic, rule-based shortcut** that bypasses semantic embedding lookup for known or frequently-asked query types. When a user's prompt matches a pattern, the system immediately resolves the correct entities and applies optional default filters—achieving perfect accuracy without relying on semantic similarity scoring.

### When to Use Query Patterns

- **High-volume recurring queries** — e.g., "open orders", "pending approvals"
- **Precise filtering requirements** — where certain conditions should always apply
- **Domain-specific questions** — where users repeat similar phrasings
- **Fallback for embedding uncertainties** — when the semantic model struggles with certain terminology

### Pattern Entry Structure

| Field | Type | Description | Example |
|---|---|---|---|
| `pattern` | `string[]` | List of phrases/keywords to match (case-insensitive substring search) | `["open orders", "pending POs", "outstanding orders"]` |
| `entities` | `string[]` | Entity names to select when pattern matches | `["purchase_order", "requisition"]` |
| `filters` | `object` | Optional default WHERE conditions (key = column/field, value = filter expression) | `{ "status": "Open", "approval_status": "Pending" }` |

### Matching Logic

1. The user's prompt is normalized (lowercased, extra whitespace removed).
2. Each `pattern` phrase is checked against the normalized prompt using **substring matching** (case-insensitive).
3. On match, the pattern's entities are immediately selected and marked with high confidence.
4. Default filters are applied to narrow result scope.
5. **Multiple patterns can match simultaneously** — their entity sets are unioned (combined).
6. Pattern matching occurs **before** semantic embedding, so matches take precedence.

### Practical Examples

```jsonc
"query_patterns": [
    {
        "pattern": ["open orders", "pending orders", "outstanding POs"],
        "entities": ["purchase_order"],
        "filters": { "status": "Open" }
    },
    {
        "pattern": ["approved requisitions", "approved indents"],
        "entities": ["requisition"],
        "filters": { "approval_status": "Approved" }
    },
    {
        "pattern": ["low stock", "below reorder", "stock alert"],
        "entities": ["stock"],
        "filters": { "closing_quantity": "< reorder_level" }
    },
    {
        "pattern": ["project wise inventory", "site inventory", "project stock"],
        "entities": ["stock", "project"],
        "filters": {}
    }
]
```

Create patterns for the 20-30 most frequently-asked questions. Use specific phrases (avoid "order" which matches too broadly). Include synonyms and set filters only for universal conditions.

---

## Term Glossary

The term glossary is a **business-language dictionary** that maps colloquial business terms and jargon to their precise physical locations in the database. It enables the semantic layer to understand domain-specific language and boost selection accuracy for business terminology that users naturally employ when asking questions.

### Purpose

When users ask about "revenue," "purchase value," "closing stock," or other business terms:
1. The glossary immediately maps these terms to their source columns and aggregation functions
2. A lexical boost is applied, increasing the confidence score for those tables
3. Downstream SQL generation can use the exact column reference and aggregation

This bridges the semantic gap between **how users talk about data** and **where it's actually stored** in the database.

### Term Glossary Entry Structure

| Field | Type | Description | Example |
|---|---|---|---|
| `term` | `string` | Business term or colloquial phrase (dictionary key) | `"revenue"`, `"closing stock"`, `"po value"` |
| `table` | `string` | Physical table containing the data | `"purchase_order_dtl"`, `"stock"` |
| `column` | `string` | Physical column name within that table | `"net_amount"`, `"closing_quantity"` |
| `aggregation` | `string` | Default aggregation function (sum, avg, count, min, max) | `"sum"`, `"count"` |

### Practical Examples

```jsonc
"term_glossary": {
    // Financial/Commercial terms
    "revenue": { "table": "purchase_order_dtl", "column": "net_amount", "aggregation": "sum" },
    "purchase value": { "table": "purchase_order", "column": "total_amount", "aggregation": "sum" },
    "po value": { "table": "purchase_order", "column": "total_amount", "aggregation": "sum" },
    "order value": { "table": "purchase_order", "column": "total_amount", "aggregation": "sum" },
    "grn value": { "table": "inward_hdr", "column": "total_amount", "aggregation": "sum" },
    "receipt value": { "table": "inward_hdr", "column": "total_amount", "aggregation": "sum" },
    "freight": { "table": "purchase_order_dtl", "column": "freight_amount", "aggregation": "sum" },
    "tax": { "table": "purchase_order_dtl", "column": "cgst_amount", "aggregation": "sum" },
    
    // Inventory terms
    "closing stock": { "table": "stock", "column": "closing_quantity", "aggregation": "sum" },
    "opening stock": { "table": "stock", "column": "opening_quantity", "aggregation": "sum" },
    "stock value": { "table": "stock", "column": "closing_amount", "aggregation": "sum" },
    "pending quantity": { "table": "purchase_order_dtl", "column": "pending_quantity", "aggregation": "sum" },
    
    // HR/Operational terms
    "headcount": { "table": "mst_employee", "column": "employee_id", "aggregation": "count" },
    "working hours": { "table": "employee_attendance_project", "column": "hours", "aggregation": "sum" },
    "expense amount": { "table": "employee_expense_book_dtl", "column": "expense_amount", "aggregation": "sum" },
    
    // Requisition terms
    "indent quantity": { "table": "requisition_dtl", "column": "quantity", "aggregation": "sum" },
    
    // Loan/Advance terms
    "loan amount": { "table": "loan_advance_hdr", "column": "amount", "aggregation": "sum" },
    "recovery": { "table": "loan_advance_dtl", "column": "recovery_amount", "aggregation": "sum" }
}
```

### How Term Matching Works

1. **Token extraction** — The user's prompt is split into individual words/tokens.
2. **Glossary lookup** — Each token is checked against glossary keys (case-insensitive).
3. **Lexical boost** — For any matching terms, the corresponding table receives a significant confidence boost (typically +0.45).
4. **Disambiguation** — When the embedding model is uncertain, glossary matches act as a tiebreaker.
5. **SQL generation** — The glossary term's `table`, `column`, and `aggregation` are used to construct precise SQL.

### Real-World Scenario

**User prompt:** "What is our total purchase value by supplier?"

- Embedding lookup identifies `purchase_order` and `supplier` as candidates
- Glossary finds "purchase value" → `purchase_order`, `total_amount`, `sum`
- The lexical boost significantly increases `purchase_order`'s confidence score
- Result: `purchase_order` is confidently selected with the correct aggregation

Map all business jargon, abbreviations, and domain-specific terms. Include inconsistent spellings (closing stock vs. closing inventory). Audit query logs to identify missing terms.

---

## Database Prefix

The optional `database_prefix` field specifies a database or schema prefix to qualify table names. Use for multi-database or multi-schema architectures.

```jsonc
{
    "database_prefix": "erp_db",  // Single database
    // OR
    "database_prefix": ["erp_db", "hr_db"],  // Multiple databases
    
    "entities": [...],
    "dimensions": [...],
    "measures": [...]
}
```

**Usage:**
- **Omit** for single-database setups (default)
- **String** — all tables qualified as `prefix.table_name`
- **Array** — tables span multiple databases; include prefix in physical table names (e.g., `["erp_db.orders", "hr_db.employees"]`)

> **Tip:** Use database prefixes only for federated or multi-tenant architectures. For single databases, omit this field.

---

## Best Practices

When designing and maintaining a semantic layer:

### Architecture and Design

1. **Single source of truth** — Maintain entity, measure, dimension, and relationship definitions in a structured configuration file (JSON, YAML, or equivalent) under version control.

2. **Consistent naming conventions** — Use a standardized naming scheme for entities, columns, and relationships (e.g., snake_case for identifiers, clear prefixes for related items).

3. **Header + Detail pattern for transactions** — Always map both header and detail tables for transactional entities, and clearly document which table each measure/column originates from.

4. **Clear entity boundaries** — Each entity represents one coherent business concept. Avoid creating entities for individual tables or over-fragmenting domains.

5. **Relationship completeness** — Ensure all logical relationships are defined. Missing relationships prevent proper foreign-key expansion and result in incomplete query resolution.

### Business Language Integration

6. **Comprehensive synonyms** — For every entity, dimension, and measure, include all variations users might reference (abbreviations, regional terms, historical names).

7. **Term glossary for domain jargon** — Map every piece of business terminology your users employ to its source column and aggregation. This directly improves accuracy for colloquial queries.

8. **Query patterns for high-volume questions** — Identify the 20-30 questions your organization asks most frequently, and encode them as explicit query patterns for deterministic, 100%-accurate resolution.

9. **Documentation comments** — Add business context to unusual or non-obvious mappings so future maintainers understand the intent.

### Data Quality and Consistency

10. **Register all date fields** — For each entity, explicitly list all available date/time fields so time-based filters resolve unambiguously.

11. **Validate relationships** — Before deploying, verify that all join paths are correct and that foreign-key columns exist in both tables.

12. **Test semantic layer changes** — Whenever you add/modify entities, dimensions, measures, or relationships, test against the common queries to ensure backward compatibility.

13. **Version control** — Treat the semantic layer configuration as code. Track all changes in git with clear commit messages explaining business rationale.

### Maintenance and Evolution

14. **Regular audits** — Periodically review user queries (logs) to identify:
    - New domain terms that need glossary entries
    - Frequently-asked questions missing from query patterns
    - Entities or dimensions that are never used (candidates for removal)
    - Consistently-failed queries (schema changes needed)

15. **Update on schema changes** — Whenever the underlying database schema changes (new tables, column renames, relationship changes), update the semantic layer immediately and test thoroughly.

16. **Document changes** — Include business rationale in commit messages. "Added 'closing stock' to term glossary" is less helpful than "Added 'closing stock' → stock.closing_quantity mapping; users frequently reference this term in inventory queries."

17. **Decompose composite measures** — Break out tax, freight, discount, and other components individually so SQL consumers can aggregate at the granularity they need.

### Integration with NLP System

18. **Leverage query patterns as deterministic shortcuts** — For queries where the semantic embedding model struggles, add an explicit query pattern. This guarantees accuracy for high-value questions.

19. **Use term glossary to boost accuracy** — Highly domain-specific jargon should live in the glossary, not in entity names. The glossary provides a lexical boost that helps the semantic model.

20. **Balance automation with configuration** — Use embeddings for discovery, but don't rely on them exclusively. Query patterns + term glossary provide deterministic, rule-based accuracy for critical questions.

21. **Test the NLP pipeline** — After semantic layer changes, re-run sample queries through the full NLP pipeline (embedding → aggregation → thresholds → expansion) to ensure end-to-end accuracy.

22. **Monitor confidence scores** — Watch the confidence scores reported by the table selector. Consistently low scores (< 0.7) for important queries indicate a need for pattern or glossary additions.

### Multi-Database and Complex Scenarios

23. **Use database prefixes for federated architectures** — If your semantic layer spans multiple databases, explicitly declare the prefix and include it in all table references.

24. **Document database routing** — For multi-database setups, maintain a map showing which entities live in which database(s).

25. **Consider query locality** — Queries that join tables from different databases are slower. When possible, normalize data into a single analytics database.

---

## Summary Checklist

Use this checklist when building or updating a semantic layer:

- [ ] All entities have clear business names, descriptions, and synonyms
- [ ] All dimensions are backed by master/reference tables with key and display columns
- [ ] All measures specify source table, source column, aggregation function, and description
- [ ] All relationships are defined with correct table/column mappings
- [ ] All date fields are registered per entity
- [ ] Term glossary covers organization-specific jargon and frequently-used terms
- [ ] Query patterns exist for the top 20-30 business questions
- [ ] Common queries are documented with entities, dimensions, measures, and filters
- [ ] Database prefixes are defined if spanning multiple databases
- [ ] All definitions are version-controlled with clear commit messages
- [ ] Semantic layer is tested against sample queries before deployment
- [ ] Documentation is kept updated as schema and business requirements evolve

---