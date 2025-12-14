# Real Estate Data Ingestion System
## A Production-Ready ETL Pipeline

**Project:** Data Ingestion Subsystem  
**Tech Stack:** Python | PostgreSQL | Pandas | Pytest  
**Quality:** 93% Test Coverage | 56 Passing Tests

---

# Project Overview

## Business Problem
- Process **9,129 real estate property records** from CSV files
- Ensure data quality through comprehensive validation
- Load clean data into PostgreSQL for analytics

## Solution Delivered
- ✅ Automated ETL pipeline with comprehensive error handling
- ✅ Separate valid and invalid records with full traceability
- ✅ Complete audit trail via logging and reject tables
- ✅ 93% test coverage for production reliability

---

# Architecture & Tech Stack

## Core Technologies
- **Python 3.11** - Pipeline orchestration & business logic
- **Pandas** - Data manipulation & transformation
- **PostgreSQL** - Enterprise data warehouse
- **Psycopg2** - Database connectivity & connection pooling
- **Pytest** - Comprehensive unit testing framework

## Key Features
- 📦 Modular design (16 Python files)
- 📊 JSONB storage for flexible rejected records
- ⚡ Batch processing (1000 rows/batch)
- 🔒 Transaction management & rollback support

---

# ETL Pipeline Flow & Medallion Architecture

## Data Quality Layers

```
🟤 BRONZE (Raw)              🥈 SILVER (Validated)           🥇 GOLD (Business-Ready)
┌──────────────────┐         ┌──────────────────┐           ┌──────────────────┐
│ real_estate.csv  │────────▶│ stg_real_estate  │──────────▶│  Future Layer    │
│ (9,129 records)  │         │ (4,110 valid)    │           │  • Aggregations  │
│                  │         │                  │           │  • Dimensions    │
│ • Raw columns    │         │ • Clean names    │           │  • Fact tables   │
│ • Invalid data   │         │ • Validated      │           │  • Business KPIs │
│ • Duplicates     │         │ • Deduplicated   │           └──────────────────┘
└──────────────────┘         └──────────────────┘
        │
        ↓
┌──────────────────┐
│   stg_rejects    │
│ (5,019 rejected) │
│ • JSONB format   │
│ • Audit trail    │
└──────────────────┘

## Pipeline Steps

┌─────────────────────────────┐
│        EXTRACT              │
│   CSV Reader Module         │
│   (9,129 records loaded)    │
└────────────┬────────────────┘
             ↓
┌─────────────────────────────┐
│       TRANSFORM             │
│  • Rename columns           │
│  • Strip whitespace         │
│  • Handle missing values    │
│  • Convert date formats     │
│  • Validate business rules  │
└────────────┬────────────────┘
             ↓
┌─────────────────────────────┐
│         LOAD                │
│  PostgreSQL UPSERT          │
│  (Conflict handling)        │
└────────────┬────────────────┘
             ↓
┌──────────────────┬──────────────────┐
│ stg_real_estate  │   stg_rejects    │
│ (4,110 valid)    │  (5,019 invalid) │
└──────────────────┴──────────────────┘
```

---

# Data Cleaning Module

## Transformation Functions

### 1. Column Renaming (snake_case standardization)
- `"data.date"` → `data_date`
- `"location.address.city"` → `city`
- `"data.parking spaces"` → `parking_spaces`

### 2. Whitespace Removal
- Strip leading/trailing spaces from all text fields
- Prevents duplicate entries from spacing variations

### 3. Missing Value Handling
- Convert placeholder values (`"0"`) to proper NULL
- Explicit handling for data quality

### 4. Date Conversion
- Parse string dates to timestamp format
- Error handling with coercion for invalid dates

**Result:** Clean, standardized, database-ready records

---

# Data Validation & Quality

## Comprehensive Validation Rules

| Rule Type | Validation Logic | Action Taken |
|-----------|-----------------|--------------|
| **Required Fields** | location_id, city, state must exist | ❌ Reject if NULL/empty |
| **NULL Validation** | data_date, property_type, zip_code, ownership_type, address_line1 | ❌ Reject if NULL |
| **Numeric Ranges** | parking_spaces ≥ 0 | ❌ Reject if negative |
| **Duplicates** | Check primary key (location_id) | ⚠️ Keep first, reject rest |

## Current Pipeline Results
- ✅ **4,110 valid records (45%)** → Loaded to `stg_real_estate`
- ❌ **5,019 rejected records (55%)** → Logged to `stg_rejects`
- 📊 Primary rejection reason: NULL `data_date` values in source data

---

# Database Schema Design

## 🟤 Bronze Layer: Raw Data Preservation

**Table: stg_rejects** (Invalid Records)
- Stores rejected records in original format
- `raw_data` (JSONB) - Complete raw record with original column names
- Maintains audit trail of all failed validations
- **Example:** `{"data.date": "0", "location.id": "CT3316"}`

## 🥈 Silver Layer: Validated & Cleaned

**Table: stg_real_estate** (Valid Records)
- 15 standardized columns with clean snake_case names
- `location_id` (PK), `city`, `state`, `data_date`, `parking_spaces`, etc.
- UPSERT logic: `INSERT ... ON CONFLICT DO UPDATE`
- Indexed for fast analytical queries

**View: v_rejects_expanded**
- Expands Bronze JSONB into Silver-quality columns
- Bridges Bronze → Silver for rejected data analysis

## 🥇 Gold Layer: Future Enhancement
- Aggregated dimension tables (dim_location, dim_property)
- Business-specific KPIs and metrics
- Denormalized fact tables for reporting

## Why JSONB for Bronze?
✅ **Preserves raw format** - Historical accuracy  
✅ **Flexible schema** - Handle any source changes  
✅ **Queryable** - PostgreSQL JSONB operators  
✅ **Indexable** - GIN indexes for performance

---

# Error Handling & Logging

## Comprehensive Logging System
**File Output:** `logs/pipeline.log`  
**Console Output:** Real-time progress updates  
**Format:** `2025-12-10 09:45:32 - etl_pipeline - INFO - Message`

## Three-Tier Error Strategy

### 1. Validation Errors
- Logged to `stg_rejects` with specific rejection reason
- Full record preserved in JSONB for analysis
- Enables data quality reporting

### 2. Database Errors
- Automatic transaction rollback
- Preserves data integrity (ACID compliance)
- Detailed error messages logged

### 3. Connection Errors
- Graceful pipeline failure
- Clear error messages for troubleshooting
- No partial data commits

## Complete Audit Trail
✅ Every rejected record tracked with reason & timestamp  
✅ Full data preservation in JSONB format  
✅ Pipeline execution metrics logged

---

# Testing & Quality Assurance

## Test Coverage: 93% (333 lines tested, 22 untested)

| Module | Lines | Coverage | Tests | Status |
|--------|-------|----------|-------|--------|
| **clean.py** | 30 | 100% ✅ | 8 | All passing |
| **validate.py** | 68 | 100% ✅ | 12 | All passing |
| **rules.py** | 36 | 97% ✅ | 7 | All passing |
| **load.py** | 83 | 100% ✅ | 17 | All passing |
| **main.py** | 59 | 100% ✅ | 4 | All passing |
| **utils/** | 19 | 100% ✅ | - | Integrated |

## Test Categories
- ✅ **Unit Tests** - Individual function validation
- ✅ **Integration Tests** - Database operations & transactions
- ✅ **Edge Cases** - Empty DataFrames, NULL values, duplicates
- ✅ **Error Scenarios** - Connection failures, rollbacks, invalid data

**Result:** 56/56 tests passing ✅ (0.68 seconds execution time)

---

# Results & Key Takeaways

## Pipeline Performance Metrics
- ⚡ **Processing Speed:** 9,129 records in seconds
- 📦 **Scalability:** Batch processing (1000 rows/batch)
- 🔒 **Data Integrity:** Zero data loss (all records tracked)
- 🛡️ **Reliability:** Transaction safety with rollback support

## Key Achievements

### 1. Modular Design
- Reusable components for future data sources (API, JSON readers)
- Clear separation of concerns (Extract, Transform, Load)

### 2. Data Quality Enforcement
- Strict validation prevents bad data from polluting warehouse
- 45% success rate reflects **actual source data quality**

### 3. Full Observability
- Complete logging & audit trail for compliance
- JSONB reject storage enables root cause analysis

### 4. Production-Ready Reliability
- 93% test coverage ensures stability
- ACID transactions protect data integrity

## Next Steps & Roadmap
1. 📡 Add API and JSON readers for additional sources
2. 🔄 Implement incremental/delta loads (process only changes)
3. 📊 Build data quality metrics dashboard
4. ⏰ Schedule automated pipeline runs (cron/Airflow)
5. 🚀 Deploy to production environment with monitoring

---

# Questions?

## Contact Information
**Project:** Data Ingestion Subsystem  
**Repository:** github.com/marlonmunoz/data-ingestion-subsystem-project

## Key Metrics Summary
- 📊 **9,129** total records processed
- ✅ **4,110** valid records (45%)
- ❌ **5,019** rejected records (55%)
- 🧪 **56** passing tests
- 📈 **93%** code coverage
- ⚡ **< 1 second** test execution time

**Thank you!**
