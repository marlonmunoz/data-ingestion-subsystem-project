# ETL Pipeline Project Walkthrough
## How This Real Estate Data Ingestion System Was Built

---

## 📋 Table of Contents
1. [Project Overview](#project-overview)
2. [Tech Stack](#tech-stack)
3. [Step-by-Step Build Process](#step-by-step-build-process)
4. [How the Pipeline Executes](#how-the-pipeline-executes)
5. [File-by-File Explanation](#file-by-file-explanation)
6. [Data Flow Journey](#data-flow-journey)

---

## Project Overview

**Goal:** Build an ETL pipeline that extracts real estate data from CSV, validates/cleans it, and loads it into PostgreSQL.

**Final Result:**
- ✅ 9,129 records processed
- ✅ 4,110 valid records loaded to database
- ✅ 5,019 rejected records logged with reasons
- ✅ 93% test coverage
- ✅ Production-ready error handling

---

## Tech Stack

### **Core Technologies**
- **Python 3.x** - Primary programming language
- **PostgreSQL** - Relational database for data storage
- **Pandas 2.3.3** - Data manipulation and analysis
- **psycopg2-binary 2.9.9** - PostgreSQL database adapter

### **Testing & Quality**
- **pytest 8.3.3** - Testing framework
- **pytest-cov** - Code coverage analysis (93% coverage achieved)

### **Web Framework (Extended Features)**
- **Flask 3.0.3** - Web application framework
- **Flask-RESTful 0.3.10** - REST API development
- **Flask-SQLAlchemy 3.1.1** - ORM for database operations
- **Flask-Migrate 4.0.7** - Database migration management

### **Database Management**
- **Alembic 1.13.3** - Database migration tool
- **SQLAlchemy 2.0.29** - SQL toolkit and ORM

### **Development Tools**
- **pipenv 2024.0.1** - Virtual environment and dependency management
- **virtualenv 20.26.3** - Python virtual environments

### **Utilities**
- **requests 2.32.3** - HTTP library for API calls
- **logging** (built-in) - Application logging and monitoring
- **JSON** (built-in) - Configuration file handling

### **Project Structure**
```
ingestion/
├── config/          # JSON configuration files
├── data/            # CSV source data
├── logs/            # Application logs
├── src/             # Source code (ETL logic)
├── tests/           # Unit tests
└── requirements.txt # Python dependencies
```

---

## Step-by-Step Build Process

### **Phase 1: Project Setup & Configuration**

#### Step 1: Create Project Structure
```
ingestion/
├── config/          # Configuration files
├── data/            # Source data files
├── logs/            # Application logs
├── src/             # Source code
│   └── readers/     # Data readers
├── tests/           # Unit tests
└── requirements.txt # Python dependencies
```

#### Step 2: Install Dependencies (`requirements.txt`)
```txt
pandas          # Data manipulation
psycopg2-binary # PostgreSQL connection
pytest          # Testing framework
pytest-cov      # Code coverage
```

**Command:** `pip install -r requirements.txt`

---

### **Phase 2: Extract - Build the CSV Reader**

#### Step 3: Create CSV Reader (`src/readers/csv_read.py`)

**Purpose:** Load CSV files into pandas DataFrames

**What it does:**
- Reads CSV files using pandas
- Logs data shape and column names
- Handles file not found errors

**Key Function:**
```python
def read_csv(file_path):
    df = pd.read_csv(file_path)
    logger.info(f"Loaded {len(df)} rows")
    return df
```

**Input:** `data/real_estate.csv` (9,129 records)  
**Output:** Pandas DataFrame with raw data

---

### **Phase 3: Transform - Build Data Cleaning**

#### Step 4: Create Cleaning Functions (`src/clean.py`)

**Purpose:** Standardize and clean raw data

**What it does:**

1. **`rename_columns(df)`** - Makes columns database-friendly
   - Converts: `"data.date"` → `"data_date"`
   - Converts: `"location.id"` → `"location_id"`
   - Result: Clean snake_case column names

2. **`strip_whitespace(df)`** - Removes extra spaces
   - Strips leading/trailing whitespace from all string columns
   - Prevents duplicate entries due to spacing

3. **`handle_missing_values(df)`** - Converts fake nulls
   - Replaces `"0"` in `data_date` with `None` (NULL)
   - Identifies truly missing data

4. **`convert_date_format(df)`** - Parses dates
   - Converts strings to proper datetime objects
   - Uses `errors='coerce'` to set invalid dates to NULL
   - Logs: "4,110 valid dates, 5,019 null dates"

**Flow:**
```
Raw CSV → Rename → Strip Spaces → Handle NULLs → Convert Dates → Clean DataFrame
```

---

### **Phase 4: Validate - Build Data Quality Rules**

#### Step 5: Create Validation Functions (`src/validate.py`)

**Purpose:** Enforce data quality rules and reject bad records

**What it does:**

1. **`validate_required_fields(df)`**
   - Checks: `location_id`, `city`, `state` must not be NULL/empty
   - Returns: (valid_df, rejected_df)
   - Tracks rejection reason for each failed record

2. **`validate_numeric_ranges(df)`**
   - Checks: `parking_spaces` must be ≥ 0 (no negatives)
   - Allows NULL values (optional field)

3. **`validate_null_values(df)`**
   - Checks critical columns for NULL: `data_date`, `property_type`, `zip_code`, `ownership_type`, `address_line1`
   - Rejects records with NULL in any critical field
   - **This catches the 5,019 records with invalid dates**

4. **`remove_duplicates(df, primary_key)`**
   - Removes duplicate `location_id` entries
   - Keeps first occurrence, rejects rest

**Each function returns:** `(valid_records, rejected_records)`

---

#### Step 6: Orchestrate All Validations (`src/rules.py`)

**Purpose:** Run all validation rules in sequence

**What it does:**

**`apply_all_validations(df, primary_key)`**
```python
1. Start with all records
2. Run validate_required_fields()    → Split valid/rejected
3. Run validate_numeric_ranges()     → Split valid/rejected
4. Run validate_null_values()        → Split valid/rejected (catches 5,019)
5. Run remove_duplicates()           → Split valid/rejected
6. Combine all rejected records
7. Return (final_valid_df, all_rejected_df)
```

**Input:** 9,129 cleaned records  
**Output:** 
- ✅ 4,110 valid records (45%)
- ❌ 5,019 rejected records (55%)

---

### **Phase 5: Load - Build Database Loader**

#### Step 7: Create Database Functions (`src/load.py`)

**Purpose:** Connect to PostgreSQL and load data

**What it does:**

1. **`get_db_connection(db_url)`**
   - Establishes connection to PostgreSQL
   - Uses connection string: `postgresql://user:pass@host:port/database`
   - Returns connection object

2. **`create_tables(conn)`**
   - Creates `stg_real_estate` table (15 columns, location_id as PK)
   - Creates `stg_rejects` table (stores rejected records as JSONB)
   - Creates `v_rejects_expanded` view (expands JSONB into columns)
   - Grants permissions on view
   - Uses `CREATE TABLE IF NOT EXISTS` (idempotent)

3. **`load_to_staging(conn, df, table_name, pk_column, batch_size=1000)`**
   - Loads valid records into `stg_real_estate`
   - Uses **UPSERT** logic: `INSERT ... ON CONFLICT DO UPDATE`
   - Batch processing: Splits 4,110 records into batches of 1,000
   - If duplicate location_id exists, UPDATE the record
   - Transaction management: Commits after each batch

4. **`load_rejected(conn, rejected_df, source_name)`**
   - Converts each rejected record to JSON
   - Stores in `stg_rejects` with rejection_reason
   - Preserves complete raw data in JSONB format
   - Adds timestamp for audit trail

**Database Schema Created:**

**Table: stg_real_estate** (🥈 Silver Layer)
```sql
- location_id (VARCHAR, PRIMARY KEY)
- city, state, county, address_line1, zip_code
- data_date (TIMESTAMP)
- property_type, ownership_type, status
- parking_spaces (INTEGER)
- congressional_district, region_id
- ada_accessible, ansi_usable
```

**Table: stg_rejects** (🟤 Bronze Layer)
```sql
- id (SERIAL, PRIMARY KEY)
- source_name (VARCHAR) - e.g., 'real_estate_csv'
- raw_data (JSONB) - Complete rejected record
- rejection_reason (TEXT) - Why it failed
- rejected_at (TIMESTAMP) - When it was rejected
```

**View: v_rejects_expanded**
- Expands JSONB into 15 columns for easy querying
- Allows: `SELECT city, state FROM v_rejects_expanded WHERE rejection_reason LIKE '%date%'`

---

### **Phase 6: Orchestration - Build Main Pipeline**

#### Step 8: Create Pipeline Orchestrator (`src/main.py`)

**Purpose:** Tie all pieces together into one executable pipeline

**What it does:**

**`run_pipeline(config_path)`** - Main ETL function
```python
1. Load configuration from config/sources.json
2. Extract: Read CSV using read_csv()
3. Transform - Clean:
   - rename_columns()
   - strip_whitespace()
   - handle_missing_values()
   - convert_date_format()
4. Transform - Validate:
   - apply_all_validations()
5. Load:
   - get_db_connection()
   - create_tables()
   - load_to_staging() for valid records
   - load_rejected() for rejected records
6. Close database connection
7. Log summary statistics
```

**Execution:** `python src/main.py`

**Console Output:**
```
======================================================================
START ETL PIPELINE
======================================================================

[1/5] Loading configuration...
 Configuration loaded
 Source: data/real_estate.csv

[2/5] Extracting Data...
 Extracted 9129 rows

[3/5] Transform Data...
 Cleaned 9129 rows

[4/5] Validating Data...
 Required fields: 9129 valid, 0 rejected
 Numeric ranges: 9129 valid, 0 rejected
 NULL validation: 4110 valid, 5019 rejected  ← Main rejection point
 Duplicates removed: 0

[5/5] Loading to Database...
 Connected to database
 Tables created
 Loaded 4110 valid records to stg_real_estate
 Loaded 5019 rejected records to stg_rejects

======================================================================
ETL PIPELINE COMPLETE ✅
======================================================================
Total records processed: 9129
Valid records loaded: 4110
Rejected records logged: 5019
Success rate: 45.0%
```

---

### **Phase 7: Configuration - Make It Flexible**

#### Step 9: Create Config Loader (`src/config.py`)

**Purpose:** Externalize configuration for easy changes

**What it does:**

**`load_config(config_path)`**
- Reads `config/sources.json`
- Returns configuration dictionary
- Allows changing source files without code changes

**Configuration File: `config/sources.json`**
```json
{
  "sources": [
    {
      "name": "real_estate_csv",
      "type": "csv",
      "path": "data/real_estate.csv",
      "target_table": "stg_real_estate",
      "primary_key": "location_id"
    }
  ],
  "defaults": {
    "db_url": "postgresql://postgres:password@localhost:5432/real_estate_db"
  }
}
```

---

### **Phase 8: Logging - Add Observability**

#### Step 10: Create Logger (`src/utils/__init__.py`)

**Purpose:** Track pipeline execution and debug issues

**What it does:**

**Logger Configuration:**
- Creates `logs/` directory if it doesn't exist
- Writes to `logs/pipeline.log`
- Also outputs to console
- Format: `2025-12-10 10:30:45 - etl_pipeline - INFO - Message`

**Used Throughout:**
```python
from utils import logger

logger.info("Pipeline started")
logger.error("Database connection failed")
```

**Log File Location:** `/Users/Marlon/Revature-Training/Data_Ingestion_Subsystem/logs/pipeline.log`

---

### **Phase 9: Testing - Ensure Quality**

#### Step 11: Build Comprehensive Test Suite

**Test Files Created:**

1. **`tests/test_clean.py`** (8 tests)
   - Tests rename_columns, strip_whitespace, handle_missing_values, convert_date_format
   - Validates edge cases (empty DataFrames, missing columns)

2. **`tests/test_validate.py`** (12 tests)
   - Tests required fields validation
   - Tests numeric range validation
   - Tests duplicate removal

3. **`tests/test_null_validation.py`** (8 tests)
   - Tests NULL validation on critical columns
   - Validates rejection reasons are tracked

4. **`tests/test_rules.py`** (7 tests)
   - Tests orchestration of all validation rules
   - Tests multiple validation failures

5. **`tests/test_load.py`** (17 tests)
   - Tests database connection
   - Tests table creation
   - Tests UPSERT logic
   - Tests batch processing
   - Tests error handling and rollbacks

6. **`tests/test_main.py`** (4 tests)
   - Tests full pipeline execution
   - Tests error scenarios (config errors, DB errors)
   - Tests connection cleanup

**Run Tests:**
```bash
pytest                                    # Run all tests
pytest --cov=src                         # With coverage report
pytest tests/test_clean.py -v           # Run specific test file
```

**Coverage Results:**
```
src/clean.py        100%
src/validate.py     100%
src/rules.py         97%
src/load.py         100%
src/main.py         100%
TOTAL                93%
```

---

## How the Pipeline Executes

### Complete Data Flow (Start to Finish)

```
1. USER RUNS: python src/main.py
   ↓

2. main.py calls run_pipeline()
   ↓

3. EXTRACT PHASE
   config.py loads config/sources.json
   csv_read.py reads data/real_estate.csv
   → DataFrame with 9,129 rows
   ↓

4. TRANSFORM PHASE (Clean)
   clean.py: rename_columns()
   → Columns: "data.date" becomes "data_date"
   
   clean.py: strip_whitespace()
   → Remove extra spaces
   
   clean.py: handle_missing_values()
   → Convert "0" to NULL in data_date
   
   clean.py: convert_date_format()
   → Parse dates, 5,019 become NULL
   ↓

5. TRANSFORM PHASE (Validate)
   rules.py: apply_all_validations()
     ├─ validate.py: validate_required_fields()
     │  → 9,129 pass (location_id, city, state present)
     │
     ├─ validate.py: validate_numeric_ranges()
     │  → 9,129 pass (no negative parking_spaces)
     │
     ├─ validate.py: validate_null_values()
     │  → 4,110 pass, 5,019 rejected (NULL data_date)
     │
     └─ validate.py: remove_duplicates()
        → 0 duplicates removed
   
   Result: valid_df (4,110), rejected_df (5,019)
   ↓

6. LOAD PHASE
   load.py: get_db_connection()
   → Connect to PostgreSQL
   
   load.py: create_tables()
   → Create stg_real_estate, stg_rejects, v_rejects_expanded
   
   load.py: load_to_staging(valid_df)
   → INSERT 4,110 records into stg_real_estate
   → Batch 1: rows 1-1000
   → Batch 2: rows 1001-2000
   → Batch 3: rows 2001-3000
   → Batch 4: rows 3001-4000
   → Batch 5: rows 4001-4110
   
   load.py: load_rejected(rejected_df)
   → INSERT 5,019 records into stg_rejects as JSONB
   ↓

7. COMPLETE
   main.py closes database connection
   Logger outputs summary statistics
   ✅ Pipeline Complete
```

---

## File-by-File Explanation

### Configuration Files

**`config/sources.json`**
- Defines data sources
- Database connection string
- Target table names
- Primary key columns

**`requirements.txt`**
- Python package dependencies
- pandas, psycopg2-binary, pytest, pytest-cov

---

### Source Code (`src/`)

**`src/main.py`** - Pipeline Orchestrator
- Entry point: `python src/main.py`
- Coordinates all ETL steps
- Error handling and logging
- 103 lines, 100% test coverage

**`src/config.py`** - Configuration Loader
- Loads JSON config files
- Returns configuration dictionary
- 26 lines, 44% coverage (error paths not tested)

**`src/readers/csv_read.py`** - CSV Reader
- Reads CSV files into DataFrames
- Logs data statistics
- 25 lines, 45% coverage (main path tested)

**`src/clean.py`** - Data Cleaning
- 4 functions: rename, strip, handle_missing, convert_date
- Makes data database-ready
- 200 lines, 100% test coverage

**`src/validate.py`** - Validation Rules
- 4 functions: required fields, numeric ranges, null values, duplicates
- Each returns (valid_df, rejected_df)
- 208 lines, 100% test coverage

**`src/rules.py`** - Validation Orchestrator
- Chains all validation functions
- Combines rejected records
- Logs validation statistics
- 57 lines, 97% test coverage

**`src/load.py`** - Database Loader
- Database connection management
- Table creation with DDL
- UPSERT logic for valid records
- JSONB storage for rejected records
- 216 lines, 100% test coverage

**`src/utils/__init__.py`** - Logger Configuration
- Sets up file and console logging
- Creates logs directory
- Configures log format
- 33 lines, 100% coverage

---

### Test Files (`tests/`)

All test files use pytest framework with mocking:
- Mock database connections
- Mock file I/O
- Test edge cases
- Verify error handling

**56 total tests, all passing ✅**

---

### Data Files

**`data/real_estate.csv`**
- Source data: 9,130 lines (1 header + 9,129 records)
- Real estate property information
- Contains invalid data (55% have NULL dates)

---

### Output

**`logs/pipeline.log`**
- Records every pipeline execution
- Timestamped entries
- Error messages with stack traces

**Database Tables:**
- `stg_real_estate` - 4,110 valid records
- `stg_rejects` - 5,019 rejected records
- `v_rejects_expanded` - View for analyzing rejects

---

## Data Flow Journey

### Example: Single Record Journey

**Original CSV Row:**
```csv
"0","LEASED","24","ACTIVE","BUILDING","3","NV8371","9","Will Conform","23669","LAS VEGAS","CLARK COUNTY","6750 VIA AUSTI PKY","NV","891193565"
```

**After Extract (`csv_read.py`):**
```python
{
  "data.date": "0",
  "data.owned or leased": "LEASED",
  "data.parking spaces": "24",
  "data.status": "ACTIVE",
  "data.type": "BUILDING",
  "location.congressional district": "3",
  "location.id": "NV8371",
  ...
}
```

**After Clean - Rename (`clean.py`):**
```python
{
  "data_date": "0",
  "ownership_type": "LEASED",
  "parking_spaces": "24",
  "status": "ACTIVE",
  "property_type": "BUILDING",
  "congressional_district": "3",
  "location_id": "NV8371",
  ...
}
```

**After Clean - Handle Missing (`clean.py`):**
```python
{
  "data_date": None,  # ← "0" converted to NULL
  "ownership_type": "LEASED",
  ...
}
```

**After Validate - NULL Check (`validate.py`):**
```
❌ REJECTED
Reason: "NULL value in data_date"
```

**After Load (`load.py`):**
Inserted into `stg_rejects`:
```sql
INSERT INTO stg_rejects (source_name, raw_data, rejection_reason)
VALUES (
  'real_estate_csv',
  '{"location_id": "NV8371", "city": "LAS VEGAS", "data_date": null, ...}'::jsonb,
  'NULL value in data_date'
);
```

**Query the Reject:**
```sql
SELECT * FROM v_rejects_expanded WHERE location_id = 'NV8371';
```

---

## Key Architectural Decisions

### 1. **Why Medallion Architecture (Bronze/Silver/Gold)?**
- **Bronze (stg_rejects):** Preserve raw data for audit/replay
- **Silver (stg_real_estate):** Clean, validated, analytics-ready
- **Gold (future):** Aggregated business metrics

### 2. **Why JSONB for Rejected Records?**
- Schema flexibility (source data may change)
- Preserve complete record without predefined structure
- PostgreSQL JSONB is queryable and indexable
- Faster than text JSON

### 3. **Why UPSERT Instead of INSERT?**
- Handle reprocessing (run pipeline multiple times)
- Update existing records if source data changes
- Idempotent operations (can run repeatedly safely)

### 4. **Why Batch Processing?**
- Performance: 1,000 rows at a time
- Memory efficiency: Don't load all data at once
- Transaction control: Commit in batches

### 5. **Why Separate Valid/Rejected Tables?**
- Data quality visibility
- Audit trail for debugging
- Business can analyze rejection patterns
- Don't pollute clean data with bad records

### 6. **Why 93% Coverage Instead of 100%?**
- Focused on business logic
- Some error paths are hard to test (network failures)
- Diminishing returns above 90%

---

## Common Interview Questions & Answers

**Q: How does your pipeline handle bad data?**  
A: We use a multi-stage validation approach. Invalid records are captured at each stage (required fields, NULL checks, numeric ranges, duplicates) and stored in `stg_rejects` with the specific rejection reason. This gives us full visibility into data quality issues.

**Q: What happens if the pipeline fails halfway?**  
A: We use PostgreSQL transactions with rollback support. If any batch fails during the load phase, we rollback that batch and log the error. The database remains in a consistent state. Each function has try-catch blocks with proper error logging.

**Q: Why is your success rate only 45%?**  
A: 55% of source records have invalid dates (value "0" instead of actual dates). Our validation correctly rejects these. This is a data quality issue with the source system. The rejects table allows us to report this back to the data provider.

**Q: How would you scale this to billions of records?**  
A: 
1. Increase batch size (currently 1,000)
2. Add parallel processing (multiprocessing/threading)
3. Use COPY command instead of INSERT for bulk loads
4. Partition tables by date ranges
5. Add incremental load logic (only process new/changed records)

**Q: How do you ensure data quality?**  
A:
1. Required field validation (location_id, city, state)
2. Data type validation (dates, numbers)
3. NULL checks on critical columns
4. Range validation (no negative parking spaces)
5. Duplicate detection
6. Comprehensive logging
7. 93% test coverage

**Q: Walk me through how one record flows through your pipeline.**  
A: [Refer to "Example: Single Record Journey" section above]

---

## Running the Project

### Setup
```bash
cd /Users/Marlon/Revature-Training/Data_Ingestion_Subsystem/ingestion
pip install -r requirements.txt
```

### Run Pipeline
```bash
python src/main.py
```

### Run Tests
```bash
pytest
pytest --cov=src --cov-report=term-missing
```

### Query Results
```sql
-- Valid records
SELECT * FROM stg_real_estate LIMIT 10;

-- Rejected records (raw JSONB)
SELECT * FROM stg_rejects LIMIT 10;

-- Rejected records (expanded columns)
SELECT * FROM v_rejects_expanded LIMIT 10;

-- Rejection summary
SELECT rejection_reason, COUNT(*) 
FROM stg_rejects 
GROUP BY rejection_reason;
```

---

## Project Statistics

- **Total Python Files:** 16
- **Lines of Code:** ~1,200
- **Test Coverage:** 93%
- **Tests:** 56 (all passing)
- **Source Records:** 9,129
- **Valid Records:** 4,110 (45%)
- **Rejected Records:** 5,019 (55%)
- **Database Tables:** 2 (stg_real_estate, stg_rejects)
- **Database Views:** 1 (v_rejects_expanded)

---

## What Makes This Production-Ready?

✅ **Error Handling** - Try-catch blocks, transaction rollbacks  
✅ **Logging** - Complete audit trail  
✅ **Testing** - 93% coverage  
✅ **Modularity** - Reusable functions  
✅ **Configuration** - Externalized settings  
✅ **Data Quality** - Multi-stage validation  
✅ **Idempotency** - Can run multiple times safely  
✅ **Documentation** - README, comments, this walkthrough  

---

**This walkthrough explains the complete journey from raw CSV to production database, showing exactly how each component works together to create a robust ETL pipeline.**
