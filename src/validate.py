import pandas as pd

def rename_columns(df, schema):
    # map raw column names to schema keys
    column_map = {
        "data.date": "data_date",
        "data.owned or leased": "ownership_type",
        "data.parking spaces": "parking_spaces",
        "data.status": "status",
        "data.type": "property_type",
        "location.congressional district": "congressional_district",
        "location.id": "location_id",
        "location.region id": "region_id",
        "data.disabilities.ADA Accessible": "ada_accessible",
        "data.disabilities.ansi usable": "ansi_usable",
        "location.address.city": "city",
        "location.address.county": "county",
        "location.address.line 1": "address_line1",
        "location.address.state": "state",
        "location.address.zip": "zip_code",
    }
    return df.rename(columns=column_map)


def cast_types(df, schema):
    """
    Cast DataFrame columns to types specified in the schema
    """
    for col, dtype in schema.items():
        if col not in df.columns:
            continue # Skip column not present
        if dtype == "int":
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        elif dtype == "float":
            df[col] = pd.to_numeric(df[col], errors="coerce")
        elif dtype == "date":
            df[col] = pd.to_datetime(df[col], errors="coerce")
        elif dtype == "str":
            df[col] = df[col].astype(str)
    return df

            
def drop_missing(df, required_fields):
    """
    Drop rows with missing values in any of the required fields
    """
    return df.dropna(subset=required_fields)

def apply_rules(df, rules):
    """
    Apply business rules to the DataFramr and return valid and rejected rows.
    """
    valid_mask = pd.Series([True] * len(df), index=df.index)
    for rule in rules:
        expr = rule["rule"]
        if ">=" in expr:
            col, _, val = expr.partition(">=")
            col = col.strip()
            valid_mask &= df[col].astype(float) >= float(val.strip())
        elif "NOT NULL" in expr:
            col = expr.replace("NOT NULL", "").strip()
            valid_mask &= df[col].notnull()
        elif "len(" in expr and ">" in expr:
            col = expr.split("(")[1].split(")")[0]
            val = expr.split(">")[-1].strip()
            valid_mask &= df[col].astype(str).str.len() > int(val)
        # Add more rule types as needed
    valid_df = df[valid_mask]
    rejected_df = df[~valid_mask]
    return valid_df, rejected_df