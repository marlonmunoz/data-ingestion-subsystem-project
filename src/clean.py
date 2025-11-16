def clean_whitespace(df):
    """
    Strip leading/trailing whitespace from all string columns
    """
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.strip()
    return df

def clean_casing(df):
    """
    Standardize casing for string columns (e.g. city names)
    """
    if "city" in df.columns:
        df["city"] = df["city"].str.title()
    return df