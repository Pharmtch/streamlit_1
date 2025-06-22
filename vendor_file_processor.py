
import pandas as pd
import os
import glob
from datetime import datetime

standard_schema = [
    "NDC", "Name", "Form", "Pack Size", "Manufacturer",
    "Qty", "Purchased Price", "Platform", "Seller", 
    "Date of Purchase", "Invoice Number"
]

field_synonyms = {
    "NDC": ["NDC Number", "NDC", "Selling Unit NDC", "Inner NDC Nbr", "UPC", "NDCText", "Item", "Material Number (Numeric)"],
    "Name": ["Name", "Product Name", "Material Name", "ITEM DESCRIPTION", "DrugName", "Description", "Material Description"],
    "Form": ["Form", "Dosage Form"],
    "Pack Size": ["Pack Size", "PackageSize", "Size", "Size/dimensions"],
    "Manufacturer": ["Manufacturer", "MFR", "Vendor Name", "Hist Vendor Name"],
    "Qty": ["Qty", "Quantity", "Quantity Ordered", "Net Qty", "Qty_Shipped", "Actual Invoice Quantity", "QTY"],
    "Purchased Price": ["Purchased price", "Item Price", "Invoice_Price", "Price", "Net Val Of Billing Item In DC"],
    "Seller": ["Seller", "Source Name", "WholeSalerNameText1"],
    "Date of Purchase": ["Date", "DATE", "Order Date", "Invc Date", "Invoice Date", "CheckoutDate", "Date of sale", "Billing Date"],
    "Invoice Number": ["Invoice Number", "Invoice", "Invc Nbr", "Order #", "Order Id", "INV/CRDT#", "Invoice No.", "Billing Document"]
}

required_fields = ["NDC", "Name", "Qty", "Date of Purchase"]

synonym_to_standard = {
    synonym: std_col
    for std_col, synonyms in field_synonyms.items()
    for synonym in synonyms
}

file_readers = {
    ".csv": pd.read_csv,
    ".xlsx": pd.read_excel,
    ".xls": pd.read_excel
}

def format_ndc(ndc):
    digits = ''.join(filter(str.isdigit, str(ndc)))
    if len(digits) < 11:
        digits = digits.zfill(11)
    if len(digits) == 11:
        return f"{digits[:5]}-{digits[5:9]}-{digits[9:]}"
    return "INVALID"

def process_vendor_file(filepath):
    ext = os.path.splitext(filepath)[1].lower()
    filename = os.path.basename(filepath)
    platform = "kinray" if filename.lower() == "kinray.csv" else filename.split()[0].lower()

    reader = file_readers.get(ext)
    if not reader:
        return None, f"❌ Unsupported file format: {filepath}", {}

    try:
        df = reader(filepath)
    except Exception as e:
        return None, f"❌ Error reading {filepath}: {e}", {}

    df.columns = df.columns.str.strip()
    import re

mapped = {col: None for col in standard_schema}

# Try direct synonym match
for col in df.columns:
    std_col = synonym_to_standard.get(col.strip())
    if std_col and mapped[std_col] is None:
        mapped[std_col] = col

# Regex fallback for unmapped columns
for std_col, pattern in intelligent_column_patterns.items():
    if mapped[std_col] is None:
        for col in df.columns:
            if re.search(pattern, col.strip(), re.IGNORECASE):
                mapped[std_col] = col
                break

    for col in df.columns:
        std_col = synonym_to_standard.get(col.strip())
        if std_col and mapped[std_col] is None:
            mapped[std_col] = col

    missing_required = [f for f in required_fields if mapped[f] is None]

    standardized = pd.DataFrame({
        col: df[mapped[col]] if mapped[col] else ""
        for col in standard_schema
    })

    if mapped.get("NDC"):
        standardized["NDC"] = df[mapped["NDC"]].astype(str).str.strip()
    else:
        def resolve_ndc(row):
            for col in ["Selling Unit NDC", "Inner NDC Nbr", "UPC"]:
                val = row.get(col)
                if pd.notna(val) and str(val).strip():
                    return str(val).strip()
            return ""
        standardized["NDC"] = df.apply(resolve_ndc, axis=1)

    standardized["NDC"] = standardized["NDC"].apply(format_ndc)
    standardized["Platform"] = platform
    standardized["Qty"] = pd.to_numeric(standardized["Qty"], errors="coerce").fillna(0).round().astype(int)
    standardized["Purchased Price"] = pd.to_numeric(
        standardized["Purchased Price"].astype(str).str.replace(r"[\$,]", "", regex=True).str.strip(),
        errors="coerce"
    ).where(lambda x: x.notna(), "N/A")

    standardized["Date of Purchase"] = pd.to_datetime(standardized["Date of Purchase"], errors="coerce").dt.date
    standardized["Invoice Number"] = standardized["Invoice Number"].astype(str).str.replace(r"\.0$", "", regex=True)

    log_msg = f"✅ Processed: {filename} | Missing: {', '.join(missing_required) if missing_required else 'None'}"
    return standardized, log_msg, mapped
