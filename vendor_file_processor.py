import pandas as pd
import os
import glob
from datetime import datetime

# Standard schema and field synonyms
STANDARD_SCHEMA = [
    "NDC", "Name", "Form", "Pack Size", "Manufacturer",
    "Qty", "Purchased Price", "Platform", "Seller", 
    "Date of Purchase", "Invoice Number"
]

FIELD_SYNONYMS = {
    "NDC": ["NDC", "Selling Unit NDC", "Inner NDC Nbr", "NDCText", "Item", "NDC Number", "Material Number (Numeric)"],
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

REQUIRED_FIELDS = {"NDC", "Name", "Qty", "Date of Purchase"}

# Reverse lookup for synonyms
SYNONYM_TO_STANDARD = {
    synonym: std for std, syns in FIELD_SYNONYMS.items() for synonym in syns
}

FILE_READERS = {
    ".csv": pd.read_csv,
    ".xlsx": pd.read_excel,
    ".xls": pd.read_excel
}

def detect_platform(filename):
    name = filename.lower()
    return "kinray" if name == "kinray.csv" else name.split()[0]

def map_columns(columns):
    mapped = {col: None for col in STANDARD_SCHEMA}
    for col in columns:
        std_col = SYNONYM_TO_STANDARD.get(col.strip())
        if std_col and not mapped[std_col]:
            mapped[std_col] = col
    return mapped

def process_vendor_file(filepath):
    ext = os.path.splitext(filepath)[1].lower()
    filename = os.path.basename(filepath)
    reader = FILE_READERS.get(ext)

    if not reader:
        return None, f"❌ Unsupported file format: {filepath}", {}

    try:
        df = reader(filepath)
    except Exception as e:
        return None, f"❌ Error reading {filepath}: {e}", {}

    df.columns = df.columns.str.strip()
    mapped = map_columns(df.columns)
    missing = [col for col in REQUIRED_FIELDS if not mapped[col]]

    # Standardize
    standardized = pd.DataFrame({
        col: df.get(mapped[col], "") for col in STANDARD_SCHEMA
    })

    # Transformations
    standardized["Platform"] = detect_platform(filename)
    standardized["Qty"] = pd.to_numeric(standardized["Qty"], errors="coerce").fillna(0).round().astype(int)

    standardized["Purchased Price"] = (
        standardized["Purchased Price"]
        .astype(str)
        .str.replace(r"[\$,]", "", regex=True)
        .str.strip()
    )
    standardized["Purchased Price"] = pd.to_numeric(standardized["Purchased Price"], errors="coerce").fillna("N/A")

    standardized["Date of Purchase"] = pd.to_datetime(standardized["Date of Purchase"], errors="coerce").dt.date
    standardized["Invoice Number"] = standardized["Invoice Number"].astype(str).str.replace(r"\.0$", "", regex=True)

    log_msg = f"✅ Processed: {filename} | Missing: {', '.join(missing) if missing else 'None'}"
    return standardized, log_msg, mapped

def process_all_vendor_files_with_mapping(folder_path=".", output_file="combined_standardized_output.csv"):
    logs, mappings, all_data = [], [], []
    extensions = [".csv", ".xlsx", ".xls"]

    files = [f for ext in extensions for f in glob.glob(os.path.join(folder_path, f"*{ext}"))]

    for file in files:
        df, log, mapping = process_vendor_file(file)
        logs.append(log)
        if mapping:
            mappings.append({"File": os.path.basename(file), **mapping})
        if df is not None:
            all_data.append(df)

    if all_data:
        combined = pd.concat(all_data, ignore_index=True)
        combined.to_csv(output_file, index=False)
        print(f"✅ Output saved to {output_file}")
    else:
        print("❌ No valid data found.")

    # Write logs and mappings
    log_path = os.path.join(folder_path, "processing_log.txt")
    with open(log_path, "w") as f:
        f.write("\n".join(logs))

    pd.DataFrame(mappings).to_csv(os.path.join(folder_path, "column_mapping_log.csv"), index=False)
    print(f"📝 Log saved to {log_path}")
    print(f"📄 Mapping log saved to column_mapping_log.csv")

if __name__ == "__main__":
    process_all_vendor_files_with_mapping("your_folder_path_here")  # Update path
