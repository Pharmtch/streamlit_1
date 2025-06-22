
import streamlit as st
import pandas as pd
import tempfile
import os
import zipfile
from io import BytesIO
from datetime import datetime

from vendor_file_processor import process_vendor_file, standard_schema

st.set_page_config(page_title="Vendor File Processor", layout="centered")
st.title("step2")

st.markdown("""
Upload multiple vendor files (CSV or Excel) and click **Process** to standardize and combine them into one report. 
Each file will be mapped dynamically, and logs will be generated for traceability.
""")

uploaded_files = st.file_uploader("Upload your vendor files", type=["csv", "xlsx"], accept_multiple_files=True)

if uploaded_files:
    if st.button("🚀 Process Files"):
        logs = []
        mappings = []
        all_data = []

        with tempfile.TemporaryDirectory() as tmpdir:
            for uploaded in uploaded_files:
                file_path = os.path.join(tmpdir, uploaded.name)
                with open(file_path, "wb") as f:
                    f.write(uploaded.read())

                df, log, mapping = process_vendor_file(file_path)
                logs.append(log)
                if mapping:
                    mappings.append({"File": uploaded.name, **mapping})
                if df is not None:
                    all_data.append(df)

            if all_data:
                combined = pd.concat(all_data, ignore_index=True)
                st.success("✅ Files processed successfully!")
                st.dataframe(combined.head())

                csv_out = combined.to_csv(index=False).encode("utf-8")
                st.download_button("⬇️ Download Combined CSV", csv_out, "combined_output.csv", "text/csv")

                mapping_df = pd.DataFrame(mappings)
                st.download_button("🗂️ Download Mapping Log", mapping_df.to_csv(index=False).encode("utf-8"), "column_mapping_log.csv", "text/csv")

                st.text_area("📋 Processing Log", "\n".join(logs), height=200)
            else:
                st.error("❌ No valid data extracted. Please check your files.")
