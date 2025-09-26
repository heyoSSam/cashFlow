import camelot
import pdfplumber
import re
import pandas as pd
from datetime import datetime

def table_find_bcc_vp_ur(file_path):
    tables = camelot.read_pdf(file_path, pages='all')

    if not tables:
        raise ValueError("No tables found in PDF")

    all_dfs = []

    for i, table in enumerate(tables[0:]):
        all_dfs.append(table.df)

    if not all_dfs:
        raise ValueError("All tables are empty")

    combined_df = pd.concat(all_dfs, ignore_index=True)
    combined_df.columns = (
        combined_df.iloc[0]
        .str.replace("\n", " ", regex=False)
        .str.split("/")
        .str[-1]
        .str.strip()
    )

    combined_df = combined_df.drop(0).reset_index(drop=True)
    return combined_df[:-1]

def bin_find_bcc_vp(file_path):
    with pdfplumber.open(file_path) as pdf:
        page = pdf.pages[0]
        text = page.extract_text()

    match = re.search(r'(?:ЖСН|ИИН)\s*[:\-]?\s*(\d+)', text, re.IGNORECASE)
    if match:
        return match.group(1)
    else:
        return None

def date_find_bcc_vp(file_path):
    with pdfplumber.open(file_path) as pdf:
        page = pdf.pages[0]
        text = page.extract_text()

    match = re.search(r"Движения по счету\s*[cC]\s*(\d{2}\.\d{2}\.\d{4})\s*по\s*(\d{2}\.\d{2}\.\d{4})", text)
    if match:
        start_date = datetime.strptime(match.group(1), "%d.%m.%Y").date()
        end_date = datetime.strptime(match.group(2), "%d.%m.%Y").date()
        return start_date, end_date
    else:
        return {"error": "Период не найден"}
