import pandas as pd
import pdfplumber
import re
from datetime import datetime


def table_find_halyk_vp(file_path):
    all_dfs = []

    with pdfplumber.open(file_path) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables()
            for table in tables:
                if not table:
                    continue
                df = pd.DataFrame(table)
                all_dfs.append(df)

    if not all_dfs:
        raise ValueError("No tables found in PDF")

    combined_df = pd.concat(all_dfs[1:], ignore_index=True)
    combined_df.columns = combined_df.iloc[0].str.replace("\n", " ", regex=False).str.strip()
    combined_df = combined_df.drop(0).reset_index(drop=True)

    if len(combined_df) > 0:
        combined_df = combined_df[:-1]

    return combined_df

def bin_find_halyk_vp(file_path):
    with pdfplumber.open(file_path) as pdf:
        page = pdf.pages[0]
        text = page.extract_text()

    match = re.search(r'(?:ИИН|БИН)\s*[:\-]?\s*(\d+)', text, re.IGNORECASE)
    if match:
        return match.group(1)
    else:
        return None

def date_find_halyk_vp(file_path):
    with pdfplumber.open(file_path) as pdf:
        page = pdf.pages[0]
        text = page.extract_text()

    match = re.search(
        r"Дата предыдущей операции:\s*(\d{2}\.\d{2}\.\d{4})\s*.*Дата последней операции:\s*(\d{2}\.\d{2}\.\d{4})",
        text
    )
    if match:
        start_date = datetime.strptime(match.group(1), "%d.%m.%Y").date()
        end_date = datetime.strptime(match.group(2), "%d.%m.%Y").date()
        return start_date, end_date
    else:
        return {"error": "Период не найден"}