import camelot
import pandas as pd
import pdfplumber
import re
from datetime import datetime

def table_find_bereke_vp(file_path):
    tables = camelot.read_pdf(file_path, pages='all')

    if not tables:
        raise ValueError("No tables found in PDF")

    all_dfs = []

    for i, table in enumerate(tables[0:]):
        all_dfs.append(table.df)

    if not all_dfs:
        raise ValueError("All tables are empty")

    combined_df = pd.concat(all_dfs, ignore_index=True)
    combined_df.columns = combined_df.iloc[0].str.replace("\n", " ", regex=False).str.strip()
    combined_df = combined_df.drop(0).reset_index(drop=True)
    return combined_df[:-1]

# def bin_find_bereke_vp(file_path):
#     with pdfplumber.open(file_path) as pdf:
#         page = pdf.pages[0]
#         text = page.extract_text()
#
#     match = re.search()
#     if match:
#         return int(match.group(1))
#     else:
#         return None

def date_find_bereke_vp(file_path):
    with pdfplumber.open(file_path) as pdf:
        page = pdf.pages[0]
        text = page.extract_text()

    match = re.search(
        r"за период с\s*(\d{2}\.\d{2}\.\d{4})\s*по\s*(\d{2}\.\d{2}\.\d{4})",
        text,
        re.IGNORECASE
    )
    if match:
        start_date = datetime.strptime(match.group(1), "%d.%m.%Y").date()
        end_date = datetime.strptime(match.group(2), "%d.%m.%Y").date()
        return start_date, end_date
    else:
        return {"error": "Период не найден"}

if __name__ == "__main__":
    print(date_find_bereke_vp("C:\\Users\PW.DESKTOP-BIOB19V\Desktop\\test\M-12-24\\1\Справка БВУ выписка_Береке 2.pdf"))