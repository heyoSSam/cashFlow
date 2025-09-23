import camelot
import pdfplumber
import re
import pandas as pd

def table_find_bcc_vp(file_path):
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
        return int(match.group(1))
    else:
        return None

if __name__ == "__main__":
    print(bin_find_bcc_vp("C:\\Users\PW.DESKTOP-BIOB19V\Desktop\декл+выписка\М-03-96БР-2025\выписка бцк.pdf"))