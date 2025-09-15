import camelot
import pandas as pd
import pdfplumber

def table_find_kaspi_vp(file_path):
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

def table_find_kaspi_debt(file_path):
    with pdfplumber.open(file_path) as pdf:
        page = pdf.pages[0]
        debt_table = page.extract_table()

    df = pd.DataFrame(debt_table[1:], columns=debt_table[0])

    return df

if __name__ == "__main__":
    print(table_find_kaspi_debt("../kaspi_debt.pdf").columns.tolist())