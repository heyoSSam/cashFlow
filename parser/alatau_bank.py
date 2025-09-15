import camelot
import pandas as pd

def table_find_alatau_vp(file_path):
    tables = camelot.read_pdf(file_path, pages='all')

    if not tables:
        raise ValueError("No tables found in PDF")

    all_dfs = []

    for i, table in enumerate(tables[1:]):
        if not table.df.empty:
            if i == 0:
                df = table.df
            else:
                df = table.df.iloc[1:]
            all_dfs.append(df)

    if not all_dfs:
        raise ValueError("All tables are empty")

    combined_df = pd.concat(all_dfs, ignore_index=True)
    combined_df.columns = combined_df.iloc[0].str.replace("\n", " ", regex=False).str.strip()
    combined_df = combined_df.drop(0).reset_index(drop=True)
    return combined_df[:-1]