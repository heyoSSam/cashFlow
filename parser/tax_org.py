import camelot

def table_find_tax_sp(file_path):
    tables = camelot.read_pdf(file_path, pages='all', flavor='stream')

    if not tables:
        raise ValueError("No tables found in PDF")

    tax_table = tables[1].df
    tax_table = tax_table.drop([0,9]).reset_index(drop=True)
    tax_table = tax_table[tax_table[1] != ""].reset_index(drop=True)

    return tax_table