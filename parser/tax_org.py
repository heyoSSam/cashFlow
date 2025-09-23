import pdfplumber
import camelot
import re

def table_find_tax_sp(file_path):
    tables = camelot.read_pdf(file_path, pages='all', flavor='stream')

    if not tables:
        raise ValueError("No tables found in PDF")

    tax_table = tables[1].df
    tax_table = tax_table.drop([0,9]).reset_index(drop=True)
    tax_table = tax_table[tax_table[1] != ""].reset_index(drop=True)

    return tax_table

def table_find_decl910(file_path):
    with pdfplumber.open(file_path) as pdf:
        page = pdf.pages[0]
        text = page.extract_text()

    match = re.search(r'Доход(?:\s+)?([\d\s]+)', text)
    if match:
        numbers_str = match.group(1)
        number = int(''.join(numbers_str.split()))
        return {"ep": round(number / 6, 3)}
    else:
        raise ValueError("Доход не найден")