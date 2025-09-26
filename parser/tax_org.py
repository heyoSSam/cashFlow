import pdfplumber
import camelot
import re
from datetime import date
from fastapi import HTTPException

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

    start_date, end_date = date_find_decl910(file_path)
    bin = bin_find_decl(file_path)
    match = re.search(r'Доход(?:\s+)?([\d\s]+)', text)
    if match:
        numbers_str = match.group(1) or ""
        numbers_str = numbers_str.strip()
        if numbers_str:
            number = int(''.join(numbers_str.split()))
            return [{"ep": round(number / 6, 3), "start_date": start_date.isoformat(), "end_date": end_date.isoformat(), "bin": bin}]
        else:
            return [{"ep": 0, "start_date": start_date.isoformat(), "end_date": end_date.isoformat(), "bin": bin}]
    else:
        raise ValueError("Доход не найден")

def table_find_decl220(file_path):
    with pdfplumber.open(file_path) as pdf:
        page = pdf.pages[0]
        text = page.extract_text()

    start_date, end_date = date_find_decl220(file_path)
    bin = bin_find_decl(file_path)
    match = re.search(r'Доход от реализации(?:\s+)?([\d\s]+)', text)
    if match:
        numbers_str = match.group(1) or ""
        numbers_str = numbers_str.strip()
        if numbers_str:
            number = int(''.join(numbers_str.split()))
            return [{"ep": round(number / 12, 3), "start_date": start_date.isoformat(), "end_date": end_date.isoformat(), "bin": bin}]
        else:
            return [{"ep": 0, "start_date": start_date.isoformat(), "end_date": end_date.isoformat(), "bin": bin}]
    else:
        return [{
            "ep": 0,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "bin": bin,
            "error": "Доход не найден"
        }]

def date_find_decl910(file_path):
    with pdfplumber.open(file_path) as pdf:
        page = pdf.pages[0]
        text = page.extract_text()

    match = re.search(r"полугодие\s*(\d)\s*год\s*((?:\d\s*){4})", text, re.IGNORECASE)
    if match:
        half = int(match.group(1))
        year_str = match.group(2).replace(" ", "")
        year = int(year_str)

        if half == 1:
            start_date = date(year, 1, 1)
            end_date = date(year, 6, 30)
        elif half == 2:
            start_date = date(year, 7, 1)
            end_date = date(year, 12, 31)
        else:
            return None, None

        return start_date, end_date
    else:
        return None, None

def date_find_decl220(file_path):
    with pdfplumber.open(file_path) as pdf:
        page = pdf.pages[0]
        text = page.extract_text()

    match = re.search(r"год\s*[:\-]?\s*((?:\d\s*){4})", text, re.IGNORECASE)
    if match:
        year_str = match.group(1).replace(" ", "")
        year = int(year_str)
        start_date = date(year, 1, 1)
        end_date = date(year, 12, 31)
        return start_date, end_date
    else:
        return None, None

def bin_find_decl(file_path):
    with pdfplumber.open(file_path) as pdf:
        page = pdf.pages[0]
        text = page.extract_text()

    match = re.search(r"(?:ИИН\s*\(БИН\)|ИИН|БИН)\s*[:\-]?\s*([\d\s]+)", text, re.IGNORECASE)
    if match:
        number_str = match.group(1)
        number = "".join(number_str.split())
        number = number[:12]
        return number
    raise HTTPException(status_code=400, detail="ИИН/БИН не найден")