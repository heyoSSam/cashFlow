import pdfplumber

def find_debt_forte(file_path):
    with pdfplumber.open(file_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if text:
                if "задолженность по неоплаченным" in text.lower() and "отсутствует" in text.lower():
                    found = True
                    break

    return found