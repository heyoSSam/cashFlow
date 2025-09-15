from dotenv import load_dotenv
from fastapi import APIRouter, UploadFile, File
from starlette.responses import PlainTextResponse
import tempfile, os

from parser.kaspi_bank import table_find_kaspi_debt
from parser.tax_org import table_find_tax_sp

load_dotenv()
main_router = APIRouter(
    prefix="/approval",
)

@main_router.post("/taxCheck", response_class=PlainTextResponse)
async def root(file: UploadFile = File(...)):
    content = await file.read()

    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
        tmp.write(content)
        temp_path = tmp.name

    try:
        df = table_find_tax_sp(temp_path)

        value_col = df.columns[1]

        values = df[value_col].astype(str).str.extract(r"(\d+)")[0].astype(int)

        if (values == 0).all():
            return "Задолженность отсутствует"
        else:
            return "Задолженность имеется"
    finally:
        os.unlink(temp_path)

@main_router.post("/debtCheckKaspi", response_class=PlainTextResponse)
async def root(file: UploadFile = File(...)):
    content = await file.read()

    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
        tmp.write(content)
        temp_path = tmp.name

    try:
        df = table_find_kaspi_debt(temp_path)

        value_col = df.columns[2]

        values = df[value_col].astype(str).str.strip().str.lower()

        if (values == "отсутствует").all():
            return "Задолженность отсутствует"
        else:
            return "Задолженность имеется"

    finally:
        os.unlink(temp_path)
