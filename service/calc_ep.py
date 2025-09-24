import re,os, importlib.util
from dotenv import load_dotenv

from fastapi import APIRouter, File, UploadFile, Form
from fastapi.responses import JSONResponse
from tempfile import NamedTemporaryFile

from langchain_experimental.agents import create_pandas_dataframe_agent
from langchain.agents.agent_types import AgentType
from langchain_google_genai import ChatGoogleGenerativeAI

from parser.bcc_bank import table_find_bcc_vp, bin_find_bcc_vp, date_find_bcc_vp
from parser.alatau_bank import table_find_alatau_vp, bin_find_alatau_vp, date_find_alatau_vp
from parser.forte_bank import table_find_forte_vp, bin_find_forte_vp, date_find_forte_vp
from parser.halyk_bank import table_find_halyk_vp, bin_find_halyk_vp, date_find_halyk_vp
from parser.kaspi_bank import table_find_kaspi_vp, bin_find_kaspi_vp, date_find_kaspi_vp
from parser.tax_org import table_find_decl910, table_find_decl220
from service.constants import PERCENTAGES

load_dotenv()
pre_router = APIRouter(
    prefix="/preapproval",
)

api_key = os.getenv("GOOGLE_API_KEY")

llm = ChatGoogleGenerativeAI(
    model="gemini-2.5-pro",
    temperature=0,
    google_api_key=api_key
)

def periods_overlap(start1, end1, start2, end2):
    return not (end1 < start2 or end2 < start1)


@pre_router.post("/ep")
async def root(
        files: list[UploadFile] = File(...),
        banks: list[str] = Form(...),
        activity: str = Form(...),
        bin: int = Form(...),
        ids_to_exclude: list[str] = Form(None)
):
    results = []
    try:
        for file, bank in zip(files, banks):
            content = await file.read()
            with NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
                tmp.write(content)
                temp_path = tmp.name

            try:
                if bank == "decl910":
                    res = table_find_decl910(temp_path)
                elif bank == "decl220":
                    res = table_find_decl220(temp_path)
                else:
                    res = calc_ep_vyp(temp_path, bank, ids_to_exclude, bin)

                results.append({
                    "bank": bank,
                    "ep": res["ep"],
                    "start_date": res["start_date"],
                    "end_date": res["end_date"],
                    "bin": res["bin"]
                })
            finally:
                os.unlink(temp_path)

        filtered_results = []

        if any(r["bank"] == "decl220" for r in results):
            filtered_results = []
            for bin_value in set(r["bin"] for r in results if r["bank"] == "decl220"):
                bin_results = [r for r in results if r["bank"] == "decl220" and r["bin"] == bin_value]
                for r in bin_results:
                    overlap = any(
                        periods_overlap(r["start_date"], r["end_date"], fr["start_date"], fr["end_date"])
                        for fr in filtered_results if fr["bin"] == bin_value
                    )
                    if not overlap:
                        filtered_results.append(r)

        elif any(r["bank"] == "decl910" for r in results):
            for bin_value in set(r["bin"] for r in results if r["bank"] == "decl910"):
                bin_results = [r for r in results if r["bank"] == "decl910" and r["bin"] == bin_value]
                for r in bin_results:
                    overlap = any(
                        periods_overlap(r["start_date"], r["end_date"], fr["start_date"], fr["end_date"])
                        for fr in filtered_results if fr["bin"] == bin_value
                    )
                    if not overlap:
                        filtered_results.append(r)

        else:
            for bin_value in set(r["bin"] for r in results):
                bin_results = [r for r in results if r["bin"] == bin_value]
                for r in bin_results:
                    overlap = any(
                        periods_overlap(r["start_date"], r["end_date"], fr["start_date"], fr["end_date"])
                        for fr in filtered_results if fr["bin"] == bin_value
                    )
                    if not overlap:
                        filtered_results.append(r)

        total = sum(r["ep"] for r in filtered_results)

        return JSONResponse(content={
            "results": results,
            "total": round(total * PERCENTAGES[activity], 3)
        })

    except Exception as e:
        return JSONResponse(content={"error": "here"}, status_code=500)

def calc_ep_vyp(temp_path, bank, ids_to_exclude, bin):
    if bank == "kaspi":
        df = table_find_kaspi_vp(temp_path)
        bin_find = bin_find_kaspi_vp(temp_path)
        start_date, end_date = date_find_kaspi_vp(temp_path)
    elif bank == "alatau":
        df = table_find_alatau_vp(temp_path)
        bin_find = bin_find_alatau_vp(temp_path)
        start_date, end_date = date_find_alatau_vp(temp_path)
    elif bank == "bcc":
        df = table_find_bcc_vp(temp_path)
        bin_find = bin_find_bcc_vp(temp_path)
        start_date, end_date = date_find_bcc_vp(temp_path)
    elif bank == "halyk":
        df = table_find_halyk_vp(temp_path)
        bin_find = bin_find_halyk_vp(temp_path)
        start_date, end_date = date_find_halyk_vp(temp_path)
    elif bank == "forte":
        df = table_find_forte_vp(temp_path)
        bin_find = bin_find_forte_vp(temp_path)
        start_date, end_date = date_find_forte_vp(temp_path)
    else:
        return JSONResponse(content={"error": "Неизвестный банк"}, status_code=400)

    agent = create_pandas_dataframe_agent(
        llm,
        df.tail(10),
        max_iterations=20,
        allow_dangerous_code=True,
        agent_type=AgentType.OPENAI_FUNCTIONS,
    )

    task = f"""
    У нас есть DataFrame и я тебе передаю только 10 строк этого DataFrame. 
    Тебе надо прочитать и проанализировать df и написать универсальный код который применим ко всей df, 
    и обязательно сохрани изначальное название колонок и их количество. 
    Подготовить данные (задать соответствующий тип для колонок: кредит, дебет это float(errors='coerce' нельзя использовать на колонках Дебет и Кредит) 
    и колонки связанные с датой давать параметр dayfirst=True
    В таблице встречаются числа в разных форматах (например: "725 \n000,00", "5,040,000.00", "800.000.00").
    Твоя задача — привести их к единому числовому формату с двумя знаками после точки (например: 725000.00).
    
    Правила обработки:
    1. Убрать символы переноса строки и заменить их пробелом.
    2. Удалить все пробелы внутри числа.
    3. Если используется запятая как разделитель десятых, заменить её на точку.
    4. Убрать все лишние разделители тысяч (точки, запятые или пробелы внутри числа), оставить только один десятичный разделитель — точку.
    5. Если после точки больше двух цифр, округлить до двух знаков.
    6. Вернуть результат строго в числовом виде, например: 725000.00, 5040000.00, 800000.00.
    посчитать чистые ежемесячные обороты. 
    Под чистыми подразумевается только транзакции реальной деятельности, а не переводы собственных средств и т.д. 
    Убирай строки где номера колонок, а не информация. 

    Среднемесячные чистые обороты – это обороты, не включающие следующие виды операций: 
    взнос собственных средств, переводы с депозита, поступления кредитных средств, возвраты денежных средств). 
    Виды операций можно определить в колонне: назначение платежа. 
    Убирай строки, которые содержат слова из 
    exclude_keywords=[перевод собственных средств, Возврат средств, Возврат ошибочных элементов платежа, возврат, Возврат, Перевод со своего, Перевод собственных средств, Возврат излишне зачисленной суммы, Перевод со счета KaspiPay на Депозит, Возврат по счетам].

    Дополнительно убирай строки, если они содержат значения из списка ids_to_exclude={ids_to_exclude} —
    это может быть отдельная колонка БИН получателя, либо значения внутри колонки Наименование получателя или схожее название по смыслу. Искать только у получателей.

    Напиши функцию table_cleaner(df), которая принимает DataFrame df и возвращает словарь в формате {{"ep": round(number / количество месяцев, 3)}}. 

    Требования:
    1. Функция должна использовать pandas.
    2. Рассчитывать ежемесячные обороты по колонкам "дебет" и "кредит".
    3. После группировки по месяцам использовать groupby(..., as_index=False).
    4. Проверить, что после группировки есть минимум 6 месяцев. Если меньше — вернуть {{"error": "выписка не пригодна"}}.
    5. В коде должно быть только функция и импорт библиотек.
    6. Никаких комментариев и лишнего текста в коде.
    
    Верни код в ```
    """

    raw_output = agent.invoke(task)

    match = re.search(r"```(?:python)?\n(.*?)```", raw_output["output"], re.DOTALL)
    if match:
        generated_code = match.group(1).strip()
    else:
        generated_code = raw_output["output"].strip()

    with open("agent_generated.py", "w", encoding="utf-8") as f:
        f.write(generated_code)

    spec = importlib.util.spec_from_file_location("agent_generated", "agent_generated.py")
    agent_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(agent_module)

    if bin == bin_find:
        ep_res = agent_module.table_cleaner(df)
    elif ids_to_exclude and bin_find in ids_to_exclude:
        ep_res = {"ep": agent_module.table_cleaner(df) * 0.5}
    else:
        ep_res = {"ep": 0}

    return {
        "ep": ep_res["ep"],
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "bin": bin
    }
