import re,io,os, importlib.util
from dotenv import load_dotenv

from fastapi import APIRouter, FastAPI, File, UploadFile, Form
from tempfile import NamedTemporaryFile
from fastapi.responses import StreamingResponse
from fastapi.responses import PlainTextResponse

from langchain_experimental.agents import create_pandas_dataframe_agent
from langchain.agents.agent_types import AgentType
from langchain_google_genai import ChatGoogleGenerativeAI

from parser.bcc_bank import table_find_bcc_vp
from parser.alatau_bank import table_find_alatau_vp
from parser.kaspi_bank import table_find_kaspi_vp

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

@pre_router.post("/ep", response_class=PlainTextResponse)
async def root(
    file: UploadFile = File(...),
    bank: str = Form(...),
    ids_to_exclude: list[str] = Form(None)
):
    content = await file.read()

    with NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        tmp.write(content)
        temp_path = tmp.name

    try:
        if bank == "kaspi":
            df = table_find_kaspi_vp(temp_path)
        elif bank == "alatau":
            df = table_find_alatau_vp(temp_path)
        elif bank == "bcc":
            df = table_find_bcc_vp(temp_path)
        else:
            return "Ошибка: неизвестный банк"

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
        и колонки связанные с датой давать параметр dayfirst=True, в таблице имеются числа формата 725 \\n000,00 
        для них нужно вместо \\n ставить пробел, убирать оставшиеся пробелы и заменять запятую на .), 
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

        Код который выйдет по итогу надо обернуть в функцию table_cleaner(df) с аргументом df 
        который возвращает переменную ежемесячных оборотов(колонны дебет и кредит) в формате DataFrame 
        (в конце когда используешь groupby всегда используй as_index=False). 
        Кроме функции и библиотек ничего больше быть не должно. 
        В конце верни только Python-код заранее проверив его на df который ты получил, 
        код должен быть без комментариев, все нужные библиотеки импортнуты над функцией, без текста и комментариев.
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

        cleaned_df = agent_module.table_cleaner(df)

        stream = io.BytesIO()
        stream.write(cleaned_df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig"))
        stream.seek(0)

        response = StreamingResponse(
            stream,
            media_type="text/csv; charset=utf-8"
        )
        response.headers["Content-Disposition"] = "attachment; filename=output.csv"
        return response

    finally:
        os.unlink(temp_path)
