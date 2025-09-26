import os, re, importlib.util

from fastapi.responses import JSONResponse

from langchain_experimental.agents import create_pandas_dataframe_agent
from langchain.agents.agent_types import AgentType

from langchain_google_genai import ChatGoogleGenerativeAI

from parser.bcc_bank import table_find_bcc_vp_ur, bin_find_bcc_vp
from parser.alatau_bank import table_find_alatau_vp_ur, bin_find_alatau_vp
from parser.forte_bank import table_find_forte_vp_ur, bin_find_forte_vp
from parser.halyk_bank import table_find_halyk_vp_ur, bin_find_halyk_vp
from parser.kaspi_bank import table_find_kaspi_vp_ur, bin_find_kaspi_vp, table_find_kaspi_vp_fl

api_key = os.getenv("GOOGLE_API_KEY")

llm = ChatGoogleGenerativeAI(
    model="gemini-2.5-pro",
    temperature=0,
    google_api_key=api_key
)

def periods_overlap(start1, end1, start2, end2):
    return not (end1 < start2 or end2 < start1)

def calc_ep_vyp_ur(temp_path, bank, ids_to_exclude, bin):
    if bank == "urkaspi":
        df = table_find_kaspi_vp_ur(temp_path)
        bin_find = bin_find_kaspi_vp(temp_path)
    elif bank == "uralatau":
        df = table_find_alatau_vp_ur(temp_path)
        bin_find = bin_find_alatau_vp(temp_path)
    elif bank == "urbcc":
        df = table_find_bcc_vp_ur(temp_path)
        bin_find = bin_find_bcc_vp(temp_path)
    elif bank == "urhalyk":
        df = table_find_halyk_vp_ur(temp_path)
        bin_find = bin_find_halyk_vp(temp_path)
    elif bank == "urforte":
        df = table_find_forte_vp_ur(temp_path)
        bin_find = bin_find_forte_vp(temp_path)
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

    Напиши функцию table_cleaner(df), которая принимает DataFrame df и возвращает список словарей ежемесячных оборотов. Все даты возвращай строго в формате YYYY-MM-DD, без времени. 
    Используй .date().isoformat() для start_date и end_date..
    Каждый словарь должен быть в формате:
    {{
        "ep": число_оборот_за_этот_месяц,
        "start_date": дата_начала_месяца,
        "end_date": дата_конца_месяца
    }}

    Требования:
    1. Функция должна использовать pandas.
    2. Рассчитывать ежемесячные обороты по колонке "кредит".
    3. После группировки по месяцам использовать groupby(..., as_index=False).
    4. Проверить, что после группировки есть минимум 6 месяцев. Если меньше — вернуть [{{"error": "выписка не пригодна"}}].
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

    if bin == bin_find or (ids_to_exclude and bin_find in ids_to_exclude):
        ep_result = agent_module.table_cleaner(df)
        for r in ep_result:
            r["bin"] = bin
    else:
        ep_result = [{"error": "Выписка неизвестного лица (выписка)", "bin": bin}]

    return ep_result

def calc_ep_vyp_fl(temp_path, bank, ids_to_exclude, bin):
    if bank == "flkaspi":
        df = table_find_kaspi_vp_fl(temp_path)
    # elif bank == "alatau":
    #     df = table_find_alatau_vp(temp_path)
    # elif bank == "bcc":
    #     df = table_find_bcc_vp(temp_path)
    # elif bank == "halyk":
    #     df = table_find_halyk_vp(temp_path)
    # elif bank == "forte":
    #     df = table_find_forte_vp(temp_path)
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
    Тебе надо прочитать и проанализировать df и написать универсальный код, который применим ко всей df,
    и обязательно сохрани изначальное название колонок и их количество. Подготовить данные (задать соответствующий тип для колонок):
    колонка Дата должна быть приведена к формату даты (dayfirst=True),
    оставить только строки, где колонка Операция содержит "Пополнение",
    колонка Сумма должна быть приведена к числовому типу float с двумя знаками после точки.
    В таблице встречаются суммы в разных форматах (например: "- 50 000,00 ₸", "+5,040,000.00", "800.000.00").
    Твоя задача — привести их к единому числовому формату с двумя знаками после точки (например: -50000.00, 5040000.00, 800000.00).
    Правила обработки чисел:
    Убрать все символы валюты и знаки +/₸/-.
    Убрать пробелы внутри числа.
    Убрать \\n
    Если используется запятая как разделитель десятых — заменить её на точку.
    Убрать все лишние разделители тысяч (точки, запятые или пробелы внутри числа), оставить только один десятичный разделитель — точку.
    Если после точки больше двух цифр, округлить до двух знаков.
    Вернуть результат строго в числовом виде (например: 50000.00, 5040000.00).
    Рассчитать среднемесячные чистые обороты по пополнениям.
    Для этого нужно:
    сгруппировать данные по месяцам (колонка Дата),
    посчитать сумму по колонке Сумма за каждый месяц.
    Дополнительно:
    Если после группировки получается меньше 6 месяцев, вернуть [{{"error": "выписка не пригодна"}}].
    Функция должна называться table_cleaner(df) и возвращать список словарей ежемесячных оборотов в формате:
    {{
        "ep": число_оборот_за_этот_месяц,
        "start_date": дата_начала_месяца (YYYY-MM-DD),
        "end_date": дата_конца_месяца (YYYY-MM-DD)
    }}
    Требования:
    Использовать pandas.
    После группировки по месяцам использовать groupby(..., as_index=False).
    Все даты возвращать строго в формате YYYY-MM-DD, без времени (используй .date().isoformat()).
    В коде должно быть только функция и импорт библиотек.
    Никаких комментариев и лишнего текста в коде.
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
    ep_result = agent_module.table_cleaner(df)
    for r in ep_result:
        r["ep"] = r["ep"] * 0.5
        r["bin"] = bin

    return ep_result