import os
from dotenv import load_dotenv

from fastapi import APIRouter, File, UploadFile, Form
from fastapi.responses import JSONResponse
from tempfile import NamedTemporaryFile

from parser.tax_org import table_find_decl910, table_find_decl220
from service.calc_ep.calc_ep import calc_ep_vyp_ur, periods_overlap, calc_ep_vyp_fl
from service.calc_ep.constants import PERCENTAGES

pre_router = APIRouter(
    prefix="/preapproval",
)

@pre_router.post("/ep")
async def root(
    files: list[UploadFile] = File(...),
    banks: list[str] = Form(...),
    activity: str = Form(...),
    bin: str = Form(...),
    ids_to_exclude: list[str] = Form(None)
):
    MAX_RETRIES = 3
    file_bytes_list = [await f.read() for f in files]
    temp_paths = []
    try:
        for content in file_bytes_list:
            tmp = NamedTemporaryFile(suffix=".pdf", delete=False)
            tmp.write(content)
            tmp.flush()
            tmp.close()
            temp_paths.append(tmp.name)

        attempt = 0
        while attempt < MAX_RETRIES:
            try:
                results = []
                for temp_path, bank in zip(temp_paths, banks):
                    if bank == "decl910":
                        res = table_find_decl910(temp_path)
                        if res[0]["bin"] != bin and (res[0]["bin"] not in ids_to_exclude):
                            return JSONResponse(content={"error": "БИН/ИИН неизвестного лица (декларация)", "bin": res[0]["bin"], "binDec": bin}, status_code=400)
                    elif bank == "decl220":
                        res = table_find_decl220(temp_path)
                        if res[0]["bin"] != bin and (res[0]["bin"] not in ids_to_exclude):
                            return JSONResponse(content={"error": "БИН/ИИН неизвестного лица (декларация)", "bin": res[0]["bin"], "binDec": bin}, status_code=400)
                    elif bank in ("flkaspi", "flforte", "flhalyk", "flalatau", "flbcc"):
                        res = calc_ep_vyp_fl(temp_path, bank, ids_to_exclude, bin)
                    elif bank in ("urkaspi", "urforte", "urhalyk", "uralatau", "urbcc"):
                        res = calc_ep_vyp_ur(temp_path, bank, ids_to_exclude, bin)
                    else:
                        return JSONResponse(content={"error": "Unknown bank"}, status_code=400)

                    for row in res:
                        if "error" in row:
                            return JSONResponse(content=row, status_code=400)
                        results.append({
                            "bank": bank,
                            "ep": row["ep"],
                            "start_date": row["start_date"],
                            "end_date": row["end_date"],
                            "bin": row["bin"]
                        })

                decl_banks = {"decl910", "decl220"}
                filtered_results = []
                total = 0

                for bin_value in set(r["bin"] for r in results):
                    bin_results = [r for r in results if r["bin"] == bin_value]

                    decl_results = [r for r in bin_results if r["bank"] in decl_banks]
                    other_results = [r for r in bin_results if r["bank"] not in decl_banks]

                    decl_results.sort(key=lambda x: 1 if x["bank"] == "decl910" else 0)

                    covered_periods = []
                    vp_sum = 0
                    vp_months = 0

                    for d in decl_results:
                        overlap = any(
                            periods_overlap(d["start_date"], d["end_date"], c["start_date"], c["end_date"])
                            for c in covered_periods
                        )
                        if not overlap:
                            total += d["ep"]
                            filtered_results.append(d)
                            covered_periods.append(d)

                    for r in other_results:
                        overlap = any(
                            periods_overlap(r["start_date"], r["end_date"], c["start_date"], c["end_date"])
                            for c in covered_periods
                        )
                        if not overlap:
                            vp_sum += r["ep"]
                            vp_months += 1
                            filtered_results.append(r)

                    if vp_months > 0:
                        total += vp_sum / vp_months

                total = round(total * PERCENTAGES[activity], 3)

                return JSONResponse(content={
                    "results": results,
                    "total": total
                })

            except Exception as e:
                attempt += 1
                if attempt >= MAX_RETRIES:
                    return JSONResponse(content={"error": str(e)}, status_code=500)

    finally:
        for p in temp_paths:
            try:
                if os.path.exists(p):
                    os.unlink(p)
            except Exception:
                pass