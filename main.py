from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from service.check_debt import main_router
from service.calc_ep import pre_router
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(pre_router)
app.include_router(main_router)