from fastapi import FastAPI
from db_manager import db_init
from routers.router import router as model_router
from migrate import router as migrate_router

app = FastAPI()

db_init()

app.include_router(model_router)

app = FastAPI(title="Database Migration App")

app.include_router(migrate_router)

