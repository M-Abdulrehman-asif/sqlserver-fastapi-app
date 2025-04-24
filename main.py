from fastapi import FastAPI
from routers.index import router as model_router
from routers.index import router as migrate_router

app = FastAPI(title="Database Migration App")

app.include_router(model_router)
app.include_router(migrate_router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8009)
