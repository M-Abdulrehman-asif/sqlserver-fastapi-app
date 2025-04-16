from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from db_manager import db_connect
from models import User, Post, Comment, Product

router = APIRouter()

model_map = {
    "users": User,
    "posts": Post,
    "comments": Comment,
    "products": Product,
}

@router.post("/create")
async def create_single_entry(request: Request, db: Session = Depends(db_connect)):
    try:
        body = await request.json()
        model_name = body.get("model")
        data = body.get("data")

        if not model_name or not data:
            raise HTTPException(status_code=400, detail="Missing 'model' or 'data' in request body.")

        model = model_map.get(model_name.lower())
        if not model:
            raise HTTPException(status_code=400, detail=f"Invalid model name '{model_name}'.")

        obj = model(**data)
        db.add(obj)
        db.commit()
        db.refresh(obj)
        return {"status": "success", "data": obj}

    except SQLAlchemyError as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")
