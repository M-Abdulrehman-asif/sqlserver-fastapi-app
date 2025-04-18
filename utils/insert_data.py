from fastapi import HTTPException
from sqlalchemy.exc import SQLAlchemyError
from utils.models import User, Post, Comment, Product

def insert_data_in_table(df, db_handler):
    rows_inserted = 0

    try:
        for model_name, model_df in df.items():
            if model_name == "users":
                model_class = User
            elif model_name == "posts":
                model_class = Post
            elif model_name == "comments":
                model_class = Comment
            elif model_name == "products":
                model_class = Product
            else:
                raise HTTPException(status_code=400, detail=f"Invalid model name '{model_name}'.")

            for _, row in model_df.iterrows():
                obj = model_class(**row.to_dict())
                db_handler.session.add(obj)

            db_handler.session.commit()
            rows_inserted += len(model_df)

    except SQLAlchemyError as e:
        db_handler.session.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

    return rows_inserted



