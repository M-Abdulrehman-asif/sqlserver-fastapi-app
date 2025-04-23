from utils.models import User, Post, Comment, Product
from fastapi import HTTPException
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from database.source_db import Base


def insert_data_in_table(sheets_dict, db_handler):
    if not isinstance(sheets_dict, dict):
        raise HTTPException(
            status_code=400,
            detail="Input data must be a dictionary of DataFrames"
        )

    results = {
        "users": {"inserted": 0, "updated": 0, "skipped": 0},
        "products": {"inserted": 0, "updated": 0, "skipped": 0},
        "posts": {"inserted": 0, "updated": 0, "skipped": 0},
        "comments": {"inserted": 0, "skipped": 0, "invalid_post_ids": []}
    }

    session = db_handler.get_session()
    try:
        Base.metadata.create_all(bind=db_handler.engine)
        processing_order = ['users', 'products', 'posts', 'comments']

        for table_name in processing_order:
            if table_name not in sheets_dict:
                continue

            df = sheets_dict[table_name]
            data = df.to_dict('records')

            if table_name == 'users':
                for record in data:
                    existing = session.query(User).filter_by(email=record['email']).first()
                    if existing:
                        for key, value in record.items():
                            setattr(existing, key, value)
                        results['users']['updated'] += 1
                    else:
                        session.add(User(**record))
                        results['users']['inserted'] += 1

            elif table_name == 'products':
                for record in data:
                    existing = session.query(Product).filter_by(name=record['name']).first()
                    if existing:
                        for key, value in record.items():
                            setattr(existing, key, value)
                        results['products']['updated'] += 1
                    else:
                        session.add(Product(**record))
                        results['products']['inserted'] += 1

            elif table_name == 'posts':
                valid_user_ids = {u.id for u in session.query(User.id).all()}
                for record in data:
                    if record['author_id'] not in valid_user_ids:
                        results['posts']['skipped'] += 1
                        continue

                    existing = session.query(Post).filter_by(title=record['title']).first()
                    if existing:
                        for key, value in record.items():
                            setattr(existing, key, value)
                        results['posts']['updated'] += 1
                    else:
                        session.add(Post(**record))
                        results['posts']['inserted'] += 1

            elif table_name == 'comments':
                valid_post_ids = {p.id for p in session.query(Post.id).all()}
                for record in data:
                    post_id = record.get('post_id')
                    if post_id not in valid_post_ids:
                        results['comments']['invalid_post_ids'].append(post_id)
                        results['comments']['skipped'] += 1
                        continue

                    existing = session.query(Comment).filter_by(
                        post_id=record['post_id'],
                        text=record['text']
                    ).first()

                    if not existing:
                        session.add(Comment(**record))
                        results['comments']['inserted'] += 1
                    else:
                        results['comments']['skipped'] += 1

            session.commit()

    except IntegrityError as e:
        session.rollback()
        raise HTTPException(
            status_code=400,
            detail=f"Foreign key violation: {str(e)}"
        )
    except SQLAlchemyError as e:
        session.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Database error: {str(e)}"
        )
    except Exception as e:
        session.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Unexpected error: {str(e)}"
        )
    finally:
        session.close()

    return results
