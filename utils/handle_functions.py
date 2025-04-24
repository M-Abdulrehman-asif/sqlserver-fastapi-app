from sqlalchemy import MetaData, select, func
from fastapi import HTTPException


def handle_tables(source_handler, target_handler):
    try:
        source_metadata = MetaData()
        source_metadata.reflect(bind=source_handler.engine)

        print(f"Reflected tables: {list(source_metadata.tables.keys())}")

        target_handler.create_db()
        source_metadata.create_all(bind=target_handler.engine)
        return source_metadata

    except Exception as e:
        print(f"Error reflecting and creating tables: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error reflecting and creating tables: {str(e)}")


def handle_users(source_session, target_session, users_table):
    try:
        max_id = target_session.execute(
            select(users_table.c.id).order_by(users_table.c.id.desc()).limit(1)
        ).scalar() or 0

        target_user_ids = {row.id for row in target_session.execute(select(users_table.c.id))}
        source_data = source_session.execute(select(users_table)).fetchall()

        rows_to_insert = []
        for row in source_data:
            row_dict = dict(row._mapping)
            new_id = max_id + row_dict['id']
            if new_id not in target_user_ids:
                row_dict['id'] = new_id
                rows_to_insert.append(row_dict)

        if rows_to_insert:
            target_session.execute(users_table.insert().values(rows_to_insert))
            target_session.commit()

        return len(rows_to_insert)
    except Exception as e:
        target_session.rollback()
        raise HTTPException(status_code=500, detail=f"Error migrating users: {str(e)}")



def handle_posts(source_session, target_session, posts_table):
    try:
        max_id = target_session.execute(
            select(posts_table.c.id).order_by(posts_table.c.id.desc()).limit(1)
        ).scalar() or 0

        users_table = posts_table.metadata.tables['users']
        valid_user_ids = {row.id for row in target_session.execute(select(users_table.c.id))}

        source_posts = source_session.execute(
            select(posts_table).where(posts_table.c.author_id.in_(valid_user_ids))
        ).fetchall()

        target_post_ids = {row.id for row in target_session.execute(select(posts_table.c.id))}

        posts_to_insert = []
        for post in source_posts:
            p_dict = dict(post._mapping)
            new_id = max_id + p_dict['id']
            if new_id not in target_post_ids:
                p_dict['id'] = new_id
                posts_to_insert.append(p_dict)

        if posts_to_insert:
            target_session.execute(posts_table.insert().values(posts_to_insert))
            target_session.commit()

        return len(posts_to_insert)
    except Exception as e:
        target_session.rollback()
        raise HTTPException(status_code=500, detail=f"Error migrating posts: {str(e)}")



def handle_comments(source_session, target_session, comments_table):
    try:
        max_id = target_session.execute(
            select(comments_table.c.id).order_by(comments_table.c.id.desc()).limit(1)
        ).scalar() or 0

        posts_table = comments_table.metadata.tables['posts']
        source_comments = source_session.execute(select(comments_table)).fetchall()

        if not source_comments:
            return 0

        post_ids_in_comments = {row.post_id for row in source_comments}
        existing_post_ids = {row.id for row in target_session.execute(
            select(posts_table.c.id).where(posts_table.c.id.in_(post_ids_in_comments))
        )}

        missing_post_ids = post_ids_in_comments - existing_post_ids
        if missing_post_ids:
            raise HTTPException(
                status_code=400,
                detail=f"Missing post IDs: {sorted(missing_post_ids)}"
            )

        existing_comments = {
            (row.post_id, row.text)
            for row in target_session.execute(select(comments_table.c.post_id, comments_table.c.text))
        }

        comments_to_insert = []
        for c in source_comments:
            c_dict = dict(c._mapping)
            if (c_dict['post_id'], c_dict['text']) not in existing_comments:
                c_dict['id'] = max_id + c_dict['id']
                comments_to_insert.append(c_dict)

        if comments_to_insert:
            target_session.execute(comments_table.insert().values(comments_to_insert))
            target_session.commit()

        return len(comments_to_insert)
    except HTTPException:
        raise
    except Exception as e:
        target_session.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Comment migration error: {str(e)}"
        )



def handle_products(source_session, target_session, products_table):
    try:
        max_id = target_session.execute(
            select(products_table.c.id).order_by(products_table.c.id.desc()).limit(1)
        ).scalar() or 0

        target_product_ids = {row.id for row in target_session.execute(select(products_table.c.id))}
        source_data = source_session.execute(select(products_table)).fetchall()

        rows_to_insert = []
        for row in source_data:
            row_dict = dict(row._mapping)
            new_id = max_id + row_dict['id']
            if new_id not in target_product_ids:
                row_dict['id'] = new_id
                rows_to_insert.append(row_dict)

        if rows_to_insert:
            target_session.execute(products_table.insert().values(rows_to_insert))
            target_session.commit()

        return len(rows_to_insert)
    except Exception as e:
        target_session.rollback()
        raise HTTPException(status_code=500, detail=f"Error migrating products: {str(e)}")



def migrate_data(source_session, target_session, table):
    try:
        table_name = table.name.lower()

        if table_name == "users":
            return handle_users(source_session, target_session, table)

        elif table_name == "posts":
            return handle_posts(source_session, target_session, table)

        elif table_name == "comments":
            return handle_comments(source_session, target_session, table)

        elif table_name == "products":
            return handle_products(source_session, target_session, table)

        else:
            raise HTTPException(status_code=400, detail=f"Migration not implemented for table: {table_name}")

    except Exception as e:
        print(f"Error migrating table {table.name}: {str(e)}")
        target_session.rollback()
        raise HTTPException(status_code=500, detail=f"Error migrating table {table.name}: {str(e)}")


def migrate_known_tables(source_session, target_session, source_metadata):
    inserted_counts = {}
    processing_order = ['users', 'posts', 'comments', 'products']

    for table_name in processing_order:
        table = source_metadata.tables.get(table_name)
        if table is None:
            print(f"Table {table_name} not found in source database")
            continue

        print(f"Processing {table_name}...")

        if table_name == 'posts':
            inserted = migrate_data(source_session, target_session, table)
            target_session.commit()
            print(f"Committed {inserted} posts before processing comments")
        else:
            inserted = migrate_data(source_session, target_session, table)

        inserted_counts[table_name] = inserted
        print(f"Migrated {inserted} rows into {table_name}")

    return inserted_counts
