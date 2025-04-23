from sqlalchemy import MetaData, select
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
        source_data = source_session.execute(select(users_table)).fetchall()
        rows_to_insert = []

        for row in source_data:
            row_dict = dict(row._mapping)
            stmt = select(users_table).where(users_table.c.email == row_dict['email']).limit(1)
            result = target_session.execute(stmt).first()
            if not result:
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
        source_posts = source_session.execute(select(posts_table)).fetchall()

        existing_post_ids = {p[0] for p in target_session.execute(select(posts_table.c.id))}

        posts_to_insert = [
            dict(p._mapping)
            for p in source_posts
            if p.id not in existing_post_ids
        ]

        if posts_to_insert:
            print(f"Inserting {len(posts_to_insert)} posts")
            target_session.execute(posts_table.insert().values(posts_to_insert))

        return len(posts_to_insert)

    except Exception as e:
        target_session.rollback()
        raise HTTPException(status_code=500, detail=f"Error migrating posts: {str(e)}")


def handle_comments(source_session, target_session, comments_table):
    try:
        posts_table = comments_table.metadata.tables['posts']

        source_comments = [
            dict(row._asdict())
            for row in source_session.execute(select(comments_table))
        ]

        if not source_comments:
            print("No comments to migrate")
            return 0

        post_ids = {c['post_id'] for c in source_comments if 'post_id' in c}

        if post_ids:
            post_ids_list = list(post_ids)

            existing_posts = {
                row[0] for row in
                target_session.execute(
                    select(posts_table.c.id).where(posts_table.c.id.in_(post_ids_list))
                )
            }
            missing_posts = post_ids - existing_posts
            if missing_posts:
                raise HTTPException(
                    status_code=400,
                    detail=f"Missing post IDs: {sorted(missing_posts)}"
                )

        existing_comments = {
            row[0] for row in
            target_session.execute(select(comments_table.c.id))
        }

        comments_to_insert = [
            c for c in source_comments
            if c.get('id') not in existing_comments
        ]

        if comments_to_insert:
            target_session.execute(
                comments_table.insert(),
                comments_to_insert
            )
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
        source_data = source_session.execute(select(products_table)).fetchall()
        rows_to_insert = []

        for row in source_data:
            row_dict = dict(row._mapping)
            stmt = select(products_table).where(products_table.c.id == row_dict['id']).limit(1)
            result = target_session.execute(stmt).first()
            if not result:
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
