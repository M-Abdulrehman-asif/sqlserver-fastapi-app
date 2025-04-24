from sqlalchemy import MetaData, select, func
from fastapi import HTTPException


def reflect_metadata(source_handler):
    try:
        if not source_handler.engine:
            raise Exception("Source database engine not connected.")

        metadata = MetaData()
        metadata.reflect(bind=source_handler.engine)
        print(f"Reflected tables: {list(metadata.tables.keys())}")
        return metadata

    except Exception as e:
        print(f"Error reflecting metadata: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error reflecting metadata: {str(e)}")


def handle_users(source_session, target_session, users_table):
    try:
        max_id = target_session.execute(
            select(func.coalesce(func.max(users_table.c.id), 0))
        ).scalar()

        existing_emails = {
            row.email for row in target_session.execute(select(users_table.c.email))
        }
        source_data = source_session.execute(select(users_table)).fetchall()

        rows_to_insert = []
        for row in source_data:
            row_dict = dict(row._mapping)

            if row_dict['email'] in existing_emails:
                continue

            new_id = max_id + row_dict['id']
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
            select(func.coalesce(func.max(posts_table.c.id), 0))
        ).scalar()

        users_table = posts_table.metadata.tables['users']

        source_posts = source_session.execute(select(posts_table)).fetchall()
        if not source_posts:
            return 0

        target_users_max_id = target_session.execute(
            select(func.coalesce(func.max(users_table.c.id), 0))
        ).scalar()
        source_users_max_id = source_session.execute(
            select(func.coalesce(func.max(users_table.c.id), 0))
        ).scalar()
        id_offset = target_users_max_id - source_users_max_id

        existing_user_ids = {row.id for row in target_session.execute(select(users_table.c.id))}

        posts_to_insert = []
        skipped_posts = []

        for post in source_posts:
            post_dict = dict(post._mapping)
            new_author_id = post_dict['author_id'] + id_offset

            if new_author_id not in existing_user_ids:
                skipped_posts.append(post_dict['id'])
                continue

            post_dict['id'] = max_id + post_dict['id']
            post_dict['author_id'] = new_author_id
            posts_to_insert.append(post_dict)

        if posts_to_insert:
            target_session.execute(posts_table.insert().values(posts_to_insert))
            target_session.commit()

        if skipped_posts:
            print(f"Skipped posts due to missing author IDs: {sorted(skipped_posts)}")

        return len(posts_to_insert)

    except Exception as e:
        target_session.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Error migrating posts: {str(e)}"
        )

def handle_comments(source_session, target_session, comments_table):
    try:
        max_id = target_session.execute(
            select(func.coalesce(func.max(comments_table.c.id), 0))
        ).scalar()

        posts_table = comments_table.metadata.tables['posts']
        users_table = comments_table.metadata.tables['users']

        source_comments = source_session.execute(select(comments_table)).fetchall()
        if not source_comments:
            return 0

        target_users_max_id = target_session.execute(
            select(func.coalesce(func.max(users_table.c.id), 0))
        ).scalar()
        source_users_max_id = source_session.execute(
            select(func.coalesce(func.max(users_table.c.id), 0))
        ).scalar()
        id_offset = target_users_max_id - source_users_max_id

        valid_post_ids = {row.id for row in target_session.execute(select(posts_table.c.id))}
        valid_user_ids = {row.id for row in target_session.execute(select(users_table.c.id))}

        has_author_id = 'author_id' in comments_table.c
        comments_to_insert = []
        skipped_comments = []

        for row in source_comments:
            c_dict = dict(row._mapping)

            new_post_id = c_dict['post_id'] + id_offset
            if new_post_id not in valid_post_ids:
                skipped_comments.append(c_dict['id'])
                continue

            if has_author_id:
                new_author_id = c_dict['author_id'] + id_offset
                if new_author_id not in valid_user_ids:
                    skipped_comments.append(c_dict['id'])
                    continue
                c_dict['author_id'] = new_author_id

            c_dict['id'] = max_id + c_dict['id']
            c_dict['post_id'] = new_post_id
            comments_to_insert.append(c_dict)

        if comments_to_insert:
            target_session.execute(comments_table.insert().values(comments_to_insert))
            target_session.commit()

        if skipped_comments:
            print(f"Skipped comments due to missing post/author: {sorted(skipped_comments)}")

        return len(comments_to_insert)

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
