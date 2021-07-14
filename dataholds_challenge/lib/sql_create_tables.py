""""""


def create_tables(conn):
    cursor = conn.cursor()

    # create table for products
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS products(
        product_id text PRIMARY KEY,
        product_category_id text,
        product_name_lenght text,
        product_description_lenght text,
        product_photos_qty text,
        product_weight_g text,
        product_length_cm text,
        product_height_cm text,
        product_width_cm text 
        )
    """)

    # create table for categories
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS categories(
            category_id integer PRIMARY KEY,
            category_name text
        )
    """)

    # create table for products merged with categories
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS products_with_categories(
            product_id text PRIMARY KEY,
            product_name_lenght text,
            product_description_lenght text,
            product_photos_qty text,
            product_weight_g text,
            product_length_cm text,
            product_height_cm text,
            product_width_cm text ,
            product_category_id integer,
            category_name text
        )
    """)

    conn.commit()
    cursor.close()


if __name__ == '__main__':
    from psycopg2 import connect
    pg_params = "host=127.0.0.1 dbname=postgres user=postgres password='1'"
    conn = connect(pg_params)
    create_tables(conn)
