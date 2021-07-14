import psycopg2


class CopyCsvToPostgres:

    def __init__(self, pg_params, csv_name, db_table_name):
        self.pg_params = pg_params
        self.csv_name = csv_name
        self.db_table_name = db_table_name

    def execute(self):
        conn = psycopg2.connect(self.pg_params)
        cursor = conn.cursor()

        with open(f'files/{self.csv_name}.csv', 'r') as f:
            next(f)
            cursor.execute(f"Truncate {self.db_table_name} Cascade;")
            cursor.copy_from(f, f'{self.db_table_name}', sep=';')

        conn.commit()
        cursor.close()


if __name__ == '__main__':
    pg_params = "host=127.0.0.1 dbname=postgres user=postgres password='1'"
    products = CopyCsvToPostgres(pg_params, csv_name="product_dataset", db_table_name="products")
    category = CopyCsvToPostgres(pg_params, csv_name="category_dataset", db_table_name="categories")
    products.execute()
    category.execute()
