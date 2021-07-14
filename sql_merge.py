import psycopg2


class InsertMergedDataToTable:
    def __init__(self, pg_params):
        self.pg_params = pg_params

    def execute(self):
        conn = psycopg2.connect(self.pg_params)
        cursor = conn.cursor()

        cursor.execute(f"Truncate products_with_categories Cascade;")
        cursor.execute(self._get_data_to_insert_query())

        conn.commit()
        cursor.close()

    @staticmethod
    def _get_data_to_insert_query():
        return """
            INSERT INTO products_with_categories
            SELECT
                product_id,
                product_name_lenght,
                product_description_lenght,
                product_photos_qty,
                product_weight_g,
                product_length_cm,
                product_height_cm,
                product_width_cm,   
                categories.category_id,
                categories.category_name
            FROM
              products AS products            
            JOIN
              categories AS categories
            ON
              CAST(products.product_category_id AS integer)=categories.category_id
            WHERE
                products.product_category_id <> '#N/D'
        """

    # @staticmethod
    # def _insert_sql_data_template():
    #     return """
    #         INSERT INTO products_with_categories (
    #             product_id,
    #             product_name_lenght,
    #             product_description_lenght,
    #             product_photos_qty,
    #             product_weight_g,
    #             product_length_cm,
    #             product_height_cm,
    #             product_width_cm,
    #             product_category_id,
    #             category_name
    #         )
    #         VALUES (
    #             %(product_id)s,
    #             %(product_name_lenght)s,
    #             %(product_description_lenght)s,
    #             %(product_photos_qty)s,
    #             %(product_weight_g)s,
    #             %(product_length_cm)s,
    #             %(product_height_cm)s,
    #             %(product_width_cm)s,
    #             %(product_category_id)s,
    #             %(category_name)s
    #         )
    #     """


if __name__ == '__main__':
    pg_params = "host=127.0.0.1 dbname=postgres user=postgres password='1'"
    InsertMergedDataToTable(pg_params).execute()

