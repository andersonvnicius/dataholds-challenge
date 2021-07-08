
from load_files import get_list_csv_files_from_path, get_df_from_csv

file_path = 'files'


file_list = get_list_csv_files_from_path(file_path)

# OR
# file_list = [
#     {
#         'filename': 'product_dataset',
#         'path': 'files/product_dataset.csv'
#     },
#     {
#         'filename': 'category_dataset',
#         'path': 'files/category_dataset.csv'
#     },
# ]

dataframes = get_df_from_csv(file_list)


# def run_main():
#     """main script"""
#     world = 'and1'
#     print(f'hello {world}')
#
#
# if __name__ == '__main__':
#     run_main()
