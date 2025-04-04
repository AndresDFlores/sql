# import pandas as pd
import os
from database_connection import *

db_conn = DatabaseConnection()
save_architecture=True

def get_architecture():

    schemas=db_conn._init_inspector.get_schema_names()
    print(schemas)

    # architecture=pd.DataFrame()
    # for schema_name in schemas:
    #     for table_name in db_conn._init_inspector.get_table_names(schema=schema_name):
    #         print(table_name)

    #         if save_architecture:
    #             table_column_objects=db_conn.get_all_table_column_objects(
    #                 schema_name=schema_name,
    #                 table_name=table_name
    #             )
    #             table_info=pd.DataFrame(table_column_objects).transpose().reset_index(drop=True)

    #             schema_table = pd.DataFrame(
    #                 dict(
    #                     schema_name=[schema_name]*table_info.shape[0],
    #                     table_name=[table_name]*table_info.shape[0]
    #                 )
    #             )
    #             taable_info=pd.concat([schema_name, table_info], axis=1)

    #             if not save_architecture:
    #                 print(table_info)

    #             architecture=pd.concat([architecture, table_info], axis=0)

    #         else:
    #             print(f'{schema_name}: {table_name}')

    # return architecture.reset_index(drop=True)

architecture=get_architecture()

# if save_architecture:
#     architecture.to_excel(os.path.join(os.getcwd(), f'{db_conn._engine.url.database}_architecture.xlsx'), index=False)