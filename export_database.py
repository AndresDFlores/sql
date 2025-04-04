import pandas as pd
import os
import sys
from database_connection import *

def export_database():

    schemas=db_conn._init_inspector.get_schema_names()

    for schema_name in schemas:
        for table_name in db_conn._init_inspector.get_table_names(schema=schema_name):

            try:
                df_dict=db_conn.import_table_where(
                    schema_name=schema_name,
                    table_name=table_name
                )

                df=pd.DataFrame(df_dict)

                save_path = f'{table_name}_test.xlsx'
                df.to_excel(save_path, index=False)

            except:
                pass


exported_db=export_database()