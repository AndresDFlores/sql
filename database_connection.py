import sqlalchemy as sqla
from sqlalchemy import create_engine, inspect, schema, MetaData, Table, text, and_, or_, cast
from sqlalchemy.orm import Session
from sqlalchemy.sql import sqltypes

from connection_strings import *

class DatabaseConnection:

    @classmethod
    def set_export_protocol(cls, export=True):
        cls.export=export


    def __init__(self):

        conn_string = ConnectionStrings().postgres()
        self._engine=create_engine(conn_string, echo=False)

        self._init_inspector = inspect(self._engine)
        self.set_export_protocol(export=True)



    def check_schema_presence(self, schema_name):
        if schema_name in inspect(self._engine).get_schema_names():
            return True
        else:
            return False
        


    def create_new_schema(self, schema_name):
        if not self.check_schema_presence(schema_name=schema_name):
            self._engine.execute(sqla.schema.CreateSchema(schema_name))
            return True
        else:
            return False
        


    def check_table_presence(self, schema_name, table_name):
        
        if table_name in inspect(self._engine).get_table_names(schema=schema_name):
            return True
        else:
            return False



    def get_table_object(self, schema_name, table_name):
        meta=MetaData(schema=schema_name)
        meta.reflect(
            bind=self._engine,
            schema=schema_name)

        table_object = Table(
            table_name, 
            meta, 
            autoload=True, 
            autoload_with=self._engine, 
            schema_name=schema_name)

        return table_object
    


    def count_table_rows(self, schema_name, table_name):
        if not self.check_table_presence(schema_name=schema_name, table_name=table_name):
            return None
        
        table=self.get_table_object(
            schema_name=schema_name,
            table_name=table_name)
        
        with Session(self._engine) as session:
            return session.query(table).count()
        


    def drop_table(self, schema_name, table_name):
        if not self.check_table_presence(schema_name=schema_name, table_name=table_name):
            return None
        
        table=self.get_table_object(
            schema_name=schema_name,
            table_name=table_name)
        
        try:
            table.drop(self._engine)
            return True
        except:
            return False
        


    def drop_table_row(self, schema_name, table_name, conditional_column, conditional_statement):
        if not self.check_table_presence(schema_name=schema_name, table_name=table_name):
            return None
        
        table=self.get_table_object(
            schema_name=schema_name,
            table_name=table_name)
        
        columns_list=table.columns.keys()
        column=table.selectable.columns.values()[columns_list.index(conditional_column)]
        with self._engine.connect() as conn:
            try:
                conn.execute(table.delete().where(column==conditional_statement))
            except:
                pass
        


    def drop_all_table_data(self, schema_name, table_name):
        if not self.check_table_presence(schema_name=schema_name, table_name=table_name):
            return None
        
        table=self.get_table_object(
            schema_name=schema_name,
            table_name=table_name)
        
        with self._engine.connect() as conn:
            try:
                conn.execute(table.delete())
            except:
                pass
    


    def get_all_table_column_objects(self, schema_name, table_name):
        if not self.check_table_presence(schema_name=schema_name, table_name=table_name):
            return None
        
        table=self.get_table_object(
            schema_name=schema_name,
            table_name=table_name)
        
        column_objects=list(table.columns)

        columns={}
        for col_obj in range(len(column_objects)):

            columns[column_objects[col_obj]]=dict(
                column_name=column_objects[col_obj].name,
                dtype=column_objects[col_obj].type,
                primary_keys=column_objects[col_obj].primary_key,
                foreign_keys=column_objects[col_obj].foreign_keys
            )

        return columns
    


    def get_column_object(self, schema_name, table_name, column_name):
        column_objects=self.get_all_table_column_objects(schema_name=schema_name, table_name=table_name)

        for col_obj in list(column_objects):
            if column_objects[col_obj]['column_name']==column_name:
                return col_obj
            


    def export_table(self, schema_name, table_name, data):
        if not self.check_table_presence(schema_name=schema_name, table_name=table_name):
            return None
        
        if self.export:

            table=self.get_table_object(
                schema_name=schema_name,
                table_name=table_name)
            
            with self._engine.connect() as conn:
                try:
                    conn.execute(table.insert(), data)
                    return True
                except:
                    print(f'NO EXPORT PERFORMED: {schema_name}.{table_name}')
                    return False



    def get_selected_columns(self, schema_name, table_name, select_columns=[]):

        def get_column_names(column_objects):
            for col_obj in list(column_objects):
                yield column_objects[col_obj]['column_name']

        column_objects = self.get_all_table_column_objects(schema_name=schema_name, table_name=table_name)
        if len(select_columns)==0:

            column_names = get_column_names(column_objects=column_objects)
            column_names = list(column_names)

            return column_objects, column_names

        else:

            all_available_column_names = [
                column_objects[col_obj]['column_name'] for col_obj in list(column_objects)
            ]

            all_available_column_names_idx = [idx for idx, vals in enumerate(all_available_column_names) if vals in select_columns]

            selected_column_objects = {}
            for key_idx, key in enumerate(column_objects):
                if key_idx in all_available_column_names_idx:
                    selected_column_objects[key]=column_objects[key]

            if len(list(selected_column_objects))==0:

                column_names = get_column_names(column_objects=column_objects)
                column_names = list(column_names)

                return column_objects, column_names

            else:
                column_names = get_column_names(column_objects=selected_column_objects)
                column_names = list(column_names)

                return selected_column_objects, column_names


    def execute_string_query(self, string_query):
        with self._engine.connect() as conn:
            try:
                query=conn.execute(text(string_query)).fetchall()
                print('STRING QUERY SUCCESSFUL')

                return query
            except:
                return None


    def import_table_where(self, schema_name, table_name, select_columns=[], and_condition=True, conditions={}):
        if not self.check_table_presence(schema_name=schema_name, table_name=table_name):
            return None

        def get_comparison_operation(string):
            
            def pop_val(val_string):
                for op in ['=', '>', '<']:
                    val_string=val_string.replace(op, '')
                return val_string

            pop_value=pop_val(val_string=string)

            operator=string.replace(pop_value, '')
            value=string.split(operator)[-1].lstrip()

            return operator, value


        table=self.get_table_object(
            schema_name=schema_name,
            table_name=table_name)

        
        #  these are the column objects that will be imported from the query
        columns_to_query = self.get_selected_columns(
            schema_name=schema_name,
            table_name=table_name,
            select_columns=select_columns
        )

        with Session(self._engine) as session:

            try:
                condition_filters=[]
                for conditional_column in list(conditions):
                    for conditional_value in conditions[conditional_column]:

                        filters=getattr(table.columns, conditional_column)

                        comp_op = get_comparison_operation(string=conditional_value)                        
                        operator = comp_op[0]
                        value = comp_op[1]

                        if operator == '==':
                            filters = filters==value
                        elif operator=='<=' or operator=='=<':
                            filters = filters<=value
                        elif operator=='>=' or operator=='=>':
                            filters = filters>=value
                        elif operator=='<':
                            filters = filters<value
                        elif operator=='>':
                            filters = filters>value
                        
                        condition_filters.append(filters)


                #  query conditional logicals
                if and_condition:
                    logical_operator=and_
                else:
                    logical_operator=or_


                #  generate SQL query
                if len(select_columns)==0:
                    select_statement=table.columns
                else:
                    select_statement=[]
                    for sel_col in select_columns:
                        select_statement.append(getattr(table.columns, sel_col))

                query = session.query(*select_statement).filter(logical_operator(*condition_filters))
            
                return query.all()
            
            except:
                print('NO IMPORT')
                return None


    def import_table_like(self, schema_name, table_name, select_columns=[], and_condition=True, conditions={}):

        '''
        Valid 'LIKE' Conditions:
            a%: value starts with 'a'
            %a: value ends with 'a'
            %a%: value contains 'a' in any position
            _a%: value contains 'a' in second position
            a_%: value starts with 'a' and contains at least 2 characters
            a__%: value starts with 'a' and contains at least 3 characters
            a%o: value starts with 'a' and ends with 'o'
        '''
        
        if not self.check_table_presence(schema_name=schema_name, table_name=table_name):
            return None


        table=self.get_table_object(
            schema_name=schema_name,
            table_name=table_name)

        
        #  these are the column objects that will be imported from the query
        columns_to_query = self.get_selected_columns(
            schema_name=schema_name,
            table_name=table_name,
            select_columns=select_columns
        )

        with Session(self._engine) as session:

            try:
                condition_filters=[]
                for conditional_column in list(conditions):
                    col_obj = table.c[conditional_column]
                    dtype = col_obj.type

                    for like_condition in conditions[conditional_column]:
                        if str(dtype) is sqltypes.Integer.__name__:
                            condition_filters.append(cast(col_obj, sqltypes.String).like(like_condition))
                        else:
                            condition_filters.append(col_obj.like(like_condition))


                #  query conditional logicals
                if and_condition:
                    logical_operator=and_
                else:
                    logical_operator=or_


                #  generate SQL query
                if len(select_columns)==0:
                    select_statement=table.columns
                else:
                    select_statement=[]
                    for sel_col in select_columns:
                        select_statement.append(getattr(table.columns, sel_col))

                query = session.query(*select_statement).filter(logical_operator(*condition_filters))
            
                return query.all()
            
            except:
                print('NO IMPORT')
                return None


if __name__=='__main__':
    db_conn = DatabaseConnection()
    print(db_conn._engine)
    