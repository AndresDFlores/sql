from get_credentials import *

class ConnectionStrings:

    def __init__(self):
        pass

    def postgres(self):

        hostname='localhost'
        username='postgres'
        password=GetCredentials(
            system_name='postgres_login',
            username = 'postgres_user'
            ).login_password
        database='postgres'
        port=5432
        
        conn_string = f'postgresql+psycopg2://{username}:{password}@{hostname}:{port}/{database}'

        return conn_string