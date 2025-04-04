import keyring
import os

class GetCredentials:

    def __init__(self, system_name, username):
        self.login_password = keyring.get_password(f'{system_name}', f'{username}')


if __name__=='__main__':
    get_cred = GetCredentials(
        system_name='postgres_login',
        username = 'postgres_user'
        )
          
    print(get_cred.login_password)