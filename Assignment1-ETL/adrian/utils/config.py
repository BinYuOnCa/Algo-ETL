import os
from os.path import join, dirname
from dotenv import load_dotenv


dotenv_path = 'utils/.myenv'
load_dotenv(dotenv_path)


db_config = {
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USERNAME'),
    'password': os.getenv('DB_PWD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}


finn_config = {
    'finn_api': os.getenv('FINN_API'),
    'finn_api_sandbox': os.getenv('FINN_API_SANDBOX'),
    'finn_webhook': os.getenv('FINN_WEBHOOK')
}

email_config = {
    'account': os.getenv('MAIL_ACCOUNT'),
    'password': os.getenv('MAIL_AUTH_CODE'),
    'smtp_server': os.getenv('MAIL_SMTP_SERVER'),
    'smtp_port': os.getenv('MAIL_SMTP_PORT'),
    'sender': os.getenv('MAIL_SENDER'),
    'receiver': os.getenv('MAIL_RECEIVER')
}

if __name__ == '__main__':
    print(db_config)
    print(finn_config)