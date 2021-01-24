import os 
import os.path
import psycopg2
from dotenv import load_dirname
dotenv_path=join(dirname(__file__),'.env')
load_dotenv(dotenv_path)
DB_CONFIG={
    "DATABASE":os.getenv('DS_Database')
    "USER":os.getenv('DS_User')
    "PASSWORD":os.getenv('DS_password')
    "HOST":os.getenv('DS_Host')
}
def connect_to_db():
    engine=psycopg2.connect(
        database=DB_CONFIG["DATABASE"],
        user=DB_CONFIG["USER"],
        password=DB_CONFIG["password"],
        host=DB_CONFIG["Host"],
    )
    return engine
