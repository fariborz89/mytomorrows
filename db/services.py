from sqlalchemy import create_engine
from utils.config import DB

db_connect = create_engine('sqlite:///' + DB)


class DbServices:

    def __init__(self):
        pass

    @staticmethod
    def query_execut(command):
        conn = db_connect.connect()
        return conn.execute(command)

    @staticmethod
    def create_table_data():
        conn = db_connect.connect()
        command_to_create = 'create table if not exists data(Street varchar (100), City varchar(50), Zip int, State varchar(4),' \
                            ' Beds int, Baths int, Size int, Type varchar(30), SaleDate date, Price int,' \
                            ' Latitude float, Longitude float);'
        conn.execute(command_to_create)
