from sqlalchemy import create_engine
from utils.config import DB


class DbServices:
    db_connect = create_engine('sqlite:///' + DB)

    @staticmethod
    def query_execute(command):
        conn = DbServices.db_connect.connect()
        return conn.execute(command)

    @staticmethod
    def create_table_data():
        conn = DbServices.db_connect.connect()
        command_to_create = 'create table if not exists data(Street varchar (100), City varchar(50), Zip int, State varchar(4),' \
                            ' Beds int, Baths int, Size int, Type varchar(30), SaleDate date, Price int,' \
                            ' Latitude float, Longitude float);'
        conn.execute(command_to_create)
