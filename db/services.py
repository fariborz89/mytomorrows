from sqlalchemy import create_engine, Table, Column, Integer, Date, Float, String, MetaData, ForeignKey

from utils.config import DB

metadata = MetaData()

sales = Table('sales', metadata,
              Column('id', Integer, primary_key=True),
              Column('street', String),
              Column('city', String, nullable=False),
              Column('zip', Integer),
              Column('state', String),
              Column('beds', Integer),
              Column('baths', Integer),
              Column('size', Integer, nullable=False),
              Column('type', String),
              Column('sale_date', Date, nullable=False),
              Column('price', Integer, nullable=False),
              Column('latitude', Float),
              Column('longitude', Float),
              )


class DbServices:
    engine = create_engine('sqlite:///' + DB)

    @staticmethod
    def query_execute(command):
        """
        Will execute the sql command
        :param command: the command to execute
        :return: The result of the db execution of command
        """
        conn = DbServices.engine.connect()
        return conn.execute(command)

    @staticmethod
    def insert_data(dict):
        """
        This function will insert new entities to the db
        :param dict: the dictionary of the entity
        :return: The result of db insert
        """
        ins = sales.insert().values(
            street=dict['street'],
            city=dict['city'],
            zip=dict['zip'],
            state=dict['state'],
            beds=dict['beds'],
            baths=dict['baths'],
            size=dict['size'],
            type=dict['type'],
            sale_date=dict['sale_date'],
            price=dict['price'],
            latitude=dict['latitude'],
            longitude=dict['longitude'])
        result = DbServices.engine.execute(ins)
        return result

    @staticmethod
    def create_sales_table():
        """
        This function will create the sales table if doesn't exist
        :return:
        """
        metadata.create_all(DbServices.engine)
        return
