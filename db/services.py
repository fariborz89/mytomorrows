from sqlalchemy import create_engine, Table, Column, Integer, Date, Float, String, MetaData, ForeignKey

from utils.config import DB

metadata = MetaData()

sales = Table('sales', metadata,
              Column('id', Integer, primary_key=True),
              Column('street', String),
              Column('city', String),
              Column('zip', Integer),
              Column('state', String),
              Column('beds', Integer),
              Column('baths', Integer),
              Column('size', Integer),
              Column('type', String),
              Column('sale_date', Date),
              Column('price', Integer),
              Column('latitude', Float),
              Column('longitude', Float),
              )


class DbServices:
    engine = create_engine('sqlite:///' + DB)

    @staticmethod
    def query_execute(command):
        '''
        :param command:
        :return:
        '''
        conn = DbServices.engine.connect()
        return conn.execute(command)

    @staticmethod
    def insert_data(dict):
        '''
        :param command:
        :return:
        '''
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
        metadata.create_all(DbServices.engine)
        return
