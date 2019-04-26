from flask import Flask, request
from flask_restful import Resource, Api
from sqlalchemy import create_engine
from datetime import datetime
from utils.config import SERVER_PORT, DB
from utils.base import create_final_dict

db_connect = create_engine('sqlite:///' + DB)
app = Flask(__name__)
api = Api(app)


class NewSales(Resource):
    def post(self):
        f = request.files['file']
        line = f.readline()
        first_line = True
        while line != '':
            if first_line:
                first_line = False
                line = f.readline()
                continue
            words = line.split(',')
            conn = db_connect.connect()
            list_time = words[8].split(' ')
            sale_date_string = ' '.join(list_time[1: 4]) + ' ' + list_time[5]
            sale_date = datetime.strptime(sale_date_string, "%b %d %H:%M:%S %Y")
            conn.execute("insert into data values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
                         (words[0], words[1], int(words[2]), words[3], int(words[4]), int(words[5]), int(words[6]),
                          words[7],
                          sale_date.strftime('%Y-%m-%d'), int(words[9]), float(words[10]), float(words[11]))
                         )
            line = f.readline()
            print(line)
        return 'file uploaded successfully', 201


class AggregatedData(Resource):
    def get(self, aggregation_type, from_date, to_date):
        from_date = datetime.strptime(from_date, "%Y-%m-%d").date()
        to_date = datetime.strptime(to_date, "%Y-%m-%d").date()
        conn = db_connect.connect()

        command = 'select min(price), max(price), avg(price), {0} from data where SaleDate ' \
                  'between \'{1}\' and \'{2}\' group by {0};'.format(aggregation_type, from_date, to_date)
        print(command)
        query = conn.execute(command)

        final_dict = create_final_dict(aggregation_type, query)
        return final_dict, 200


class FilteredAggregatedData(Resource):
    def get(self, aggregation_type, from_date, to_date):
        city_condition = ''
        size_condition = ''
        type_condition = ''
        first_and = False
        second_and = False
        first_and_char = ''
        second_and_char = ''

        if 'city' in request.args:
            city = request.args['city']
            city_condition = 'City=\'' + city + '\''
            first_and = True

        if 'size' in request.args:
            size = request.args['size']
            if first_and:
                first_and_char = 'And '
            size_condition = first_and_char + 'Size=' + size
            second_and = True

        if 'type' in request.args:
            type = request.args['type']
            if first_and or second_and:
                second_and_char = 'And '
            type_condition = second_and_char + 'Type=\'' + type + '\''

        from_date = datetime.strptime(from_date, "%Y-%m-%d").date()
        to_date = datetime.strptime(to_date, "%Y-%m-%d").date()
        conn = db_connect.connect()

        command = 'select min(price), max(price), avg(price), {0} from data where SaleDate ' \
                  'between \'{1}\' and \'{2}\' And {3} {4} {5} group by {0};'.format \
            (aggregation_type, from_date, to_date, city_condition, size_condition, type_condition)

        print(command)
        query = conn.execute(command)

        return create_final_dict(aggregation_type, query), 200


api.add_resource(NewSales, '/v1/buildings/sale')  # Route_1
api.add_resource(AggregatedData, '/v1/buildings/sale/aggregated/<aggregation_type>/<from_date>/<to_date>')  # Route_2
api.add_resource(FilteredAggregatedData,
                 '/v1/buildings/sale/aggregated/filter/<aggregation_type>/<from_date>/<to_date>')  # Route_3


if __name__ == '__main__':
    app.run(port=SERVER_PORT)
