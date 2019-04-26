from flask import request
from flask_restful import Resource

from datetime import datetime
from utils.base import create_final_dict
from db.services import DbServices


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
            list_time = words[8].split(' ')
            sale_date_string = ' '.join(list_time[1: 4]) + ' ' + list_time[5]
            sale_date = datetime.strptime(sale_date_string, "%b %d %H:%M:%S %Y")

            dict = {
                'street': words[0],
                'city': words[1],
                'zip': int(words[2]),
                'state': words[3],
                'beds': int(words[4]),
                'baths': int(words[5]),
                'size': int(words[6]),
                'type': words[7],
                'sale_date': sale_date,
                'price': int(words[9]),
                'latitude': float(words[10]),
                'longitude': float(words[11])}

            DbServices.insert_data(dict)
            line = f.readline()
            print line
        return 'file uploaded successfully', 201


class AggregatedData(Resource):
    def get(self, aggregation_type, from_date, to_date):
        from_date = datetime.strptime(from_date, "%Y-%m-%d").date()
        to_date = datetime.strptime(to_date, "%Y-%m-%d").date()

        command = 'select min(price), max(price), avg(price), {0} from sales where sale_date ' \
                  'between \'{1}\' and \'{2}\' group by {0};'.format(aggregation_type, from_date, to_date)
        query = DbServices.query_execute(command)
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
            city_condition = 'city=\'' + city + '\''
            first_and = True

        if 'size' in request.args:
            size = request.args['size']
            if first_and:
                first_and_char = 'And '
            size_condition = first_and_char + 'size=' + size
            second_and = True

        if 'type' in request.args:
            type = request.args['type']
            if first_and or second_and:
                second_and_char = 'And '
            type_condition = second_and_char + 'type=\'' + type + '\''

        from_date = datetime.strptime(from_date, "%Y-%m-%d").date()
        to_date = datetime.strptime(to_date, "%Y-%m-%d").date()
        # conn = db_connect.connect()

        command = 'select min(price), max(price), avg(price), {0} from sales where sale_date ' \
                  'between \'{1}\' and \'{2}\' And {3} {4} {5} group by {0};'.format \
            (aggregation_type, from_date, to_date, city_condition, size_condition, type_condition)

        query = DbServices.query_execute(command)

        return create_final_dict(aggregation_type, query), 200
