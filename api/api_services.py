
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
            # conn = db_connect.connect()
            list_time = words[8].split(' ')
            sale_date_string = ' '.join(list_time[1: 4]) + ' ' + list_time[5]
            sale_date = datetime.strptime(sale_date_string, "%b %d %H:%M:%S %Y")
            command_to_insert = 'insert into data values(\'{0}\', \'{1}\', {2}, \'{3}\', {4}, {5}, {6},' \
                                ' \'{7}\', \'{8}\', {9}, {10}, {11});'.format(
                words[0], words[1], int(words[2]), words[3], int(words[4]), int(words[5]), int(words[6]),
                words[7],
                sale_date.strftime('%Y-%m-%d'), int(words[9]), float(words[10]), float(words[11]))  # type: str
            #
            # conn.execute(commnad)
            DbServices.query_execute(command_to_insert)
            line = f.readline()
            print(line)
        return 'file uploaded successfully', 201


class AggregatedData(Resource):
    def get(self, aggregation_type, from_date, to_date):
        from_date = datetime.strptime(from_date, "%Y-%m-%d").date()
        to_date = datetime.strptime(to_date, "%Y-%m-%d").date()
        # conn = db_connect.connect()

        command = 'select min(price), max(price), avg(price), {0} from data where SaleDate ' \
                  'between \'{1}\' and \'{2}\' group by {0};'.format(aggregation_type, from_date, to_date)
        print(command)
        # query = conn.execute(command)
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
        # conn = db_connect.connect()

        command = 'select min(price), max(price), avg(price), {0} from data where SaleDate ' \
                  'between \'{1}\' and \'{2}\' And {3} {4} {5} group by {0};'.format \
            (aggregation_type, from_date, to_date, city_condition, size_condition, type_condition)

        print(command)
        # query = conn.execute(command)
        query = DbServices.query_execute(command)

        return create_final_dict(aggregation_type, query), 200

