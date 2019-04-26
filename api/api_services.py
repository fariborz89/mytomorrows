from flask import request
from flask_restful import Resource

from datetime import datetime
from utils.base import create_final_dict
from db.services import DbServices


class NewSales(Resource):
    def post(self):

        f = request.files['file']

        # check the correctness of file
        correct_file, issue = self.check_input_file(f)
        if not correct_file:
            return issue, 400

        # go back to the begin of the file
        f.seek(0)
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
                'city': words[1].lower(),
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
        return 'File uploaded successfully', 201

    def check_input_file(self, f):
        line = f.readline()

        # checking the first line order
        is_allowed, issue = self.check_first_line(line)
        if not is_allowed:
            return False, issue
        first_line = True

        # checking all sales to be sure they have city, size, price and sale_date
        while line != '':
            if first_line:
                first_line = False
                line = f.readline()
                continue
            words = line.split(',')
            if words[1] == '' or words[6] == '' or words[7] == '' or words[8] == '':
                return False, "This sale does not have enough information: " + line
            line = f.readline()
        return True, 'ok'

    def check_first_line(self, line):
        if line.lower().strip() == 'street,city,zip,state,beds,baths,sq__ft,type,sale_date,price,latitude,longitude':
            return True, 'ok'
        return False, "First line is not in order."


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
            city_condition = 'city=\'' + city.lower() + '\''
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

        command = 'select min(price), max(price), avg(price), {0} from sales where sale_date ' \
                  'between \'{1}\' and \'{2}\' And {3} {4} {5} group by {0};'.format \
            (aggregation_type, from_date, to_date, city_condition, size_condition, type_condition)

        query = DbServices.query_execute(command)

        return create_final_dict(aggregation_type, query), 200
