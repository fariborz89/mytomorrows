from flask import Flask, request
from flask_restful import Resource, Api
from sqlalchemy import create_engine
from datetime import datetime
from utils.config import SERVER_PORT, DB

db_connect = create_engine('sqlite:///' + DB)
app = Flask(__name__)
api = Api(app)


class NewSales(Resource):
    def post(self):
        f = request.files['file']
        line = f.readline()
        first_line = True
        while line != '':
            # print(line)
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
            print line
        return 'file uploaded successfully', 201

api.add_resource(NewSales, '/v1/buildings/sale')  # Route_1

if __name__ == '__main__':
    app.run(port=SERVER_PORT)