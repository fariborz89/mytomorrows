from flask import Flask, request
from flask_restful import Resource, Api

from utils.config import SERVER_PORT
from db.services import DbServices
from api.api_services import NewSales, AggregatedData, FilteredAggregatedData


app = Flask(__name__)
api = Api(app)

api.add_resource(NewSales, '/v1/buildings/sale')  # Route_1
api.add_resource(AggregatedData, '/v1/buildings/sale/aggregated/<aggregation_type>/<from_date>/<to_date>')  # Route_2
api.add_resource(FilteredAggregatedData,
                 '/v1/buildings/sale/aggregated/filter/<aggregation_type>/<from_date>/<to_date>')  # Route_3

if __name__ == '__main__':
    DbServices.create_table_data()
    app.run(port=SERVER_PORT)
