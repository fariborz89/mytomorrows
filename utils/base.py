from models.price import CityPrice, SizePrice, TypePrice


def create_final_dict(aggregation_type, query):
    """
    :param aggregation_type: Is the aggregation based on city, size or type
    :param query: The result of database execution command, this input has the list of response.
    :return: dictionary of the aggregated data which can be shown as json
    """
    final_list = []
    if aggregation_type == 'city':
        for r in query.cursor.fetchall():
            obj = CityPrice(r[0], r[1], r[2], r[3])
            final_list.append(obj)

    if aggregation_type == 'size':
        for r in query.cursor.fetchall():
            obj = SizePrice(r[0], r[1], r[2], r[3])
            final_list.append(obj)

    if aggregation_type == 'type':
        for r in query.cursor.fetchall():
            obj = TypePrice(r[0], r[1], r[2], r[3])
            final_list.append(obj)
    final_dict = [obj.__dict__ for obj in final_list]
    return final_dict
