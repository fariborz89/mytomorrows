class Price:
    def __init__(self, min, max, avg):
        self.min = min
        self.max = max
        self.avg = avg


class CityPrice(Price):
    def __init__(self, min, max, avg, city):
        Price.__init__(self, min, max, avg)
        self.city = city


class SizePrice(Price):
    def __init__(self, min, max, avg, size):
        Price.__init__(self, min, max, avg)
        self.size = size


class TypePrice(Price):
    def __init__(self, min, max, avg, type):
        Price.__init__(self, min, max, avg)
        self.type = type