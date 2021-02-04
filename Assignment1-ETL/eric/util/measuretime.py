import time

class MeasureTime:
    '''`
    with catchtime() as t:
        pass
    print(t)
    print(repr(t))
    print(float(t))
    print 0+t
    print 1*t
    '''
    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, type, value, traceback):
        self.end = time.time()

    def __float__(self):
        return float(self.end - self.start)

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return '{:.3f}'.format(float(self))
