from lib.query import FinnhubQuery, OldDataQuery
import unittest


class TestQuery(unittest.TestCase):
    def test_restful_candles(self):
        result = FinnhubQuery().restful_candles('AAPL', '1', 1605629627, 1605629727)
        self.assertEqual(len(result), 2)

    def test_api_candles(self):
        result = FinnhubQuery().api_candles('AAPL', '1', 1605629627, 1605629727)
        self.assertEqual(len(result), 2)

    def test_query_latest(self):
        result = OldDataQuery().latest_candle('CIM', '1')
        self.assertEqual(len(result), 1)


if __name__ == '__main__':
    unittest.main()
