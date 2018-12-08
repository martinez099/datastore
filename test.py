import json
import unittest

import redis

import datastore


prod1 = {
  "Name": "Product1",
  "Description": "Description1",
  "Vendor": "Vendor1",
  "Price": 123,
  "Currency": "EUR",
  "MainCategory": "Category1",
  "Images": ["asdfasdfasdfasdfadsfasdfasdfasdf", "yvcycvycvyxvcycxvvxcyxcvyvxc", "qwerqwreqwerqwerqwerqwrer"]
}

prod2 = {
  "Name": "Product2",
  "Description": "Description2",
  "Vendor": "Vendor2",
  "Price": 23.23,
  "Currency": "EUR",
  "MainCategory": "Category2",
  "Images": ["asdfasdfasdfasfffdfadsfasdfasdfasdf", "yqqqqvcycvycvyxvcycxvvxcyxcvyvxc", "qyyyywerqwreqwerqwerqwerqwrer"]
}

prod3 = {
    "Name": "Product3",
    "Description": "Description3",
    "Vendor": "Vendor3",
    "Price": 2.23,
    "Currency": "EUR",
    "MainCategory": "Category2",
    "Images": ["asdfasdfasdfasfffdfadsfasdfasdf", "yqqqqvcycvycvyxvcycxvvxcyxcvy", "yyyyywerqwreqwerqwerqwerqwrer"]
}


class DataStoreTestCase(unittest.TestCase):

    redis = None

    def __init__(self, methodName='runTest'):
        super(DataStoreTestCase, self).__init__(methodName)
        datastore.app.testing = True
        self.app = datastore.app.test_client()

    @classmethod
    def setUpClass(cls):
        cls.redis = redis.StrictRedis(decode_responses=True)

    def test_a_insert_product1(self):
        rv = self.app.post('/product', data=json.dumps(prod1))
        assert b'1' in rv.data

    def test_b_insert_product2(self):
        rv = self.app.post('/product', data=json.dumps(prod2))
        assert b'2' in rv.data

    def test_c_insert_product3(self):
        rv = self.app.post('/product', data=json.dumps(prod3))
        assert b'3' in rv.data

    def test_cc_get(self):
        rv = self.app.get('/category/2')
        assert b'Category2' == rv.data

    def test_d_list(self):
        rv = self.app.get('/list/2')
        assert 2 == len(json.loads(rv.data))

    def test_dd_list(self):
        rv = self.app.get('/list')
        assert 'Category1' == json.loads(rv.data)[0]

    def test_e_search(self):
        rv = self.app.get('/search/Prod')
        assert 3 == len(json.loads(rv.data))

    def test_f_delete(self):
        rv = self.app.delete('/product/1')
        assert b'True' in rv.data

    def test_g_search2(self):
        rv = self.app.get('/search/Prod')
        assert 2 == len(json.loads(rv.data))

    def test_h_update(self):
        rv = self.app.put('/product/2', data=json.dumps(prod3))
        assert b'True' in rv.data

    def test_i_get(self):
        rv = self.app.get('/product/2')
        assert 'Product3' == json.loads(rv.data).get('Name')

    def test_k_get(self):
        rv = self.app.get('/image/10')
        assert b'asdfasdfasdfasfffdfadsfasdfasdf' == rv.data

    def test_l_delete(self):
        rv = self.app.delete('/product/2')
        assert b'True' in rv.data

    def test_m_delete(self):
        rv = self.app.delete('/product/3')
        assert b'True' in rv.data

    @classmethod
    def tearDownClass(cls):
        cls.redis.flushdb()


if __name__ == '__main__':
    unittest.main()
