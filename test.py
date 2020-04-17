import json
import unittest

import redis

import main as datastore


prod1 = {
    "Name": "Product1",
    "Description": "Description1",
    "Vendor": "Vendor1",
    "Price": 123,
    "Currency": "EUR",
    "MainCategoryName": "Category1",
    "Images": ["asdfasdfasdfasdfadsfasdfasdfasdf", "yvcycvycvyxvcycxvvxcyxcvyvxc"]
}

prod2 = {
    "Name": "Product2",
    "Description": "Description2",
    "Vendor": "Vendor2",
    "Price": 23.23,
    "Currency": "EUR",
    "MainCategoryName": "Category2",
    "Images": ["asdfasdfasdfasfffdfadsfasdfasdfasdf", "yqqqqvcycvycvyxvcycxvvxcyxcvyvxc", "qyyyywerqwreqwewerqwerqwrer"]
}

prod3 = {
    "Name": "Product3",
    "Description": "Description3",
    "Vendor": "Vendor3",
    "Price": 2.23,
    "Currency": "EUR",
    "MainCategoryName": "Category2",
    "Images": ["asdfasdfasdfasfffdfadsfasdfasdf"]
}

prod4 = {
    "Name": "Product4",
    "Description": "Description4",
    "Vendor": "Vendor3",
    "Price": 4.21,
    "Currency": "EUR",
    "MainCategoryName": "Category2",
    "Images": ["asdfasdfatttttttadsfasdfasdf", "wwwwwwwycvyxvcycxvvxcyxcvy", "yopvfpofvpofvvmmäööploo"]
}

prod5 = {
    "Name": "Product5",
    "Description": "Description5",
    "Vendor": "Vendor1",
    "Price": 0.98,
    "Currency": "EUR",
    "MainCategoryName": "Category3",
    "Images": ["vuuvuuvuvsduwefubuiewfaodfsdf", "qsqsqsqdsypotrihkdptzobopgfmbgb"]
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

    def test_a_insert_products(self):
        for prod in [prod1, prod2, prod3, prod4, prod5]:
            rv = self.app.post('/product', data=json.dumps(prod))
            assert rv.status_code == 200

    def test_b_get(self):
        rv = self.app.get('/product/3')
        assert 'Product3' == json.loads(rv.data)['Name']

    def test_c_get(self):
        rv = self.app.get('/category/2')
        assert 'Category2' == json.loads(rv.data)['Name']

    def test_d_list(self):
        rv = self.app.get('/list/2')
        assert 3 == len(json.loads(rv.data))

    def test_e_list(self):
        rv = self.app.get('/list')
        assert 'Category2' == json.loads(rv.data)[0]

    def test_f_search(self):
        rv = self.app.get('/search/Prod')
        assert 5 == len(json.loads(rv.data))

    def test_g_delete(self):
        rv = self.app.delete('/product/1')
        assert b'true' in rv.data

    def test_h_search(self):
        rv = self.app.get('/search/Prod')
        assert 4 == len(json.loads(rv.data))

    def test_i_update(self):
        new_prod = prod2
        new_prod['Description'] = 'anotherDescription'
        new_prod['Images'] = ["asdfasdfasdfasfffdfadsfasdfasdfasdf", "yqqqqvcycvycvyxvcycxvvxcyxcvyvxc", "changedImage"]
        rv = self.app.put('/product/2', data=json.dumps(new_prod))
        assert b'true' in rv.data

    def test_j_get(self):
        rv = self.app.get('/product/2')
        assert 'anotherDescription' == json.loads(rv.data).get('Description')

    def test_k_get(self):
        rv = self.app.get('/image/10')
        assert b'vuuvuuvuvsduwefubuiewfaodfsdf' == rv.data

    @classmethod
    def tearDownClass(cls):
        cls.redis.flushdb()


if __name__ == '__main__':
    unittest.main()
