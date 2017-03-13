import unittest

from mongodb_backend import testing
from expects import *
from destral.transaction import Transaction

import doctest
from mongodb_backend import mongodb2
from mongodb_backend import orm_mongodb

def load_tests(loader, tests, ignore):
    tests.addTests(doctest.DocTestSuite(mongodb2))
    return tests

class MongoDBBackendTest(testing.MongoDBTestCase):

    @unittest.skip('No views defined in this module')
    def test_all_views(self):
        pass
    
    @unittest.skip('No access rules defined')
    def test_access_rules(self):
        pass

    def test_mdbpool(self):
        from mongodb_backend.mongodb2 import mdbpool
        expect(mdbpool._connection).to(be_none)

        # If we try to access to the connection mongodb connects
        expect(mdbpool.connection).to_not(be_none)

    def test_default_mongodb_name(self):
        from mongodb_backend.mongodb2 import mdbpool
        expect(self.openerp.config.options).to_not(have_keys(
            'mongodb_name',
            'mongodb_port',
            'mongodb_host',
            'mongodb_user',
            'mongodb_pass'
        ))
        # After accessing to getting object variables are defined
        db = mdbpool.get_db()
        expect(self.openerp.config.options).to(have_keys(
            mongodb_name=self.database
        ))

    def test_compute_order_parsing(self):
        class order_test(orm_mongodb.orm_mongodb):
            _name = 'order.test'

        with Transaction().start(self.database) as txn:
            cursor = txn.cursor
            uid = txn.user

            testing_class = order_test(cursor)

            res = testing_class._compute_order(cursor, uid, 'test desc')
            self.assertEqual(res, [('test', -1)])

            res = testing_class._compute_order(cursor, uid, 'test asc')
            self.assertEqual(res, [('test', 1)])

            res = testing_class._compute_order(cursor, uid, 'test desc, '
                                                            'test2 desc')
            self.assertEqual(res, [('test', -1), ('test2', -1)])

            res = testing_class._compute_order(cursor, uid, 'test asc, '
                                                            'test2 desc')
            self.assertEqual(res, [('test', 1), ('test2', -1)])
